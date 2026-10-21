"""Gerador de carga em malha aberta sobre a API do tracking-service.

A taxa de operações é mantida constante independentemente da latência da API: se o
serviço demorar a responder (como o Software A, que publica dentro da requisição), as
requisições se acumulam em voo em vez de reduzir a carga oferecida. Isso mantém a
"carga de escrita constante" exigida pelos cenários.

Cada operação é uma linha do RetailRocket preparado: um evento de comportamento
(POST /events) ou uma mudança de propriedade de item (POST /items/{id}/properties). Recebe um
X-Correlation-ID novo e é registrada no ledger (CSV), que é o lado cliente do oráculo: tudo o
que recebeu 2xx foi commitado na origem.
"""

from __future__ import annotations

import asyncio
import csv
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import aiohttp

from scripts.comum.retailrocket import Operacao

CAMPOS_LEDGER = ["t_envio", "t_resposta", "operacao", "event_id", "tipo", "visitor_id", "item_id",
                 "n_agregados", "correlation_id", "http_status", "latencia_ms", "versao_visitante", "versao_item", "erro"]


@dataclass
class Janela:
    """Contadores acumulados desde a última leitura (snapshot)."""
    enviadas: int = 0
    sucesso: int = 0
    erro: int = 0
    nao_enviadas: int = 0
    latencias_ms: list[float] = field(default_factory=list)


class GeradorCarga:
    def __init__(self, api_url: str, operacoes: Iterator[Operacao], taxa: float, ledger: Path,
                 max_em_voo: int = 2000, timeout_s: float = 60.0):
        """
        taxa: operações por segundo; 0 envia o mais rápido possível, limitado por max_em_voo.
        max_em_voo: requisições simultâneas. Com taxa definida, a operação que não cabe é
            contada como não enviada (a carga oferecida não foi sustentada).
        """
        self.api_url = api_url.rstrip("/")
        self.operacoes = operacoes
        self.taxa = taxa
        self.max_em_voo = max_em_voo
        self.timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._em_voo = 0
        self._tarefas: set[asyncio.Task] = set()
        self._retida: Operacao | None = None

        self.commitadas: set[str] = set()  # correlation_ids que receberam 2xx
        self.total = Janela()
        self._janela = Janela()
        self._ledger_f = open(ledger, "w", newline="")
        self._ledger = csv.DictWriter(self._ledger_f, fieldnames=CAMPOS_LEDGER)
        self._ledger.writeheader()

    # ------------------------------------------------------------------ leitura
    def snapshot(self) -> Janela:
        """Devolve os contadores do intervalo desde a última chamada e zera o intervalo."""
        j, self._janela = self._janela, Janela()
        return j

    @property
    def em_voo(self) -> int:
        return self._em_voo

    # ------------------------------------------------------------------ execução
    async def executar(self, parar: asyncio.Event, max_operacoes: int | None = None) -> None:
        """Emite operações na taxa configurada até `parar` ser acionado (ou esgotar)."""
        conector = aiohttp.TCPConnector(limit=self.max_em_voo, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=conector, timeout=self.timeout) as sessao:
            intervalo = 1.0 / self.taxa if self.taxa > 0 else 0.0
            proximo = time.perf_counter()
            emitidas = 0
            while not parar.is_set() and (max_operacoes is None or emitidas < max_operacoes):
                if self._em_voo >= self.max_em_voo and not intervalo:
                    # Sem taxa definida (rajada): espera uma vaga em vez de descartar.
                    await asyncio.sleep(0.002)
                    continue
                if self._retida is not None:
                    op, self._retida = self._retida, None
                else:
                    try:
                        op = next(self.operacoes)
                    except StopIteration:
                        break
                if self._em_voo >= self.max_em_voo:
                    # Com taxa definida: a operação não é enviada nesta vaga e isso fica
                    # registrado, pois significa que a carga oferecida não foi sustentada.
                    self.total.nao_enviadas += 1
                    self._janela.nao_enviadas += 1
                    self._retida = op
                else:
                    t = asyncio.create_task(self._enviar(sessao, op))
                    self._tarefas.add(t)
                    t.add_done_callback(self._tarefas.discard)
                    emitidas += 1
                if intervalo:
                    proximo += intervalo
                    espera = proximo - time.perf_counter()
                    if espera > 0:
                        await asyncio.sleep(espera)
                    elif espera < -1.0:
                        proximo = time.perf_counter()  # não tenta compensar atrasos longos
                elif emitidas % 200 == 0:
                    await asyncio.sleep(0)
            # Espera as requisições em voo terminarem (respeitando o timeout do cliente).
            if self._tarefas:
                await asyncio.gather(*list(self._tarefas), return_exceptions=True)
        self._ledger_f.flush()

    def fechar(self) -> None:
        self._ledger_f.close()

    # ------------------------------------------------------------------ interno
    async def _enviar(self, sessao: aiohttp.ClientSession, op: Operacao) -> None:
        corr = str(uuid.uuid4())
        linha = {"operacao": "comportamento" if op.comportamento else "propriedade", "event_id": op.event_id,
                 "tipo": op.tipo if op.comportamento else op.propriedade,
                 "visitor_id": "" if op.visitor_id is None else op.visitor_id, "item_id": op.item_id,
                 "n_agregados": op.n_agregados, "correlation_id": corr,
                 "http_status": 0, "versao_visitante": "", "versao_item": "", "erro": ""}
        self._em_voo += 1
        self.total.enviadas += 1
        self._janela.enviadas += 1
        t0 = time.time()
        p0 = time.perf_counter()
        try:
            async with sessao.post(f"{self.api_url}{op.caminho()}", json=op.corpo(),
                                   headers={"X-Correlation-ID": corr}) as resp:
                linha["http_status"] = resp.status
                corpo = await resp.json(content_type=None)
                if 200 <= resp.status < 300:
                    linha["versao_visitante"] = corpo.get("visitor_version", "")
                    linha["versao_item"] = corpo.get("item_version", "")
                else:
                    linha["erro"] = str(corpo.get("error", ""))[:200]
        except Exception as e:  # timeout, conexão recusada etc.
            linha["erro"] = f"{type(e).__name__}: {e}"[:200]
        finally:
            self._em_voo -= 1
        lat = (time.perf_counter() - p0) * 1000
        linha.update(t_envio=f"{t0:.6f}", t_resposta=f"{time.time():.6f}", latencia_ms=f"{lat:.2f}")
        self._ledger.writerow(linha)

        ok = 200 <= linha["http_status"] < 300
        self._janela.latencias_ms.append(lat)
        for j in (self.total, self._janela):
            if ok:
                j.sucesso += 1
            else:
                j.erro += 1
        if ok:
            self.commitadas.add(corr)
