"""Cenário 3 — Reinícios repetidos do componente de propagação.

O componente que propaga as mudanças é encerrado sem aviso (SIGKILL) e religado em ciclos
curtos, sob carga constante, durante --duracao-falha segundos (padrão: 10 minutos):

  - Software A (Domain Events): o próprio tracking-service, que publica depois do commit;
  - Software B (CDC): o Kafka Connect, onde roda a tarefa do Debezium.

Cada ciclo: SIGKILL, religa, espera o componente ficar pronto (A: /healthz responde; B: a
tarefa do conector volta a RUNNING), deixa no ar por --tempo-no-ar segundos e mata de novo.
No B, --tempo-no-ar precisa ser menor que o intervalo de gravação de offsets do Connect
(CDC_OFFSET_FLUSH_INTERVAL_MS, padrão 60 s), para que cada morte aconteça antes de o
progresso ser confirmado.

No A, as requisições que estavam no meio quando o serviço morreu ficam sem resposta. Cada
operação grava uma linha própria em `events`, com o correlation_id da requisição, então a
análise decide pela origem quais delas chegaram a ser commitadas (seção "ambiguas" do
resumo.json). Os eventos que esperavam retentativa na memória do tracking-service morrem com
ele: sem Outbox, não há registro durável do que faltava publicar.

Exemplos (na raiz do repositório, com o venv ativo):

  python -m scripts.cenario3.reinicios_repetidos --modo ambos
  python -m scripts.cenario3.reinicios_repetidos --modo cdc --duracao-falha 180 --tempo-no-ar 20 --regime 30

Saída em resultados/cenario3/<execução>/: os mesmos arquivos do Cenário 1, mais ciclos.csv
(um ciclo por linha: morte, religamento e instante em que voltou a ficar pronto).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import time

import aiohttp

from scripts.comum import ambiente as amb
from scripts.comum.execucao import ExecucaoBase, argumentos_comuns, executar


def componente(modo: str) -> str:
    return amb.MODOS[modo]["origem"] if modo == "domain-events" else "kafka-connect"


class Reinicios(ExecucaoBase):
    cenario = 3
    campos_extra = ["componente_no_ar", "ciclos"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.alvo = componente(self.modo)
        self.no_ar = True
        self.ciclos: list[dict] = []

    def parametros(self) -> dict:
        a = self.args
        return {**super().parametros(), "duracao_falha_s": a.duracao_falha, "tempo_no_ar_s": a.tempo_no_ar,
                "sinal": a.sinal, "componente": componente(self.modo),
                "offset_flush_interval_ms": int(os.environ.get("CDC_OFFSET_FLUSH_INTERVAL_MS", 60000))}

    async def falha(self) -> None:
        a = self.args
        inicio = self.marcos["falha_inicio"] = time.time()
        fim = inicio + a.duracao_falha
        self.log(f"FALHA: ciclos de {a.sinal} em {self.alvo} por {a.duracao_falha:.0f}s "
                 f"(no ar {a.tempo_no_ar:g}s entre mortes)")
        with open(self.pasta / "ciclos.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ciclo", "morte", "religado", "pronto", "tempo_ate_pronto_s"])
            w.writeheader()
            n = 0
            while time.time() < fim:
                n += 1
                c = {"ciclo": n}
                rc, out = await amb.compose_async("kill", "-s", a.sinal, self.alvo)
                c["morte"] = round(time.time(), 3)
                self.no_ar = False
                if rc != 0:
                    self.log(f"falha ao matar {self.alvo}: {out.strip()}")
                rc, out = await amb.ligar_async(self.alvo)
                c["religado"] = round(time.time(), 3)
                pronto = await self._esperar_pronto(a.limite_pronto)
                self.no_ar = True
                c["pronto"] = round(pronto, 3) if pronto else ""
                c["tempo_ate_pronto_s"] = round(pronto - c["morte"], 2) if pronto else ""
                w.writerow(c)
                f.flush()
                self.ciclos.append(c)
                self.log(f"ciclo {n}: {self.alvo} morto e religado; pronto em "
                         f"{c['tempo_ate_pronto_s'] if pronto else 'NÃO FICOU PRONTO'}s")
                await asyncio.sleep(max(0.0, min(a.tempo_no_ar, fim - time.time())))
        self.marcos["ciclos"] = len(self.ciclos)
        self.marcos["falha_fim"] = time.time()
        self.log(f"fim dos ciclos ({len(self.ciclos)}); {self.alvo} fica no ar")

    async def _esperar_pronto(self, limite_s: float) -> float | None:
        limite = time.time() + limite_s
        while time.time() < limite:
            if self.modo == "domain-events":
                try:
                    async with self.sessao.get(f"{amb.API_URL}/healthz", timeout=aiohttp.ClientTimeout(total=1)) as r:
                        if r.status == 200:
                            return time.time()
                except Exception:
                    pass
            elif await amb.estado_conector(self.sessao) == "RUNNING":
                return time.time()
            await asyncio.sleep(0.25)
        return None

    async def amostra_extra(self) -> dict:
        return {"componente_no_ar": int(self.no_ar), "ciclos": len(self.ciclos)}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    argumentos_comuns(p)
    p.add_argument("--duracao-falha", type=float, default=600, help="segundos de ciclos de reinício (padrão: 600)")
    p.add_argument("--tempo-no-ar", type=float, default=20,
                   help="segundos que o componente fica no ar, depois de pronto, antes da próxima morte (padrão: 20)")
    p.add_argument("--sinal", default="SIGKILL", choices=["SIGKILL", "SIGTERM"],
                   help="SIGKILL não deixa o processo finalizar nada (padrão); SIGTERM permite o desligamento limpo")
    p.add_argument("--limite-pronto", type=float, default=180,
                   help="tempo máximo de espera para o componente voltar a ficar pronto em cada ciclo (s)")
    a = p.parse_args()
    flush_s = int(os.environ.get("CDC_OFFSET_FLUSH_INTERVAL_MS", 60000)) / 1000
    if a.modo in ("cdc", "ambos") and a.tempo_no_ar >= flush_s:
        print(f"AVISO: --tempo-no-ar ({a.tempo_no_ar:g}s) não é menor que o intervalo de gravação de offsets "
              f"do Connect ({flush_s:g}s); no B, parte das mortes vai acontecer depois da confirmação do progresso")

    def extras(r: dict) -> dict:
        amb_ = r.get("ambiguas", {})
        return {"ciclos": r["reinicios"]["ciclos"], "sem_resposta": amb_.get("sem_resposta"),
                "sem_resposta_commitadas": amb_.get("commitadas"),
                "sem_resposta_nao_commitadas": amb_.get("nao_commitadas")}

    executar(Reinicios, a, "resultados/cenario3", f"ciclos{a.duracao_falha:.0f}s",
             lambda modo: amb.reiniciar_ambiente(modo, build=a.build), extras)


if __name__ == "__main__":
    main()
