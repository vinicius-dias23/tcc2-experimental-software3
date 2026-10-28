"""Esqueleto comum das execuções dos cenários 2, 3 e 5.

Cada execução segue as mesmas fases do Cenário 1 (Seção 4.4):

  1. regime        : carga constante com o sistema saudável (linha de base);
  2. falha         : a injeção própria do cenário (método `falha` da subclasse), com a carga
                     continuando;
  3. recuperação   : a carga continua por `--recuperacao` segundos depois que o cenário dá a
                     falha por encerrada;
  4. estabilização : a carga para e a coleta segue até origem e destino convergirem ou até não
                     chegar nada por `--sem-progresso` segundos.

A subclasse grava em `self.marcos` o instante em que a falha começou (`falha_inicio`) e
terminou (`falha_fim`), mais os marcos próprios do cenário. A análise em
`scripts.comum.analise` usa esses marcos e lê as seções extras de cada cenário.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import statistics
import time
from datetime import datetime
from pathlib import Path

import aiohttp
import psycopg

from scripts.carga.gerador import GeradorCarga
from scripts.comum import ambiente as amb
from scripts.comum.analise import analisar, imprimir
from scripts.comum.coleta import Coletor
from scripts.comum.retailrocket import FluxoOperacoes, info_seed

CAMPOS_SERIE = ["t", "instante", "fase", "api_enviadas", "api_sucesso", "api_erro", "api_nao_enviadas",
                "api_p99_ms", "em_voo", "commitadas_total", "destino_mensagens_total", "destino_correlacoes",
                "gap", "divergencia", "publicacao_ok_total", "publicacao_falhas_total", "conector_estado",
                "origem_disponivel", "origem_db_bytes", "origem_wal_bytes", "origem_agregados_tocados",
                "slot_ativo", "slot_retido_bytes", "slot_atraso_bytes",
                "retentativa_pendentes", "confirmados_com_atraso_total", "retentativa_recuperados_total", "declarados_perdidos_total",
                "conector_reinicios_auto_total", "conector_vigia_desistiu"]


class ExecucaoBase:
    cenario: int = 0
    campos_extra: list[str] = []

    def __init__(self, modo: str, args: argparse.Namespace, pasta: Path, rotulo: str):
        self.modo, self.args, self.pasta, self.rotulo = modo, args, pasta, rotulo
        self.fase = "regime"
        self.ultima: dict = {}
        self.marcos: dict = {"modo": modo, "cenario": self.cenario, "parametros": self.parametros()}

    # ------------------------------------------------------------------ ganchos da subclasse
    def parametros(self) -> dict:
        a = self.args
        return {"taxa_ops_s": a.taxa, "regime_s": a.regime, "recuperacao_carga_s": a.recuperacao,
                "intervalo_amostra_s": a.intervalo, "recuperacao_automatica": amb.parametros_recuperacao()}

    async def preparar(self) -> None:
        """Roda depois da reinicialização do ambiente e antes da carga."""

    async def falha(self) -> None:
        """Injeta a falha. Ao retornar, `falha_inicio` e `falha_fim` precisam estar nos marcos."""
        raise NotImplementedError

    async def amostra_extra(self) -> dict:
        return {}

    def tarefas_extras(self, parar: asyncio.Event) -> list:
        """Corrotinas que rodam durante toda a execução (ex.: leitura do estado do grupo)."""
        return []

    def servicos_para_logs(self) -> list[str]:
        s = [amb.MODOS[self.modo]["origem"], amb.MODOS[self.modo]["consumidor"], *amb.BROKERS]
        if self.modo == "cdc":
            s += ["kafka-connect", "connect-watchdog"]
        return s

    # ------------------------------------------------------------------ execução
    async def rodar(self, info: dict) -> dict:
        a = self.args
        inicio = time.time()
        self.marcos["inicio"] = inicio
        self.coletor = Coletor(inicio)
        await self.coletor.conectar()
        self.gerador = GeradorCarga(amb.API_URL, FluxoOperacoes(info, inicio_rodada=a.rodada_inicial), a.taxa,
                                    self.pasta / "ledger.csv", max_em_voo=a.max_em_voo)
        parar_carga, parar_amostra = asyncio.Event(), asyncio.Event()
        async with aiohttp.ClientSession() as sessao:
            self.sessao = sessao
            await self.preparar()
            t_carga = asyncio.create_task(self.gerador.executar(parar_carga))
            t_amostra = asyncio.create_task(self._amostrador(parar_amostra))
            extras = [asyncio.create_task(c) for c in self.tarefas_extras(parar_amostra)]

            self.log(f"regime por {a.regime:.0f}s a {a.taxa:g} ops/s")
            await asyncio.sleep(a.regime)

            self.fase = "falha"
            try:
                await self.falha()
            except Exception as e:
                self.log(f"ERRO durante a injeção: {type(e).__name__}: {e}")
                self.marcos.setdefault("erros_injecao", []).append(f"{type(e).__name__}: {e}")
            self.marcos.setdefault("falha_fim", time.time())

            self.fase = "recuperacao"
            self.log(f"recuperação: carga por mais {a.recuperacao:.0f}s")
            await asyncio.sleep(a.recuperacao)

            parar_carga.set()
            await t_carga
            self.gerador.fechar()
            self.marcos["carga_fim"] = time.time()
            self.fase = "estabilizacao"
            self.log("carga encerrada; aguardando convergência")
            await self._aguardar_convergencia()
            self.marcos["fim"] = time.time()
            parar_amostra.set()
            await t_amostra
            await asyncio.gather(*extras, return_exceptions=True)

        faltantes = await self.coletor.exportar(self.pasta)
        if faltantes:
            self.marcos["exportacoes_faltantes"] = faltantes
            self.log(f"não foi possível exportar {', '.join(faltantes)} (banco fora do ar)")
        await self.coletor.fechar()
        (self.pasta / "marcos.json").write_text(json.dumps(self.marcos, indent=2))
        self._salvar_logs()
        return analisar(self.pasta)

    async def _aguardar_convergencia(self) -> None:
        a = self.args
        inicio = time.time()
        while time.time() - inicio < a.estabilizacao_max:
            await asyncio.sleep(a.intervalo)
            u = self.ultima
            if u.get("gap") == 0 and u.get("divergencia") == 0:
                self.log("origem e destino convergiram")
                return
            ultimo = self.coletor.ultimo_recebimento
            parado = ultimo is None or time.time() - ultimo.timestamp() > a.sem_progresso
            if parado and time.time() - inicio > a.sem_progresso:
                self.log(f"sem mensagens novas há {a.sem_progresso:.0f}s; encerrando com divergência "
                         f"(gap={u.get('gap')}, divergência={u.get('divergencia')})")
                return
            if self.precisa_intervir(u):
                await self.intervir_conector()
        self.log("limite de estabilização atingido")

    def precisa_intervir(self, linha: dict) -> bool:
        """Intervenção manual (--reiniciar-conector-falho): só quando a tarefa está em FAILED e o
        reinício automático já desistiu (ou está desligado)."""
        return (self.modo == "cdc" and self.args.reiniciar_conector_falho and linha.get("conector_estado") == "FAILED"
                and str(linha.get("conector_vigia_desistiu", "")) in ("1", "1.0"))

    async def intervir_conector(self) -> None:
        if "intervencao_conector" not in self.marcos:
            self.marcos["intervencao_conector"] = time.time()
            self.log("tarefa do conector em FAILED: reiniciando (intervenção registrada)")
        await amb.reiniciar_tarefas_falhas(self.sessao)

    async def _amostrador(self, parar: asyncio.Event) -> None:
        with open(self.pasta / "serie_temporal.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CAMPOS_SERIE + self.campos_extra)
            w.writeheader()
            while not parar.is_set():
                t0 = time.time()
                try:
                    linha = await self._amostra()
                    w.writerow(linha)
                    f.flush()
                    self.ultima = linha
                    self.log(self.resumo_amostra(linha))
                    if self.fase == "recuperacao" and self.precisa_intervir(linha):
                        await self.intervir_conector()
                except Exception as e:
                    self.log(f"erro na amostragem: {type(e).__name__}: {e}")
                try:
                    await asyncio.wait_for(parar.wait(), max(0.0, self.args.intervalo - (time.time() - t0)))
                except asyncio.TimeoutError:
                    pass

    def resumo_amostra(self, linha: dict) -> str:
        return (f"{linha['fase']:<13} ok={linha['api_sucesso']:<5} erro={linha['api_erro']:<4} "
                f"p99={linha['api_p99_ms']}ms gap={linha['gap']:<6} div={linha['divergencia']:<6} "
                f"{'' if linha['origem_disponivel'] else 'ORIGEM FORA DO AR '}"
                f"{'conector=' + linha['conector_estado'] if self.modo == 'cdc' else 'falhas_pub=' + str(linha['publicacao_falhas_total'])}")

    async def _amostra(self) -> dict:
        j = self.gerador.snapshot()
        bd = await self.coletor.amostrar()
        met = await amb.ler_metricas(self.sessao, amb.API_URL)
        gap = len(self.gerador.commitadas - self.coletor.recebidos)
        linha = {
            "t": f"{time.time():.3f}", "instante": datetime.now().strftime("%H:%M:%S"), "fase": self.fase,
            "api_enviadas": j.enviadas, "api_sucesso": j.sucesso, "api_erro": j.erro,
            "api_nao_enviadas": j.nao_enviadas,
            "api_p99_ms": round(statistics.quantiles(j.latencias_ms, n=100)[98], 1) if len(j.latencias_ms) > 1 else "",
            "em_voo": self.gerador.em_voo, "commitadas_total": len(self.gerador.commitadas),
            "destino_mensagens_total": bd["destino_mensagens_total"],
            "destino_correlacoes": bd["destino_correlacoes_distintas"], "gap": gap,
            "divergencia": await self.coletor.divergencia(),
            "publicacao_ok_total": met.get("events_published_total", ""),
            "publicacao_falhas_total": met.get("events_publish_failed_total", ""),
            "conector_estado": await amb.estado_conector(self.sessao) if self.modo == "cdc" else "",
            "retentativa_pendentes": met.get("events_retry_pending", ""),
            "confirmados_com_atraso_total": met.get("events_late_confirmed_total", ""),
            "retentativa_recuperados_total": met.get("events_retry_recovered_total", ""),
            "declarados_perdidos_total": met.get("events_declared_lost_total", ""),
        }
        if self.modo == "cdc":
            vigia = await amb.ler_metricas(self.sessao, amb.VIGIA_URL)
            linha["conector_reinicios_auto_total"] = vigia.get("connector_auto_restarts_total", "")
            linha["conector_vigia_desistiu"] = vigia.get("connector_restart_gave_up", "")
        linha.update({k: bd[k] for k in ("origem_disponivel", "origem_db_bytes", "origem_wal_bytes",
                                          "origem_agregados_tocados", "slot_ativo", "slot_retido_bytes",
                                          "slot_atraso_bytes")})
        linha.update(await self.amostra_extra())
        return linha

    def _salvar_logs(self) -> None:
        logs = self.pasta / "logs"
        logs.mkdir(exist_ok=True)
        for s in self.servicos_para_logs():
            r = amb.compose("logs", "--no-color", "--timestamps", s, check=False, capturar=True, timeout=120)
            (logs / f"{s}.log").write_text(r.stdout + r.stderr)

    def log(self, msg: str) -> None:
        linha = f"[{datetime.now():%H:%M:%S}] [{self.modo} {self.rotulo}] {msg}"
        with open(self.pasta / "execucao.log", "a") as f:
            f.write(linha + "\n")
        print(linha, flush=True)


# ---------------------------------------------------------------------- linha de comando
def argumentos_comuns(p: argparse.ArgumentParser, taxa: float = 100, regime: float = 120,
                      recuperacao: float = 120) -> None:
    p.add_argument("--modo", required=True, choices=["domain-events", "cdc", "ambos"])
    p.add_argument("--repeticoes", type=int, default=1)
    p.add_argument("--taxa", type=float, default=taxa, help=f"operações por segundo (padrão: {taxa:g})")
    p.add_argument("--regime", type=float, default=regime, help="segundos de carga antes da falha")
    p.add_argument("--recuperacao", type=float, default=recuperacao, help="segundos de carga depois da falha")
    p.add_argument("--estabilizacao-max", type=float, default=900, help="limite de espera pela convergência (s)")
    # 300 s, como no Cenário 1: depois de uma falha, o Kafka Connect pode levar ~100 s para a
    # tarefa do Debezium voltar a ler o WAL. Com um limite menor, a execução do Software B
    # terminaria antes da retomada e registraria como perda o que ainda estava no WAL.
    p.add_argument("--sem-progresso", type=float, default=300,
                   help="encerra a estabilização se nenhuma mensagem chegar por este tempo (s, padrão: 300)")
    p.add_argument("--intervalo", type=float, default=5, help="intervalo de amostragem (s)")
    p.add_argument("--reiniciar-conector-falho", action="store_true",
                   help="no CDC, intervenção manual: reinicia a tarefa do Debezium em FAILED depois que o "
                        "reinício automático desistir (registra a intervenção)")
    p.add_argument("--sem-recuperacao", action="store_true",
                   help="desliga a retentativa do Software A e o reinício automático do Debezium no Software B")
    p.add_argument("--max-em-voo", type=int, default=2000)
    p.add_argument("--build", action="store_true", help="reconstrói a imagem dos serviços ao subir")
    p.add_argument("--saida", default=None, help="pasta dos resultados (padrão: resultados/cenarioN)")


CAMPOS_CONSOLIDADO = ["execucao", "modo", "repeticao", "duracao_falha_s", "commitadas", "perda_eventos", "perda_pct",
                      "perda_mensagens", "duplicacao", "violacao_ordem", "div_eventos_ausentes", "div_visitantes", "div_itens",
                      "api_sucesso_falha", "api_p99_falha_ms", "api_p99_regime_ms", "retomada_emissao_s",
                      "retomada_entrega_s", "convergencia_s", "recuperados_retentativa", "declarados_perdidos",
                      "reinicios_auto_conector"]


def consolidar(arquivo: Path, r: dict, rep: int, extras: dict | None = None) -> None:
    extras = extras or {}
    novo = not arquivo.exists()
    i, t, api = r["integridade"], r["tempos"], r["api"]
    rec = r.get("recuperacao", {})
    linha = {
        "execucao": r["execucao"], "modo": r["modo"], "repeticao": rep, "duracao_falha_s": r["duracao_falha_s"],
        "commitadas": i["operacoes_commitadas"], "perda_eventos": i["perda_eventos"],
        "perda_pct": i["perda_eventos_pct"], "perda_mensagens": i["perda_mensagens"],
        "duplicacao": i["duplicacao_mensagens"], "violacao_ordem": i["violacao_ordem"],
        "div_eventos_ausentes": i["divergencia_final"].get("eventos_ausentes_no_destino", ""),
        "div_visitantes": i["divergencia_final"].get("visitantes_divergentes", ""),
        "div_itens": i["divergencia_final"].get("itens_divergentes", ""),
        "api_sucesso_falha": api["falha"]["taxa_sucesso"], "api_p99_falha_ms": api["falha"]["latencia_p99_ms"],
        "api_p99_regime_ms": api["regime"]["latencia_p99_ms"],
        "retomada_emissao_s": t.get("retomada_emissao_s"), "retomada_entrega_s": t.get("retomada_entrega_s"),
        "convergencia_s": t.get("convergencia_s"),
        "recuperados_retentativa": rec.get("recuperados_por_retentativa", ""),
        "declarados_perdidos": rec.get("declarados_perdidos", ""),
        "reinicios_auto_conector": rec.get("reinicios_automaticos", ""), **extras,
    }
    with open(arquivo, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(linha))
        if novo:
            w.writeheader()
        w.writerow(linha)


def executar(cls, a: argparse.Namespace, saida_padrao: str, rotulo: str, reiniciar, extras_consolidado=None) -> None:
    """Laço de repetições × softwares. `reiniciar(modo)` sobe o ambiente limpo para a execução."""
    modos = ["domain-events", "cdc"] if a.modo == "ambos" else [a.modo]
    amb.definir_recuperacao(not a.sem_recuperacao)
    saida = amb.RAIZ / (a.saida or saida_padrao)
    saida.mkdir(parents=True, exist_ok=True)
    for rep in range(1, a.repeticoes + 1):
        for modo in modos:
            nome = f"{datetime.now():%Y%m%d-%H%M%S}_{modo}_{rotulo}_rep{rep}"
            pasta = saida / nome
            pasta.mkdir()
            reiniciar(modo)
            a.build = False
            a.rodada_inicial = 0
            info = info_seed(amb.ORIGEM_DSN)
            print(f"{info['total'] - info['carregadas']} operações no conjunto de reprodução "
                  f"({info['arquivo']}, {info['carregadas']} na carga inicial)")
            r = asyncio.run(cls(modo, a, pasta, rotulo).rodar(info))
            imprimir(r)
            consolidar(saida / "consolidado.csv", r, rep, extras_consolidado(r) if extras_consolidado else None)
    print(f"\nResultados em {saida}")
