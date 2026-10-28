"""Cenário 5 — Queda de consumidor e rebalanceamento do grupo.

O consumidor (analytics-service) roda em --instancias contêineres do mesmo grupo, cada um com
--workers membros, todos lendo em lotes como um consumidor Java: até --max-poll-records
mensagens por lote, e um membro que passa de --max-poll-interval-ms processando um lote sai
do grupo, o que dispara um rebalanceamento. Sob carga alta, uma instância é encerrada
(SIGKILL por padrão) por --janela segundos e depois religada.

O limite precisa estar calibrado perto do tempo real de processamento de um lote cheio. Para
medir esse tempo nesta máquina, rode antes com --calibrar: o script represa a fila (para
os consumidores por --represamento segundos), mede os lotes cheios durante a drenagem e
sugere o valor de --max-poll-interval-ms para cada software.

Fases de cada execução:

  1. regime        : carga alta com todas as instâncias no ar;
  2. falha         : a instância --instancia-derrubada cai por --janela segundos; o grupo só
                     percebe depois do CONSUMER_SESSION_TIMEOUT (10 s) e rebalanceia;
  3. recuperação   : a instância volta (novo rebalanceamento) e a carga continua;
  4. estabilização : a carga para e a coleta segue até drenar e convergir.

O estado do grupo é lido do coordenador a cada --intervalo-grupo segundos (GET /group de uma
instância que não caiu) e gravado em grupo.csv; a análise conta os rebalanceamentos e o tempo
fora do estado Stable por fase (seção "grupo" do resumo.json).

Exemplos (na raiz do repositório, com o venv ativo):

  python -m scripts.cenario5.queda_consumidor --modo ambos --calibrar
  python -m scripts.cenario5.queda_consumidor --modo ambos --max-poll-interval-ms 900
  python -m scripts.cenario5.queda_consumidor --modo cdc --taxa 300 --janela 60 --regime 30 --recuperacao 60
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import time

import aiohttp

from scripts.comum import ambiente as amb
from scripts.comum.execucao import ExecucaoBase, argumentos_comuns, executar


class Acumulador:
    """Soma contadores de várias instâncias sem perder o que foi contado antes de uma delas
    reiniciar (o contador do /metrics volta a zero quando o processo recomeça)."""

    def __init__(self):
        self.ultimo: dict[tuple[int, str], float] = {}
        self.total: dict[str, float] = {}

    def somar(self, instancia: int, metricas: dict[str, float]) -> None:
        for nome, v in metricas.items():
            ant = self.ultimo.get((instancia, nome))
            inc = v if ant is None or v < ant else v - ant
            self.total[nome] = self.total.get(nome, 0) + inc
            self.ultimo[(instancia, nome)] = v


METRICAS = ["messages_consumed_total", "group_expulsions_total", "poll_batches_total", "poll_batch_ms_sum",
            "poll_full_batches_total", "poll_full_batch_ms_sum"]


class QuedaConsumidor(ExecucaoBase):
    cenario = 5
    campos_extra = ["grupo_estado", "grupo_membros", "consumidor_mensagens", "consumidor_expulsoes",
                    "lotes_cheios", "lote_cheio_ms_soma", "lote_ms_max"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.instancias = list(range(1, self.args.instancias + 1))
        self.acum = Acumulador()
        self.lote_max = 0.0
        self.grupo: dict = {}

    def parametros(self) -> dict:
        a = self.args
        return {**super().parametros(), "instancias": a.instancias, "workers_por_instancia": a.workers,
                "max_poll_interval_ms": a.max_poll_interval_ms, "max_poll_records": a.max_poll_records,
                "instancia_derrubada": a.instancia_derrubada, "janela_s": a.janela, "parada": a.parada}

    def observadora(self) -> int:
        """Instância que responde pelo estado do grupo: a primeira que não é derrubada."""
        return next(i for i in self.instancias if i != self.args.instancia_derrubada)

    async def falha(self) -> None:
        a = self.args
        alvo = amb.consumidor(self.modo, a.instancia_derrubada)
        self.marcos["falha_inicio"] = time.time()
        self.log(f"FALHA: {a.parada} em {alvo} por {a.janela:g}s")
        args = ["kill", "-s", "SIGKILL", alvo] if a.parada == "kill" else [a.parada, alvo]
        rc, out = await amb.compose_async(*args)
        if rc != 0:
            raise RuntimeError(f"não foi possível derrubar {alvo}: {out}")
        await asyncio.sleep(a.janela)
        self.log(f"religando {alvo}")
        rc, out = await (amb.compose_async("unpause", alvo) if a.parada == "pause" else amb.ligar_async(alvo))
        if rc != 0:
            raise RuntimeError(f"não foi possível religar {alvo}: {out}")
        self.marcos["falha_fim"] = time.time()

    def tarefas_extras(self, parar: asyncio.Event) -> list:
        return [self._ler_grupo(parar)]

    async def _ler_grupo(self, parar: asyncio.Event) -> None:
        url = f"{amb.url_consumidor(self.observadora())}/group"
        with open(self.pasta / "grupo.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["t", "fase", "estado", "membros", "erro"])
            w.writeheader()
            while not parar.is_set():
                linha = {"t": f"{time.time():.3f}", "fase": self.fase, "estado": "", "membros": "", "erro": ""}
                try:
                    async with self.sessao.get(url, timeout=aiohttp.ClientTimeout(total=3)) as r:
                        if r.status == 200:
                            g = await r.json()
                            linha.update(estado=g["state"], membros=g["members"])
                        else:
                            linha["erro"] = (await r.text()).strip()[:200]
                except Exception as e:
                    linha["erro"] = type(e).__name__
                if linha["estado"] and linha["estado"] != self.grupo.get("estado"):
                    self.log(f"grupo: {self.grupo.get('estado') or '?'} → {linha['estado']} ({linha['membros']} membros)")
                if linha["estado"]:
                    self.grupo = {"estado": linha["estado"], "membros": linha["membros"]}
                w.writerow(linha)
                f.flush()
                try:
                    await asyncio.wait_for(parar.wait(), self.args.intervalo_grupo)
                except asyncio.TimeoutError:
                    pass

    async def amostra_extra(self) -> dict:
        lidas = await asyncio.gather(*(amb.ler_metricas(self.sessao, amb.url_consumidor(i)) for i in self.instancias))
        for i, met in zip(self.instancias, lidas):
            self.acum.somar(i, {k: met[k] for k in METRICAS if k in met})
            self.lote_max = max(self.lote_max, met.get("poll_batch_ms_max", 0))
        t = self.acum.total
        return {"grupo_estado": self.grupo.get("estado", ""), "grupo_membros": self.grupo.get("membros", ""),
                "consumidor_mensagens": int(t.get("messages_consumed_total", 0)),
                "consumidor_expulsoes": int(t.get("group_expulsions_total", 0)),
                "lotes_cheios": int(t.get("poll_full_batches_total", 0)),
                "lote_cheio_ms_soma": int(t.get("poll_full_batch_ms_sum", 0)), "lote_ms_max": int(self.lote_max)}

    def resumo_amostra(self, linha: dict) -> str:
        return (super().resumo_amostra(linha) + f" grupo={linha['grupo_estado']}/{linha['grupo_membros']}"
                f" expulsões={linha['consumidor_expulsoes']}")

    def servicos_para_logs(self) -> list[str]:
        return [*super().servicos_para_logs(), *(amb.consumidor(self.modo, i) for i in self.instancias[1:])]


class Calibracao(QuedaConsumidor):
    """Represa a fila parando todos os consumidores e mede os lotes cheios na drenagem."""

    async def falha(self) -> None:
        a = self.args
        alvos = [amb.consumidor(self.modo, i) for i in self.instancias]
        self.marcos["falha_inicio"] = time.time()
        # Parada limpa (SIGTERM) em vez de congelar: um lote congelado no meio do processamento
        # entraria na média com a duração inteira do represamento.
        self.log(f"CALIBRAÇÃO: consumidores parados por {a.represamento:g}s para represar a fila")
        await amb.compose_async("stop", *alvos)
        await asyncio.sleep(a.represamento)
        await amb.ligar_async(*alvos)
        self.marcos["falha_fim"] = time.time()


def subir(modo: str, a: argparse.Namespace, intervalo_ms: int) -> None:
    os.environ["CONSUMER_WORKERS"] = str(a.workers)
    os.environ["CONSUMER_MAX_POLL_RECORDS"] = str(a.max_poll_records)
    os.environ["CONSUMER_MAX_POLL_INTERVAL"] = f"{intervalo_ms}ms"
    amb.reiniciar_ambiente(modo, build=a.build)
    extras = [amb.consumidor(modo, i) for i in range(2, a.instancias + 1)]
    if extras:
        print(f"[ambiente] subindo {', '.join(extras)}")
        amb.compose("up", "-d", "--no-deps", "--wait", "--wait-timeout", "120", *extras)
    for i in range(1, a.instancias + 1):
        amb.esperar_http(f"{amb.url_consumidor(i)}/healthz", 60)


def calibrar(a: argparse.Namespace) -> None:
    a.janela = 0
    a.saida = a.saida or "resultados/cenario5/calibracao"
    sugestoes = {}

    def registrar(r: dict) -> dict:
        serie = list(csv.DictReader(open(amb.RAIZ / a.saida / r["execucao"] / "serie_temporal.csv")))
        u = serie[-1]
        n, soma = int(u["lotes_cheios"] or 0), int(u["lote_cheio_ms_soma"] or 0)
        medio = soma / n if n else None
        sugestoes[r["modo"]] = {"lotes_cheios": n, "lote_cheio_medio_ms": round(medio, 1) if medio else None,
                                "lote_ms_max": int(u["lote_ms_max"] or 0),
                                "sugestao_max_poll_interval_ms": int(medio * 1.1) if medio else None}
        print(f"\n[calibração] {r['modo']}: {sugestoes[r['modo']]}")
        return {"lotes_cheios": n, "lote_cheio_medio_ms": sugestoes[r["modo"]]["lote_cheio_medio_ms"]}

    # Intervalo de 1 h: leitura em lotes ligada, mas ninguém sai do grupo durante a calibração.
    executar(Calibracao, a, a.saida, "calibracao", lambda modo: subir(modo, a, 3_600_000), registrar)
    destino = amb.RAIZ / a.saida / "calibracao.json"
    destino.write_text(json.dumps({"max_poll_records": a.max_poll_records, "taxa": a.taxa,
                                   "resultado": sugestoes}, indent=2))
    print(f"\nSugestões gravadas em {destino}")
    print("A sugestão é 10% acima do tempo médio de um lote cheio. Para comparar os dois softwares nas mesmas "
          "condições, use um único valor nas execuções dos dois.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    argumentos_comuns(p, taxa=300, regime=60, recuperacao=120)
    p.add_argument("--instancias", type=int, default=3, choices=[2, 3], help="instâncias consumidoras (padrão: 3)")
    p.add_argument("--workers", type=int, default=2, help="membros do grupo por instância (padrão: 2)")
    p.add_argument("--max-poll-interval-ms", type=int, default=1000,
                   help="tempo máximo de processamento de um lote antes de o membro sair do grupo (padrão: 1000; "
                        "calibre com --calibrar)")
    p.add_argument("--max-poll-records", type=int, default=500, help="mensagens por lote (padrão: 500)")
    p.add_argument("--instancia-derrubada", type=int, default=2, help="instância que cai (padrão: 2)")
    p.add_argument("--janela", type=float, default=60, help="segundos com a instância fora do ar (padrão: 60)")
    p.add_argument("--parada", choices=["kill", "stop", "pause"], default="kill",
                   help="kill = SIGKILL (padrão), stop = SIGTERM com saída limpa do grupo, pause = congelada")
    p.add_argument("--intervalo-grupo", type=float, default=0.5, help="intervalo de leitura do estado do grupo (s)")
    p.add_argument("--calibrar", action="store_true",
                   help="só mede o tempo de um lote cheio em cada software e sugere --max-poll-interval-ms")
    p.add_argument("--represamento", type=float, default=30,
                   help="na calibração, segundos com os consumidores parados para acumular mensagens")
    a = p.parse_args()
    if not 1 <= a.instancia_derrubada <= a.instancias:
        p.error("--instancia-derrubada precisa ser uma das instâncias")
    if a.calibrar:
        calibrar(a)
        return

    def extras(r: dict) -> dict:
        g = r["grupo"]
        return {"rebalanceamentos_falha": g["rebalanceamentos"]["falha"],
                "rebalanceamentos_recuperacao": g["rebalanceamentos"]["recuperacao"],
                "nao_estavel_falha_s": g["tempo_nao_estavel_s"]["falha"],
                "nao_estavel_recuperacao_s": g["tempo_nao_estavel_s"]["recuperacao"],
                "expulsoes_falha": g["expulsoes_max_poll"]["falha"],
                "max_poll_interval_ms": a.max_poll_interval_ms}

    executar(QuedaConsumidor, a, "resultados/cenario5", f"poll{a.max_poll_interval_ms}ms",
             lambda modo: subir(modo, a, a.max_poll_interval_ms), extras)


if __name__ == "__main__":
    main()
