"""Testes de carga sobre o backend que estiver no ar (Domain Events ou CDC).

Três perfis:

  constante     taxa fixa por um tempo; mede vazão, latência da API e atraso de propagação.
  rajada        N operações o mais rápido possível (limitado por --concorrencia); mede quanto
                tempo a fila leva para entregar tudo ao destino.
  represamento  para o consumidor, gera N operações, religa o consumidor e mede o tempo de
                drenagem do acúmulo: é o caso em que as filas têm muitas mensagens para
                sincronizar ao mesmo tempo.

O modo (domain-events ou cdc) é detectado pelo /healthz do tracking-service.
As operações vêm do conjunto de reprodução do RetailRocket (a parcela fora da carga inicial)
e, quando ele se esgota, de clones com event_id novo. No CDC cada evento de comportamento vira
três mensagens (o evento, o visitante e o item) e cada mudança de propriedade duas (o evento e
o item); no Domain Events, uma por operação.

Exemplos:
  python -m scripts.carga.teste_carga constante --taxa 200 --duracao 300
  python -m scripts.carga.teste_carga rajada --operacoes 50000 --concorrencia 300
  python -m scripts.carga.teste_carga represamento --operacoes 100000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import urllib.request
from datetime import datetime
from pathlib import Path

from scripts.carga.gerador import GeradorCarga
from scripts.comum import ambiente as amb
from scripts.comum.analise import analisar, imprimir
from scripts.comum.coleta import Coletor
from scripts.comum.retailrocket import FluxoOperacoes, info_seed


def modo_no_ar() -> str:
    with urllib.request.urlopen(f"{amb.API_URL}/healthz", timeout=5) as r:
        return json.load(r)["mode"]


async def drenar(coletor: Coletor, gerador: GeradorCarga, limite_s: float, sem_progresso_s: float,
                 log) -> dict:
    """Espera o destino receber todas as operações commitadas e convergir com a origem.
    Devolve tempos e vazão."""
    await coletor.amostrar()
    t0 = time.time()
    msgs0 = coletor.mensagens_recebidas
    ultimo_progresso, ultimo_total = t0, msgs0
    pico_gap = 0
    while True:
        await coletor.amostrar()
        gap = len(gerador.commitadas - coletor.recebidos)
        pico_gap = max(pico_gap, gap)
        agora = time.time()
        if coletor.mensagens_recebidas != ultimo_total:
            ultimo_progresso, ultimo_total = agora, coletor.mensagens_recebidas
        if int(agora - t0) % 5 == 0:
            log(f"drenando: faltam {gap} operações, {coletor.mensagens_recebidas - msgs0} mensagens recebidas")
        # Toda operação ter ao menos uma mensagem não basta no CDC (são até três por operação):
        # a drenagem só termina quando visitantes, itens e eventos também batem com a origem.
        if gap == 0 and await coletor.divergencia() == 0:
            break
        if agora - ultimo_progresso > sem_progresso_s or agora - t0 > limite_s:
            log(f"drenagem interrompida com {gap} operações sem mensagem no destino")
            break
        await asyncio.sleep(1)
    dur = time.time() - t0
    recebidas = coletor.mensagens_recebidas - msgs0
    return {"drenagem_s": round(dur, 2), "mensagens_drenadas": recebidas,
            "vazao_consumo_msgs_s": round(recebidas / dur, 1) if dur >= 1 else None,
            "operacoes_sem_mensagem_ao_fim": len(gerador.commitadas - coletor.recebidos), "pico_gap": pico_gap}


async def executar(a: argparse.Namespace, modo: str, pasta: Path) -> dict:
    info = info_seed(amb.ORIGEM_DSN)
    inicio = time.time()
    marcos = {"modo": modo, "inicio": inicio, "parametros": {k: v for k, v in vars(a).items() if k != "func"}}
    coletor = Coletor(inicio)
    await coletor.conectar()
    rodada = 0 if a.reset else int(time.time())
    taxa = a.taxa if a.perfil == "constante" else 0
    conc = a.concorrencia if a.perfil != "constante" else a.max_em_voo
    gerador = GeradorCarga(amb.API_URL, FluxoOperacoes(info, inicio_rodada=rodada), taxa,
                           pasta / "ledger.csv", max_em_voo=conc)

    def log(msg):
        linha = f"[{datetime.now():%H:%M:%S}] [{a.perfil} {modo}] {msg}"
        print(linha, flush=True)
        with open(pasta / "execucao.log", "a") as f:
            f.write(linha + "\n")

    consumidor = amb.MODOS[modo]["consumidor"]
    resumo: dict = {"perfil": a.perfil, "modo": modo}
    parar = asyncio.Event()

    if a.perfil == "represamento":
        log(f"parando o consumidor {consumidor}")
        amb.compose("stop", consumidor)
        marcos["consumidor_parado"] = time.time()

    t_carga = time.time()
    if a.perfil == "constante":
        async def cronometro():
            await asyncio.sleep(a.duracao)
            parar.set()
        asyncio.create_task(cronometro())
        log(f"carga constante de {a.taxa} ops/s por {a.duracao:.0f}s")
        await gerador.executar(parar)
    else:
        log(f"enviando {a.operacoes} operações (concorrência {conc})")
        await gerador.executar(parar, max_operacoes=a.operacoes)
    dur_carga = time.time() - t_carga
    gerador.fechar()
    marcos["carga_fim"] = time.time()
    resumo.update(operacoes_enviadas=gerador.total.enviadas, operacoes_commitadas=len(gerador.commitadas),
                  erros_api=gerador.total.erro, duracao_carga_s=round(dur_carga, 2),
                  vazao_api_ops_s=round(gerador.total.enviadas / dur_carga, 1))
    log(f"{gerador.total.enviadas} operações em {dur_carga:.1f}s ({resumo['vazao_api_ops_s']} ops/s), "
        f"{gerador.total.erro} erros")

    if a.perfil == "represamento":
        await coletor.amostrar()
        log(f"religando o consumidor com {len(gerador.commitadas - coletor.recebidos)} operações represadas")
        marcos["consumidor_religado"] = time.time()
        amb.compose("start", consumidor)

    resumo.update(await drenar(coletor, gerador, a.limite_drenagem, a.sem_progresso, log))
    marcos["fim"] = time.time()
    await coletor.exportar(pasta)
    await coletor.fechar()
    (pasta / "marcos.json").write_text(json.dumps(marcos, indent=2, default=str))
    completo = analisar(pasta)
    resumo["api"] = completo["api"]["regime"]
    resumo["integridade"] = completo["integridade"]
    (pasta / "resumo_carga.json").write_text(json.dumps(resumo, indent=2, ensure_ascii=False))
    imprimir(completo)
    print(json.dumps({k: v for k, v in resumo.items() if k not in ("api", "integridade")}, indent=2))
    return resumo


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="perfil", required=True)
    c = sub.add_parser("constante")
    c.add_argument("--taxa", type=float, default=200)
    c.add_argument("--duracao", type=float, default=300)
    c.add_argument("--max-em-voo", type=int, default=2000)
    for nome in ("rajada", "represamento"):
        s = sub.add_parser(nome)
        s.add_argument("--operacoes", type=int, default=50000)
        s.add_argument("--concorrencia", type=int, default=200, help="requisições simultâneas")
    for s in sub.choices.values():
        s.add_argument("--limite-drenagem", type=float, default=3600)
        s.add_argument("--sem-progresso", type=float, default=120)
        s.add_argument("--reset", action="store_true",
                       help="reinicializa o ambiente antes (e usa os event_id originais do dataset)")
        s.add_argument("--modo", choices=list(amb.MODOS), help="com --reset, qual backend subir")
        s.add_argument("--saida", default="resultados/carga")
    a = p.parse_args()

    if a.reset:
        if not a.modo:
            raise SystemExit("--reset exige --modo")
        amb.reiniciar_ambiente(a.modo)
    modo = modo_no_ar()
    pasta = amb.RAIZ / a.saida / f"{datetime.now():%Y%m%d-%H%M%S}_{modo}_{a.perfil}"
    pasta.mkdir(parents=True)
    asyncio.run(executar(a, modo, pasta))
    print(f"\nResultados em {pasta}")


if __name__ == "__main__":
    main()
