"""Cálculo das métricas de uma execução a partir dos arquivos brutos gravados na pasta dela.

Uso: python -m scripts.comum.analise resultados/<execucao>

Arquivos lidos: marcos.json, ledger.csv, serie_temporal.csv, origem_visitantes.csv,
destino_visitantes.csv, origem_itens.csv, destino_itens.csv, origem_eventos.csv,
destino_eventos.csv e destino_mensagens.csv.
Grava resumo.json na mesma pasta.

Definições (Seção 4.5 e Cenário 1):
- operação commitada: resposta 2xx da API (o serviço só responde depois do commit);
  operações sem resposta (timeout) cujo evento existe na origem com o mesmo
  correlation_id também contam.
- perda de eventos: operação commitada da qual nenhuma mensagem chegou ao destino.
- perda de mensagens: mensagens esperadas e não recebidas. No Domain Events cada operação
  gera 1 mensagem; no CDC gera 1 por linha alterada: o evento e cada agregado tocado (3 num
  evento de comportamento: evento, visitante e item; 2 numa mudança de propriedade: evento e
  item).
- duplicação: mensagens extras para a mesma (correlation_id, entidade, chave).
- violação de ordem: mensagem de visitante ou de item com versão menor que outra já recebida
  do mesmo agregado, reportada também por entidade.
- divergência final: visitantes e itens tocados cujo estado (versão, contadores e, no item,
  categoria e disponibilidade) difere entre origem e destino, e eventos da execução ausentes no
  destino.

Seção "recuperacao": o que a recuperação automática fez. No Software A, eventos confirmados
pelo broker depois que a requisição desistiu de esperar, eventos publicados por uma
retentativa e eventos declarados perdidos; a diferença entre a perda medida pelo oráculo e os
declarados perdidos é a perda que nenhum contador registrou. No Software B, reinícios
automáticos da tarefa do Debezium e se o vigia desistiu.
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def _ts(v: str) -> float | None:
    return datetime.fromisoformat(v).timestamp() if v else None


def _ler(p: Path) -> list[dict]:
    if not p.exists():
        return []
    with open(p, newline="") as f:
        return list(csv.DictReader(f))


def _pct(valores: list[float], q: float) -> float | None:
    if not valores:
        return None
    if len(valores) == 1:
        return round(valores[0], 2)
    return round(statistics.quantiles(valores, n=100, method="inclusive")[int(q) - 1], 2)


def _fase(t: float, m: dict) -> str:
    if m.get("falha_inicio") is None or t < m["falha_inicio"]:
        return "regime"
    if m.get("falha_fim") is None or t < m["falha_fim"]:
        return "falha"
    return "recuperacao"


def _ok(r: dict) -> bool:
    return r["http_status"].isdigit() and 200 <= int(r["http_status"]) < 300


def analisar(pasta: Path) -> dict:
    m = json.loads((pasta / "marcos.json").read_text())
    modo = m["modo"]
    ledger = _ler(pasta / "ledger.csv")
    serie = _ler(pasta / "serie_temporal.csv")
    msgs = _ler(pasta / "destino_mensagens.csv")
    vis_o = {r["visitor_id"]: r["estado"] for r in _ler(pasta / "origem_visitantes.csv")}
    vis_d = {r["visitor_id"]: r["estado"] for r in _ler(pasta / "destino_visitantes.csv")}
    itens_o = {r["item_id"]: r["estado"] for r in _ler(pasta / "origem_itens.csv")}
    itens_d = {r["item_id"]: r["estado"] for r in _ler(pasta / "destino_itens.csv")}
    ev_o = {r["event_id"]: r for r in _ler(pasta / "origem_eventos.csv")}
    ev_d = {r["event_id"] for r in _ler(pasta / "destino_eventos.csv")}
    corr_na_origem = {r["correlation_id"] for r in ev_o.values()}

    # ------------------------------------------------------------------ API por fase
    api: dict[str, dict] = {}
    por_fase: dict[str, list[dict]] = defaultdict(list)
    for r in ledger:
        por_fase[_fase(float(r["t_envio"]), m)].append(r)
    for fase in ("regime", "falha", "recuperacao"):
        rs = por_fase.get(fase, [])
        ok = [r for r in rs if _ok(r)]
        lat = [float(r["latencia_ms"]) for r in rs]
        api[fase] = {
            "operacoes": len(rs),
            "sucesso": len(ok),
            "taxa_sucesso": round(len(ok) / len(rs), 4) if rs else None,
            "latencia_p50_ms": _pct(lat, 50),
            "latencia_p99_ms": _pct(lat, 99),
        }

    # ------------------------------------------------------------------ oráculo
    commitadas: dict[str, dict] = {}
    ambiguas = 0
    for r in ledger:
        st = int(r["http_status"]) if r["http_status"].isdigit() else 0
        if 200 <= st < 300:
            commitadas[r["correlation_id"]] = r
        elif st == 0:
            if r["correlation_id"] in corr_na_origem:
                commitadas[r["correlation_id"]] = r
            else:
                ambiguas += 1

    # No Domain Events a mensagem tem uma linha de evento e as linhas dos agregados; no CDC
    # cada linha é uma mensagem. "principal" marca uma linha por mensagem.
    def principal(x: dict) -> bool:
        return modo == "cdc" or x["entity"] == "event"

    unidades: dict[str, Counter] = defaultdict(Counter)
    for x in msgs:
        if x["correlation_id"] and principal(x):
            unidades[x["correlation_id"]][(x["entity"], x["entity_key"])] += 1

    def esperadas(op: dict) -> int:
        return 1 + int(op["n_agregados"]) if modo == "cdc" else 1

    perdidas_por_fase: Counter = Counter()
    commitadas_por_fase: Counter = Counter()
    perda_msgs = esperadas_total = 0
    for corr, op in commitadas.items():
        fase = _fase(float(op["t_envio"]), m)
        commitadas_por_fase[fase] += 1
        recebidas = unidades.get(corr)
        if not recebidas:
            perdidas_por_fase[fase] += 1
        e = esperadas(op)
        esperadas_total += e
        perda_msgs += max(0, e - (len(recebidas) if recebidas else 0))

    duplicadas = sum(n - 1 for c in unidades.values() for n in c.values() if n > 1)
    mensagens_recebidas = sum(1 for x in msgs if principal(x))

    violacoes_por_entidade: Counter = Counter()
    maior: dict[tuple[str, str], int] = {}
    for x in msgs:  # já ordenadas por id (ordem de chegada)
        if x["entity"] not in ("visitor", "item") or not x["version"]:
            continue
        v, k = int(x["version"]), (x["entity"], x["entity_key"])
        if v < maior.get(k, 0):
            violacoes_por_entidade[x["entity"]] += 1
        maior[k] = max(v, maior.get(k, 0))
    violacoes = sum(violacoes_por_entidade.values())

    # ------------------------------------------------------------------ divergência final
    vis_div = sum(1 for k, e in vis_o.items() if vis_d.get(k) != e)
    itens_div = sum(1 for k, e in itens_o.items() if itens_d.get(k) != e)
    ev_ausentes = sum(1 for k in ev_o if k not in ev_d)

    # ------------------------------------------------------------------ tempos
    fi, ff = m.get("falha_inicio"), m.get("falha_fim")
    tempos: dict[str, float | None] = {}
    if ff is not None:
        emit = [t for x in msgs if (t := _ts(x["kafka_timestamp"])) is not None and t >= ff]
        entr = [t for x in msgs if (t := _ts(x["received_at"])) is not None and t >= ff]
        tempos["retomada_emissao_s"] = round(min(emit) - ff, 2) if emit else None
        tempos["retomada_entrega_s"] = round(min(entr) - ff, 2) if entr else None
        conv = [float(s["t"]) for s in serie
                if float(s["t"]) >= ff and s.get("divergencia") not in ("", None) and int(s["divergencia"]) == 0]
        tempos["convergencia_s"] = round(conv[0] - ff, 2) if conv else None
    if fi is not None:
        regime = [s for s in serie if float(s["t"]) < fi]
        falha = [s for s in serie if float(s["t"]) >= fi]
        base_gap = max((int(s["gap"]) for s in regime), default=0)
        tempos["deteccao_gap_s"] = next((round(float(s["t"]) - fi, 2) for s in falha
                                         if int(s["gap"]) > max(base_gap * 2, 10)), None)
        if modo == "domain-events":
            base = float(regime[-1]["publicacao_falhas_total"] or 0) if regime else 0.0
            tempos["deteccao_erro_publicacao_s"] = next(
                (round(float(s["t"]) - fi, 2) for s in falha
                 if s["publicacao_falhas_total"] and float(s["publicacao_falhas_total"]) > base), None)
        else:
            tempos["deteccao_conector_s"] = next(
                (round(float(s["t"]) - fi, 2) for s in falha if s["conector_estado"] not in ("RUNNING", "")), None)
            base_slot = max((int(s["slot_atraso_bytes"] or 0) for s in regime), default=0)
            tempos["deteccao_slot_s"] = next(
                (round(float(s["t"]) - fi, 2) for s in falha
                 if s["slot_atraso_bytes"] and int(s["slot_atraso_bytes"]) > max(2 * base_slot, 16 << 20)), None)

        # Momento em que o banco de origem deixou de responder (ex.: disco esgotado pelo WAL retido).
        tempos["origem_indisponivel_s"] = next(
            (round(float(s["t"]) - fi, 2) for s in falha if s.get("origem_disponivel") == "0"), None)

    # ------------------------------------------------------------------ efeito colateral
    def maximo(campo: str, fase: str) -> int | None:
        vals = [int(s[campo]) for s in serie if s.get(campo) not in ("", None) and _fase(float(s["t"]), m) == fase]
        return max(vals) if vals else None

    colateral = {
        "wal_max_bytes": {f: maximo("origem_wal_bytes", f) for f in ("regime", "falha", "recuperacao")},
        "db_max_bytes": {f: maximo("origem_db_bytes", f) for f in ("regime", "falha", "recuperacao")},
    }
    if modo == "cdc":
        colateral["slot_retido_max_bytes"] = {f: maximo("slot_retido_bytes", f) for f in ("regime", "falha", "recuperacao")}
        colateral["estados_conector_observados"] = sorted({s["conector_estado"] for s in serie if s.get("conector_estado")})

    divergencia_final = {
        "visitantes_tocados": len(vis_o),
        "visitantes_divergentes": vis_div,
        "itens_tocados": len(itens_o),
        "itens_divergentes": itens_div,
        "eventos_da_execucao": len(ev_o),
        "eventos_ausentes_no_destino": ev_ausentes,
    }
    faltantes = m.get("exportacoes_faltantes", [])
    if any(f.startswith("origem_") for f in faltantes):
        # Sem a origem não há com o que comparar (ex.: banco caiu por disco cheio).
        divergencia_final = {"indisponivel": "banco de origem fora do ar ao fim da execução"}
    if faltantes:
        colateral["exportacoes_faltantes"] = faltantes
    total_commit = len(commitadas)
    resumo = {
        "execucao": pasta.name,
        "modo": modo,
        "parametros": m.get("parametros", {}),
        "duracao_falha_s": round(ff - fi, 1) if fi is not None and ff is not None else None,
        "api": api,
        "integridade": {
            "operacoes_commitadas": total_commit,
            "operacoes_commitadas_por_fase": dict(commitadas_por_fase),
            "operacoes_commitadas_por_tipo": dict(Counter(op["tipo"] for op in commitadas.values())),
            "operacoes_ambiguas_excluidas": ambiguas,
            "perda_eventos": sum(perdidas_por_fase.values()),
            "perda_eventos_por_fase": dict(perdidas_por_fase),
            "perda_eventos_pct": round(100 * sum(perdidas_por_fase.values()) / total_commit, 4) if total_commit else None,
            "mensagens_esperadas": esperadas_total,
            "mensagens_recebidas": mensagens_recebidas,
            "perda_mensagens": perda_msgs,
            "duplicacao_mensagens": duplicadas,
            "violacao_ordem": violacoes,
            "violacao_ordem_por_entidade": dict(violacoes_por_entidade),
            "divergencia_final": divergencia_final,
        },
        "tempos": tempos,
        "efeito_colateral": colateral,
    }
    resumo["recuperacao"] = _recuperacao(m, serie, resumo["integridade"]["perda_eventos"])
    (pasta / "resumo.json").write_text(json.dumps(resumo, indent=2, ensure_ascii=False))
    return resumo


def _acumulado(serie: list[dict], campo: str) -> int | None:
    """Total de um contador ao longo da execução, somando o que foi contado antes de cada
    reinício do processo (o /metrics volta a zero quando o serviço recomeça)."""
    total, anterior, visto = 0.0, 0.0, False
    for s in serie:
        if s.get(campo) in ("", None):
            continue
        v, visto = float(s[campo]), True
        total += v - anterior if v >= anterior else v
        anterior = v
    return int(total) if visto else None


def _recuperacao(m: dict, serie: list[dict], perda_eventos: int) -> dict:
    politica = m.get("parametros", {}).get("recuperacao_automatica", {})
    if m["modo"] == "domain-events":
        declarados = _acumulado(serie, "declarados_perdidos_total")
        pend = [int(float(s["retentativa_pendentes"])) for s in serie if s.get("retentativa_pendentes") not in ("", None)]
        return {
            "politica": {k: v for k, v in politica.items() if k.startswith("PUBLISH_RETRY")},
            "confirmados_com_atraso": _acumulado(serie, "confirmados_com_atraso_total"),
            "recuperados_por_retentativa": _acumulado(serie, "retentativa_recuperados_total"),
            "declarados_perdidos": declarados,
            "pendentes_max": max(pend) if pend else None,
            "perda_nao_declarada": perda_eventos - declarados if declarados is not None else None,
        }
    desistiu = [s for s in serie if str(s.get("conector_vigia_desistiu", "")) in ("1", "1.0")]
    return {
        "politica": {k: v for k, v in politica.items() if k.startswith("CONNECT_RESTART")},
        "reinicios_automaticos": _acumulado(serie, "conector_reinicios_auto_total"),
        "vigia_desistiu": bool(desistiu),
        "desistencia_apos_falha_s": round(float(desistiu[0]["t"]) - m["falha_inicio"], 2)
        if desistiu and m.get("falha_inicio") else None,
        "intervencao_manual": "intervencao_conector" in m,
    }


def imprimir(r: dict) -> None:
    i = r["integridade"]
    print(f"\n=== {r['execucao']} ({r['modo']}) — falha de {r['duracao_falha_s']} s ===")
    for fase, a in r["api"].items():
        print(f"  API {fase:<12} ops={a['operacoes']:<7} sucesso={a['taxa_sucesso']}  "
              f"p50={a['latencia_p50_ms']} ms  p99={a['latencia_p99_ms']} ms")
    print(f"  commitadas={i['operacoes_commitadas']}  perda de eventos={i['perda_eventos']} "
          f"({i['perda_eventos_pct']}%) por fase={i['perda_eventos_por_fase']}")
    print(f"  mensagens esperadas={i['mensagens_esperadas']} recebidas={i['mensagens_recebidas']} "
          f"perdidas={i['perda_mensagens']} duplicadas={i['duplicacao_mensagens']} fora de ordem={i['violacao_ordem']}")
    print(f"  divergência final={i['divergencia_final']}")
    print(f"  tempos={r['tempos']}")
    print(f"  efeito colateral={r['efeito_colateral']}")
    print(f"  recuperacao={r['recuperacao']}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    imprimir(analisar(Path(sys.argv[1])))
