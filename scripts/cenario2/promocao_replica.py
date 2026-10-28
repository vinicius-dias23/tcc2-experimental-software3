"""Cenário 2 — Promoção de réplica após queda do nó primário.

O banco de origem ganha uma réplica física assíncrona (postgres-origem-replica). Sob carga,
o primário é encerrado sem aviso (SIGKILL), a réplica é promovida e o nome `postgres-origem`
passa a apontar para ela, como o endpoint de um banco gerenciado faz no failover. O
tracking-service e o Debezium reconectam ao mesmo nome sem nenhuma mudança de configuração.

Fases de cada execução:

  1. regime        : carga constante com primário e réplica saudáveis;
  2. falha         : a réplica sai da rede por --atraso-replicacao segundos (partição de rede:
                     as transações commitadas no primário nesse intervalo não chegam a ela); o
                     primário cai; depois de --atraso-promocao segundos a réplica é promovida
                     e assume o endereço;
  3. recuperação   : a carga continua contra a réplica promovida;
  4. estabilização : a carga para e a coleta segue até convergir ou parar de progredir.

Logo após a promoção, o script grava em origem_pos_promocao.csv os eventos desta execução que
a réplica promovida tem. A análise compara isso com o ledger para achar as operações
confirmadas ao cliente que a promoção apagou e, delas, as que chegaram ao destino (eventos
órfãos). Como cada operação grava uma linha própria em `events`, a comparação é direta, pelo
correlation_id, sem depender das versões dos agregados.

Exemplos (na raiz do repositório, com o venv ativo):

  python -m scripts.cenario2.promocao_replica --modo ambos
  python -m scripts.cenario2.promocao_replica --modo cdc --regime 30 --recuperacao 60 --atraso-replicacao 5

Saída em resultados/cenario2/<execução>/: os mesmos arquivos do Cenário 1, mais
origem_pos_promocao.csv e a seção "promocao" do resumo.json.
"""

from __future__ import annotations

import argparse
import asyncio
import time

import psycopg

from scripts.comum import ambiente as amb
from scripts.comum.execucao import ExecucaoBase, argumentos_comuns, executar


class Promocao(ExecucaoBase):
    cenario = 2
    campos_extra = ["origem_no"]

    def parametros(self) -> dict:
        a = self.args
        return {**super().parametros(), "atraso_replicacao_s": a.atraso_replicacao,
                "atraso_promocao_s": a.atraso_promocao, "reiniciar_conector_falho": a.reiniciar_conector_falho}

    async def falha(self) -> None:
        a = self.args
        self.marcos["falha_inicio"] = time.time()
        if a.atraso_replicacao > 0:
            self.log(f"FALHA: réplica isolada da rede por {a.atraso_replicacao:g}s (replicação interrompida)")
            await amb.isolar_replica()
            self.marcos["replicacao_interrompida"] = time.time()
            await asyncio.sleep(a.atraso_replicacao)
        self.marcos["lsn_primario_na_queda"] = await _consultar(amb.ORIGEM_DSN, "SELECT pg_current_wal_lsn()::text")

        async def lsn_replica() -> None:
            out = await amb.psql_replica(
                "SELECT coalesce(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::text")
            self.marcos["lsn_replica_na_promocao"] = out.strip() or None

        self.log("FALHA: derrubando o primário")
        self.marcos.update(await amb.failover_origem(a.atraso_promocao, self.log, apos_queda=lsn_replica,
                                                     apos_promocao=self._retratar_replica))
        await self.coletor.trocar_origem(amb.REPLICA_DSN)
        self.marcos["falha_fim"] = time.time()

    async def _retratar_replica(self) -> None:
        """O que a réplica promovida tem dos eventos desta execução, lido logo depois da
        promoção e antes de ela assumir o endereço (portanto, antes de qualquer escrita nova)."""
        # Por `docker compose exec`: a réplica pode estar fora da rede neste momento.
        out = await amb.psql_replica(
            f"""COPY (SELECT event_id, correlation_id::text AS correlation_id, created_at
                        FROM events WHERE created_at >= to_timestamp({float(self.marcos['inicio'])})
                       ORDER BY event_id) TO STDOUT WITH CSV HEADER""")
        (self.pasta / "origem_pos_promocao.csv").write_text(out)
        self.log(f"retrato da réplica promovida: {max(0, len(out.splitlines()) - 1)} eventos desta execução")

    async def amostra_extra(self) -> dict:
        return {"origem_no": "replica_promovida" if self.coletor.dsn["origem"] == amb.REPLICA_DSN else "primario"}

    def servicos_para_logs(self) -> list[str]:
        return [*super().servicos_para_logs(), amb.PRIMARIO, amb.REPLICA]


async def _consultar(dsn: str, sql: str):
    try:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True, connect_timeout=3) as c:
            cur = await c.execute(sql)
            return (await cur.fetchone())[0]
    except psycopg.Error:
        return None


def subir_com_replica(modo: str, build: bool) -> None:
    amb.reiniciar_ambiente(modo, build=build)
    print(f"[ambiente] subindo {amb.REPLICA}")
    amb.compose("up", "-d", "--no-deps", "--wait", "--wait-timeout", "300", amb.REPLICA)
    limite = time.time() + 120
    while time.time() < limite:
        # Além da réplica, só o Debezium (Software B) abre conexão de replicação.
        with psycopg.connect(amb.ORIGEM_DSN) as c:
            r = c.execute("SELECT state FROM pg_stat_replication WHERE application_name NOT LIKE 'Debezium%'").fetchall()
        if any(s == "streaming" for (s,) in r):
            print("[ambiente] réplica em streaming")
            return
        time.sleep(1)
    raise SystemExit("a réplica não entrou em streaming")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    argumentos_comuns(p)
    p.add_argument("--atraso-replicacao", type=float, default=5,
                   help="segundos com a réplica fora da rede antes da queda do primário; as transações desse "
                        "intervalo não chegam à réplica (0 = só o atraso natural da replicação; padrão: 5)")
    p.add_argument("--atraso-promocao", type=float, default=5,
                   help="segundos entre a queda do primário e a promoção da réplica (padrão: 5)")
    a = p.parse_args()

    def extras(r: dict) -> dict:
        pr = r["promocao"]
        return {"perdidas_na_promocao": pr["transacoes_perdidas_na_promocao"], "eventos_orfaos": pr["eventos_orfaos"],
                "eventos_so_no_destino": pr["conteudo_final"]["eventos_so_no_destino"],
                "destino_versao_maior": pr["conteudo_final"]["agregados_com_versao_maior_no_destino"],
                "conteudo_diferente": pr["conteudo_final"]["mesma_versao_conteudo_diferente"],
                "slot_recriado_s": pr["slot_recriado_apos_promocao_s"]}

    executar(Promocao, a, "resultados/cenario2", f"atraso{a.atraso_replicacao:g}s",
             lambda modo: subir_com_replica(modo, a.build), extras)


if __name__ == "__main__":
    main()
