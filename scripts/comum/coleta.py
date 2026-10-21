"""Coleta de métricas nos bancos de origem e destino durante e ao fim de uma execução."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from scripts.comum.ambiente import DESTINO_DSN, ORIGEM_DSN, SLOT

# Estado comparado entre origem e destino, como texto: versão e contadores do funil; no item,
# também categoria e disponibilidade.
ESTADO_VISITANTE = "concat_ws('|', version, views, add_to_carts, purchases)"
ESTADO_ITEM = ("concat_ws('|', version, coalesce(category_id::text, '-'), coalesce(available::text, '-'), "
               "views, add_to_carts, purchases)")


class Coletor:
    """Amostra o estado das duas bases. Mantém em memória os correlation_ids já recebidos
    no destino, lendo received_events de forma incremental.

    No Domain Events uma mensagem gera até três linhas em received_events (o evento e os
    agregados tocados, sempre com uma linha de evento); no CDC cada mensagem gera uma. As
    mensagens são contadas assim: todas as linhas no CDC, só as de evento no Domain Events."""

    def __init__(self, inicio_epoch: float):
        self.inicio = datetime.fromtimestamp(inicio_epoch, tz=timezone.utc)
        self.recebidos: set[str] = set()
        self.mensagens_recebidas = 0
        self.ultimo_recebimento: datetime | None = None
        self._ultimo_id = 0
        self._id_inicial = 0
        self.origem: psycopg.AsyncConnection | None = None
        self.destino: psycopg.AsyncConnection | None = None
        self.dsn = {"origem": ORIGEM_DSN, "destino": DESTINO_DSN}

    async def conectar(self) -> None:
        self.destino = await psycopg.AsyncConnection.connect(DESTINO_DSN, autocommit=True)
        cur = await self.destino.execute("SELECT coalesce(max(id), 0) FROM received_events")
        self._id_inicial = self._ultimo_id = (await cur.fetchone())[0]
        await self._garantir("origem")

    async def _garantir(self, qual: str) -> psycopg.AsyncConnection | None:
        """Devolve a conexão, reconectando se preciso. None se o banco não responder:
        no Cenário 1 o banco de origem do CDC pode cair por falta de disco."""
        conn = getattr(self, qual)
        if conn is not None and not conn.closed:
            return conn
        try:
            conn = await psycopg.AsyncConnection.connect(self.dsn[qual], autocommit=True, connect_timeout=3)
        except psycopg.Error:
            conn = None
        setattr(self, qual, conn)
        return conn

    async def _consultar(self, qual: str, sql: str, params=()) -> list | None:
        conn = await self._garantir(qual)
        if conn is None:
            return None
        try:
            cur = await conn.execute(sql, params)
            return await cur.fetchall()
        except psycopg.OperationalError:
            await conn.close()
            setattr(self, qual, None)
            return None

    async def fechar(self) -> None:
        for c in (self.origem, self.destino):
            if c is not None:
                await c.close()

    async def amostrar(self) -> dict:
        """Uma amostra: disco/WAL/slot na origem e mensagens novas no destino.
        Campos da origem ficam vazios (e origem_disponivel=0) se ela não responder."""
        a: dict = {k: "" for k in ("origem_db_bytes", "origem_wal_bytes", "origem_agregados_tocados",
                                   "slot_ativo", "slot_retido_bytes", "slot_atraso_bytes")}
        r = await self._consultar("origem", """
            SELECT pg_database_size(current_database()),
                   (SELECT coalesce(sum(size), 0) FROM pg_ls_waldir()),
                   (SELECT count(*) FROM visitors WHERE updated_at >= %s)
                   + (SELECT count(*) FROM items WHERE updated_at >= %s)""", (self.inicio, self.inicio))
        a["origem_disponivel"] = int(r is not None)
        if r:
            a["origem_db_bytes"], a["origem_wal_bytes"], a["origem_agregados_tocados"] = r[0]
            slot = await self._consultar("origem", """
                SELECT active,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
                FROM pg_replication_slots WHERE slot_name = %s""", (SLOT,))
            if slot:
                a["slot_ativo"], a["slot_retido_bytes"], a["slot_atraso_bytes"] = slot[0]

        novas = await self._consultar(
            "destino", """SELECT id, correlation_id::text, received_at, mensagem
                          FROM (SELECT id, correlation_id, received_at,
                                       source = 'cdc' OR entity = 'event' AS mensagem
                                FROM received_events WHERE id > %s) r ORDER BY id""",
            (self._ultimo_id,)) or []
        n = 0
        for id_, corr, quando, mensagem in novas:
            if corr:
                self.recebidos.add(corr)
            n += mensagem
            self._ultimo_id = id_
            self.ultimo_recebimento = quando
        self.mensagens_recebidas += n
        a["destino_mensagens_novas"] = n
        a["destino_mensagens_total"] = self.mensagens_recebidas
        a["destino_correlacoes_distintas"] = len(self.recebidos)
        return a

    async def divergencia(self) -> int | str:
        """Visitantes e itens tocados na execução cujo estado (versão e contadores; no item,
        também categoria e disponibilidade) difere entre origem e destino, mais os eventos da
        execução que ainda não chegaram ao destino. Vazio se a origem não responder."""
        total = 0
        for tabela, chave, estado in (("visitors", "visitor_id", ESTADO_VISITANTE), ("items", "item_id", ESTADO_ITEM)):
            o = await self._consultar(
                "origem", f"SELECT {chave}, {estado} FROM {tabela} WHERE updated_at >= %s", (self.inicio,))
            d = await self._consultar(
                "destino", f"SELECT {chave}, {estado} FROM {tabela} WHERE applied_at >= %s", (self.inicio,))
            if o is None or d is None:
                return ""
            destino = dict(d)
            total += sum(1 for k, e in o if destino.get(k) != e)
        eo = await self._consultar("origem", "SELECT count(*) FROM events WHERE created_at >= %s", (self.inicio,))
        ed = await self._consultar("destino", "SELECT count(*) FROM events WHERE created_at >= %s", (self.inicio,))
        if eo is None or ed is None:
            return ""
        return total + max(0, eo[0][0] - ed[0][0])

    async def exportar(self, pasta: Path) -> list[str]:
        """Grava os dados brutos da execução, para que a análise possa ser refeita offline.
        Devolve os arquivos que não puderam ser exportados (banco fora do ar)."""
        consultas = [
            ("origem", "origem_visitantes.csv",
             f"""SELECT visitor_id, {ESTADO_VISITANTE} AS estado, version, last_event_id,
                       last_correlation_id::text, updated_at
                FROM visitors WHERE updated_at >= %s ORDER BY visitor_id""", (self.inicio,)),
            ("destino", "destino_visitantes.csv",
             f"""SELECT visitor_id, {ESTADO_VISITANTE} AS estado, version, last_event_id,
                       last_correlation_id::text, applied_at
                FROM visitors WHERE applied_at >= %s ORDER BY visitor_id""", (self.inicio,)),
            ("origem", "origem_itens.csv",
             f"""SELECT item_id, {ESTADO_ITEM} AS estado, version, last_event_id,
                       last_correlation_id::text, updated_at
                FROM items WHERE updated_at >= %s ORDER BY item_id""", (self.inicio,)),
            ("destino", "destino_itens.csv",
             f"""SELECT item_id, {ESTADO_ITEM} AS estado, version, last_event_id,
                       last_correlation_id::text, applied_at
                FROM items WHERE applied_at >= %s ORDER BY item_id""", (self.inicio,)),
            ("origem", "origem_eventos.csv",
             """SELECT event_id, event_type, visitor_id, item_id, property, value,
                       correlation_id::text, created_at
                FROM events WHERE created_at >= %s ORDER BY event_id""", (self.inicio,)),
            ("destino", "destino_eventos.csv",
             """SELECT event_id, correlation_id::text, created_at, applied_at
                FROM events WHERE created_at >= %s ORDER BY event_id""", (self.inicio,)),
            ("destino", "destino_mensagens.csv",
             """SELECT id, received_at, source, topic, kafka_partition, kafka_offset, kafka_timestamp,
                       event_type, entity, entity_key, version, correlation_id::text, applied
                FROM received_events WHERE id > %s ORDER BY id""", (self._id_inicial,)),
        ]
        falhas = []
        for qual, arquivo, sql, params in consultas:
            conn = await self._garantir(qual)
            try:
                if conn is None:
                    raise psycopg.OperationalError(f"banco de {qual} indisponível")
                await self._exportar(conn, pasta / arquivo, sql, params)
            except psycopg.OperationalError:
                falhas.append(arquivo)
                setattr(self, qual, None)
        return falhas

    @staticmethod
    async def _exportar(conn, arquivo: Path, sql: str, params) -> None:
        cur = conn.cursor()
        await cur.execute(sql, params)
        nomes = [d.name for d in cur.description]
        with open(arquivo, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(nomes)
            while lote := await cur.fetchmany(5000):
                w.writerows(lote)
