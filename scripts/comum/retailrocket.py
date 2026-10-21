"""Leitura do dataset RetailRocket preparado e montagem das operações reproduzidas pela API.

O dataset original (events.csv, item_properties_part1.csv, item_properties_part2.csv e
category_tree.csv) é convertido uma vez por scripts/dados/preparar_retailrocket.py em três
arquivos, lidos pelo seed (cmd/seed/main.go) e por estes scripts:

  operacoes.csv   uma linha por operação de negócio, em ordem de horário: os eventos de
                  comportamento (view, addtocart, transaction) e as mudanças de categoria ou
                  de disponibilidade dos itens (property)
  itens.csv       o catálogo inicial: cada item com a primeira categoria e a primeira
                  disponibilidade observadas
  categorias.csv  a árvore de categorias

A regra de corte é a mesma do seed: cada linha de operacoes.csv recebe o identificador E +
posição (E000000001 é a primeira), consideram-se as primeiras `total` linhas e as primeiras
`carregadas` delas vão para a carga inicial. As demais formam o conjunto de reprodução, usado
como carga transacional. Os dois números ficam gravados em seed_info.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, replace
from pathlib import Path

import psycopg

# Arquivos do Kaggle (retailrocket/ecommerce-dataset).
ARQS_KAGGLE = ["events.csv", "item_properties_part1.csv", "item_properties_part2.csv", "category_tree.csv"]
COLUNAS_EVENTOS = ["timestamp", "visitorid", "event", "itemid", "transactionid"]
COLUNAS_PROPRIEDADES = ["timestamp", "itemid", "property", "value"]
COLUNAS_CATEGORIAS = ["categoryid", "parentid"]

# Arquivos preparados.
ARQ_OPERACOES = "operacoes.csv"
ARQ_ITENS = "itens.csv"
ARQ_CATEGORIAS = "categorias.csv"
COLUNAS_OPERACOES = ["timestamp", "event", "visitorid", "itemid", "transactionid", "property", "value"]
COLUNAS_ITENS = ["itemid", "categoryid", "available"]

TIPOS_COMPORTAMENTO = ("view", "addtocart", "transaction")
PROPRIEDADES = ("categoryid", "available")


def _do_env(chave: str) -> str | None:
    """Variável do processo ou, se ausente, do .env da raiz (o mesmo que o Compose lê)."""
    valor = os.environ.get(chave)
    env = Path(__file__).resolve().parents[2] / ".env"
    if not valor and env.exists():
        for linha in env.read_text().splitlines():
            k, _, v = linha.partition("=")
            if k.strip() == chave and v.strip():
                valor = v.strip().strip('"').strip("'")
    return valor or None


def pasta_dados() -> Path:
    """Mesma pasta montada no contêiner do seed: RETAILROCKET_DATA_DIR do ambiente ou do .env."""
    raiz = Path(__file__).resolve().parents[2]
    return (raiz / (_do_env("RETAILROCKET_DATA_DIR") or "./data/retailrocket")).resolve()


def id_evento(n: int) -> str:
    """Identificador da linha n de operacoes.csv (1 = primeira). Precisa coincidir com o seed."""
    return f"E{n:09d}"


@dataclass(frozen=True)
class Operacao:
    """Uma linha de operacoes.csv, reproduzida como POST /events (comportamento) ou
    POST /items/{id}/properties (mudança de propriedade)."""
    event_id: str
    timestamp: int
    tipo: str
    visitor_id: int | None
    item_id: int
    transaction_id: int | None
    propriedade: str | None
    valor: str | None

    @property
    def comportamento(self) -> bool:
        return self.tipo != "property"

    @property
    def n_agregados(self) -> int:
        """Agregados tocados: visitante e item num evento de comportamento, só o item numa
        mudança de propriedade. No CDC cada um vira uma mensagem, além da do evento."""
        return 2 if self.comportamento else 1

    def caminho(self) -> str:
        return "/events" if self.comportamento else f"/items/{self.item_id}/properties"

    def corpo(self) -> dict:
        if self.comportamento:
            return {"event_id": self.event_id, "timestamp": self.timestamp, "event": self.tipo,
                    "visitor_id": self.visitor_id, "item_id": self.item_id, "transaction_id": self.transaction_id}
        return {"event_id": self.event_id, "timestamp": self.timestamp, "property": self.propriedade,
                "value": self.valor}

    def clone(self, rodada: int) -> "Operacao":
        """Cópia com event_id novo, para reutilizar o conjunto em cargas longas. Visitantes e
        itens são os mesmos, então a cópia movimenta agregados que já existem."""
        return replace(self, event_id=f"{self.event_id}-r{rodada}")


def ler_operacao(n: int, linha: list[str], idx: dict[str, int]) -> Operacao:
    c = lambda nome: linha[idx[nome]]
    inteiro = lambda nome: int(c(nome)) if c(nome) else None
    return Operacao(id_evento(n), int(c("timestamp")), c("event"), inteiro("visitorid"), int(c("itemid")),
                    inteiro("transactionid"), c("property") or None, c("value") or None)


def info_seed(dsn: str) -> dict:
    """Parâmetros da carga inicial gravados pelo seed."""
    with psycopg.connect(dsn) as c:
        r = c.execute("""SELECT seed_fraction, data_file, operations_total, operations_seeded,
                                visitors_seeded, items_seeded, synthetic FROM seed_info""").fetchone()
    if r is None:
        raise SystemExit("seed_info vazio: a carga inicial não rodou")
    info = dict(zip(("fracao", "arquivo", "total", "carregadas", "visitantes", "itens", "sintetico"), r))
    if info["sintetico"]:
        print("ATENÇÃO: o banco foi carregado com a amostra SINTÉTICA, não com o dataset RetailRocket real")
    return info


class FluxoOperacoes:
    """Itera o conjunto de reprodução (linhas carregadas+1 até total). Com repetir=True,
    recomeça com clones de event_id novo quando o conjunto acaba."""

    def __init__(self, info: dict, pasta: Path | None = None, repetir: bool = True, inicio_rodada: int = 0):
        self.caminho = (pasta or pasta_dados()) / info["arquivo"]
        if not self.caminho.exists():
            raise SystemExit(f"{self.caminho} não encontrado. O seed usou {info['arquivo']}; confira "
                             f"RETAILROCKET_DATA_DIR (README, seção Dados).")
        self.inicio, self.fim = info["carregadas"], info["total"]
        if self.fim <= self.inicio:
            raise SystemExit("não sobrou nenhuma operação para a carga: reduza SEED_FRACTION")
        self.repetir, self.rodada = repetir, inicio_rodada
        self._abrir()

    def __len__(self) -> int:
        return self.fim - self.inicio

    def _abrir(self) -> None:
        self._f = open(self.caminho, newline="", encoding="utf-8")
        self._r = csv.reader(self._f)
        cab = next(self._r)
        self._idx = {c.strip(): i for i, c in enumerate(cab)}
        faltando = [c for c in COLUNAS_OPERACOES if c not in self._idx]
        if faltando:
            raise SystemExit(f"{self.caminho}: colunas ausentes {faltando}")
        for _ in range(self.inicio):  # pula a parcela da carga inicial
            next(self._r)
        self._n = self.inicio

    def __iter__(self):
        return self

    def __next__(self) -> Operacao:
        if self._n >= self.fim:
            self._f.close()
            if not self.repetir:
                raise StopIteration
            self.rodada += 1
            self._abrir()
        linha = next(self._r)
        self._n += 1
        op = ler_operacao(self._n, linha, self._idx)
        return op if self.rodada == 0 else op.clone(self.rodada)
