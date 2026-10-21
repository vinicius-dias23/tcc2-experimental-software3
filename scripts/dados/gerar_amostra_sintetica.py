"""Gera uma amostra SINTÉTICA no mesmo formato dos CSVs do dataset RetailRocket.

Serve para validar o pipeline sem o dataset real (que exige login no Kaggle). Os quatro
arquivos (events.csv, item_properties_part1.csv, item_properties_part2.csv e
category_tree.csv) têm as colunas e o formato dos originais: horários em milissegundos entre
maio e setembro de 2015, events.csv fora de ordem, propriedades em fotografias semanais com as
anonimizadas em hash. As proporções imitam as do RetailRocket: ~96,7% de view, ~2,5% de
addtocart e ~0,8% de transaction, popularidade dos itens concentrada (Zipf), a maioria dos
visitantes com poucos eventos e alguns muito ativos, compras agrupadas por transactionid.
NÃO usar nos experimentos do TCC.

A pasta recebe um arquivo _SINTETICO, que a preparação e o seed detectam e registram em
seed_info.synthetic.

Uso:
  python -m scripts.dados.gerar_amostra_sintetica --eventos 200000 --saida data/retailrocket-sintetico
  python -m scripts.dados.preparar_retailrocket --destino data/retailrocket-sintetico
  echo "RETAILROCKET_DATA_DIR=./data/retailrocket-sintetico" >> .env
"""

from __future__ import annotations

import argparse
import bisect
import csv
import random
from pathlib import Path

from scripts.comum.retailrocket import COLUNAS_CATEGORIAS, COLUNAS_EVENTOS, COLUNAS_PROPRIEDADES

INICIO_MS = 1430622000000   # 2015-05-03, primeiro dia do RetailRocket
FIM_MS = 1442545200000      # 2015-09-18, último dia
SEMANA_MS = 7 * 24 * 3600 * 1000
TIPOS = [("view", 0.967), ("addtocart", 0.025), ("transaction", 0.008)]
PROPRIEDADES_HASH = ["888", "790", "917", "364", "283", "159", "112", "678", "202", "776"]


def _cumulativa(pesos: list[float]) -> list[float]:
    acc, out = 0.0, []
    for p in pesos:
        acc += p
        out.append(acc)
    return out


def gerar(n_eventos: int, saida: Path, semente: int) -> None:
    rnd = random.Random(semente)
    saida.mkdir(parents=True, exist_ok=True)

    # Árvore de categorias: ~1.700 no original; aqui proporcional, com 3 níveis.
    n_cat = max(30, min(1669, n_eventos // 150))
    raizes = list(range(max(3, n_cat // 50)))
    categorias = {c: None for c in raizes}
    for c in range(len(raizes), n_cat):
        categorias[c] = rnd.choice(list(categorias))
    folhas = [c for c in categorias if c not in set(categorias.values())]

    # Itens: ~235 mil no events.csv original para 2,7 milhões de eventos (1 para 12).
    n_itens = max(100, n_eventos // 12)
    itens = list(range(n_itens))
    rnd.shuffle(itens)
    pop = _cumulativa([1 / (i + 1) ** 0.6 for i in range(n_itens)])  # Zipf: poucos itens muito vistos

    # Visitantes: ~1,4 milhão para 2,7 milhões de eventos; a maioria só aparece uma vez.
    n_vis = max(50, n_eventos // 2)
    ativ = _cumulativa([1 / (i + 1) ** 0.6 for i in range(n_vis)])

    def escolher(cum: list[float]) -> int:
        return bisect.bisect_left(cum, rnd.random() * cum[-1])

    eventos, transacao = [], 0
    while len(eventos) < n_eventos:
        ts = rnd.randint(INICIO_MS, FIM_MS)
        visitante = escolher(ativ)
        tipo = rnd.choices([t for t, _ in TIPOS], [p for _, p in TIPOS])[0]
        item = itens[escolher(pop)]
        if tipo == "transaction":
            transacao += 1
            # Uma compra pode ter vários itens, com o mesmo horário e o mesmo transactionid.
            for _ in range(1 + (rnd.random() < 0.2) * rnd.randint(1, 3)):
                eventos.append((ts, visitante, "transaction", item, transacao))
                item = itens[escolher(pop)]
        else:
            eventos.append((ts, visitante, tipo, item, ""))
    eventos = eventos[:n_eventos]
    # No original o arquivo não está ordenado por horário.
    rnd.shuffle(eventos)
    with open(saida / "events.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS_EVENTOS)
        w.writerows(eventos)

    # Propriedades: fotografias semanais de cada item. categoryid muda raramente e available
    # alterna com mais frequência; as demais propriedades são hashes ("n552.000 639502 ...").
    linhas = []
    for item in range(n_itens):
        categoria = rnd.choice(folhas)
        disponivel = int(rnd.random() < 0.6)
        ts = INICIO_MS + rnd.randint(0, SEMANA_MS)
        hashes = rnd.sample(PROPRIEDADES_HASH, 3)
        while ts < FIM_MS:
            if rnd.random() < 0.005:
                categoria = rnd.choice(folhas)
            if rnd.random() < 0.05:
                disponivel = 1 - disponivel
            linhas.append((ts, item, "categoryid", categoria))
            linhas.append((ts, item, "available", disponivel))
            for h in hashes:
                linhas.append((ts, item, h, f"n{rnd.randint(1, 999)}.000 {rnd.randint(100000, 999999)}"))
            ts += SEMANA_MS
    rnd.shuffle(linhas)
    meio = len(linhas) // 2
    for nome, parte in (("item_properties_part1.csv", linhas[:meio]), ("item_properties_part2.csv", linhas[meio:])):
        with open(saida / nome, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(COLUNAS_PROPRIEDADES)
            w.writerows(parte)

    with open(saida / "category_tree.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS_CATEGORIAS)
        for c, pai in categorias.items():
            w.writerow([c, "" if pai is None else pai])

    (saida / "_SINTETICO").write_text("Amostra sintética gerada por scripts/dados/gerar_amostra_sintetica.py. "
                                      "Não usar nos experimentos.\n")
    print(f"{len(eventos)} eventos, {n_itens} itens, {len(linhas)} linhas de propriedades e "
          f"{len(categorias)} categorias sintéticos em {saida}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--eventos", type=int, default=200000)
    p.add_argument("--saida", default="data/retailrocket-sintetico")
    p.add_argument("--semente", type=int, default=42)
    a = p.parse_args()
    gerar(a.eventos, Path(a.saida), a.semente)


if __name__ == "__main__":
    main()
