"""Prepara o dataset RetailRocket para o protótipo: coloca os CSVs do Kaggle em
data/retailrocket/ e gera os arquivos que o seed e a carga leem.

O dataset (https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) exige login no
Kaggle, por isso não está no repositório. Quatro jeitos de trazê-lo:

  # 1. com a CLI do Kaggle (precisa de ~/.kaggle/kaggle.json)
  python -m scripts.dados.preparar_retailrocket --kaggle

  # 2. a partir do zip baixado pelo navegador (botão Download da página do dataset)
  python -m scripts.dados.preparar_retailrocket --zip ~/Downloads/archive.zip

  # 3. a partir de uma pasta com os CSVs já extraídos
  python -m scripts.dados.preparar_retailrocket --pasta ~/Downloads/retailrocket

  # 4. só refazer a preparação dos CSVs que já estão em data/retailrocket/
  python -m scripts.dados.preparar_retailrocket

O que a preparação faz (uma vez; leva uns minutos no dataset inteiro):

- events.csv (2,7 milhões de linhas, fora de ordem no arquivo) é ordenado por horário. Cada
  linha vira uma operação de comportamento: view, addtocart ou transaction.
- item_properties_part1/2.csv (20 milhões de linhas, fotografias semanais das propriedades)
  é filtrado para as duas propriedades que o dataset publica em claro, categoryid e available;
  as demais são hashes sem significado. A primeira observação de cada item forma o catálogo
  inicial (itens.csv). Daí em diante, só as observações que mudam o valor viram operação
  (property): repetir na fotografia seguinte o mesmo valor não é uma mudança de negócio.
- As duas listas são intercaladas por horário em operacoes.csv. Empates mantêm a ordem
  original, com os eventos de comportamento antes das mudanças de propriedade.
- category_tree.csv é copiado para categorias.csv.

Com --sem-propriedades, só os eventos de comportamento entram em operacoes.csv.

Depois disso basta subir o backend: o seed lê data/retailrocket/ (RETAILROCKET_DATA_DIR).
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import time
import zipfile
from collections import Counter
from pathlib import Path

from scripts.comum.retailrocket import (ARQ_CATEGORIAS, ARQ_ITENS, ARQ_OPERACOES, ARQS_KAGGLE, COLUNAS_CATEGORIAS,
                                        COLUNAS_EVENTOS, COLUNAS_ITENS, COLUNAS_OPERACOES, COLUNAS_PROPRIEDADES,
                                        PROPRIEDADES, TIPOS_COMPORTAMENTO)

RAIZ = Path(__file__).resolve().parents[2]
MARCADOR_SINTETICO = "_SINTETICO"


def _n(v: int) -> str:
    return f"{v:,}".replace(",", ".")


def _cabecalho(arquivo: Path, esperado: list[str]) -> dict[str, int]:
    with open(arquivo, newline="", encoding="utf-8-sig") as f:
        cab = [c.strip() for c in next(csv.reader(f))]
    faltando = [c for c in esperado if c not in cab]
    if faltando:
        raise SystemExit(f"{arquivo}: colunas ausentes {faltando}; esperado {','.join(esperado)}")
    return {c: i for i, c in enumerate(cab)}


def ler_eventos(arquivo: Path) -> list[tuple]:
    """(timestamp, ordem, tipo, visitante, item, transação) de cada linha de events.csv."""
    idx = _cabecalho(arquivo, COLUNAS_EVENTOS)
    it, iv, ie, ii, itx = (idx[c] for c in COLUNAS_EVENTOS)
    out = []
    with open(arquivo, newline="", encoding="utf-8-sig") as f:
        r = csv.reader(f)
        next(r)
        for n, l in enumerate(r):
            tipo = l[ie]
            if tipo not in TIPOS_COMPORTAMENTO:
                raise SystemExit(f"{arquivo}: linha {n + 2}: tipo de evento desconhecido {tipo!r}")
            out.append((int(l[it]), n, tipo, int(l[iv]), int(l[ii]), l[itx]))
    return out


def ler_propriedades(arquivos: list[Path]) -> tuple[list[tuple], int]:
    """(timestamp, ordem, item, propriedade, valor) das linhas de categoryid e available.
    Devolve também o total de linhas lidas (todas as propriedades)."""
    out, total, ordem = [], 0, 0
    for arquivo in arquivos:
        idx = _cabecalho(arquivo, COLUNAS_PROPRIEDADES)
        it, ii, ip, iv = (idx[c] for c in COLUNAS_PROPRIEDADES)
        with open(arquivo, newline="", encoding="utf-8-sig") as f:
            next(f)
            for linha in f:
                total += 1
                # Filtro rápido antes do parser: 99% das linhas são propriedades com hash.
                if ",categoryid," not in linha and ",available," not in linha:
                    continue
                l = next(csv.reader([linha]))
                if l[ip] not in PROPRIEDADES:
                    continue
                out.append((int(l[it]), ordem, int(l[ii]), l[ip], l[iv].strip()))
                ordem += 1
    return out, total


def preparar(pasta: Path, com_propriedades: bool = True) -> dict:
    t0 = time.time()
    arquivos = {a: pasta / a for a in ARQS_KAGGLE}
    faltando = [a for a, p in arquivos.items() if not p.exists()]
    if faltando:
        raise SystemExit(f"arquivos ausentes em {pasta}: {', '.join(faltando)}. Use --kaggle, --zip ou --pasta.")

    print(f"lendo {arquivos['events.csv']}")
    eventos = ler_eventos(arquivos["events.csv"])
    print(f"  {_n(len(eventos))} eventos de comportamento ({time.time() - t0:.0f}s)")
    print("lendo item_properties_part1.csv e item_properties_part2.csv (só categoryid e available)")
    props, total_props = ler_propriedades([arquivos["item_properties_part1.csv"], arquivos["item_properties_part2.csv"]])
    print(f"  {_n(len(props))} de {_n(total_props)} linhas são categoryid ou available ({time.time() - t0:.0f}s)")

    # Catálogo inicial e mudanças: percorre as propriedades em ordem de horário.
    props.sort()
    catalogo: dict[int, dict[str, str]] = {}
    mudancas = []
    for ts, ordem, item, prop, valor in props:
        atual = catalogo.setdefault(item, {})
        if prop not in atual:
            atual[prop] = valor  # primeira observação: entra no catálogo inicial
        elif atual[prop] != valor:
            atual[prop] = valor
            mudancas.append((ts, 1, ordem, "property", "", item, "", prop, valor))
    # O catálogo inicial guarda o primeiro valor observado, não o último.
    iniciais: dict[int, dict[str, str]] = {}
    for ts, ordem, item, prop, valor in props:
        iniciais.setdefault(item, {}).setdefault(prop, valor)
    del props
    print(f"  {_n(len(iniciais))} itens no catálogo inicial, {_n(len(mudancas))} mudanças de propriedade")

    operacoes = [(ts, 0, n, tipo, str(v), item, tx, "", "") for ts, n, tipo, v, item, tx in eventos]
    del eventos
    if com_propriedades:
        operacoes.extend(mudancas)
    else:
        print("  --sem-propriedades: as mudanças de propriedade ficam fora da carga")
    operacoes.sort()

    destino_ops = pasta / ARQ_OPERACOES
    with open(destino_ops, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS_OPERACOES)
        for ts, _, _, tipo, visitante, item, tx, prop, valor in operacoes:
            w.writerow([ts, tipo, visitante, item, tx, prop, valor])
    tipos = Counter(o[3] for o in operacoes)
    print(f"{destino_ops}: {_n(len(operacoes))} operações ({time.time() - t0:.0f}s)")

    with open(pasta / ARQ_ITENS, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUNAS_ITENS)
        for item in sorted(iniciais):
            p = iniciais[item]
            w.writerow([item, p.get("categoryid", ""), p.get("available", "")])

    idx = _cabecalho(arquivos["category_tree.csv"], COLUNAS_CATEGORIAS)
    with open(arquivos["category_tree.csv"], newline="", encoding="utf-8-sig") as fi, \
            open(pasta / ARQ_CATEGORIAS, "w", newline="") as fo:
        r, w = csv.reader(fi), csv.writer(fo)
        next(r)
        w.writerow(COLUNAS_CATEGORIAS)
        categorias = 0
        for l in r:
            w.writerow([l[idx["categoryid"]], l[idx["parentid"]]])
            categorias += 1

    resumo = {
        "operacoes": len(operacoes), "por_tipo": dict(tipos), "itens_catalogo": len(iniciais),
        "categorias": categorias, "linhas_propriedades_lidas": total_props,
        "primeiro_timestamp": operacoes[0][0] if operacoes else None,
        "ultimo_timestamp": operacoes[-1][0] if operacoes else None,
        "mudancas_de_propriedade_incluidas": com_propriedades,
        "sintetico": (pasta / MARCADOR_SINTETICO).exists(), "duracao_s": round(time.time() - t0, 1),
    }
    (pasta / "preparacao.json").write_text(json.dumps(resumo, indent=2, ensure_ascii=False))
    print("  tipos: " + ", ".join(f"{t} {_n(n)}" for t, n in tipos.most_common()))
    print(f"  {_n(categorias)} categorias; preparação concluída em {resumo['duracao_s']}s")
    return resumo


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--kaggle", action="store_true", help="baixa com a CLI do Kaggle")
    g.add_argument("--zip", type=Path, help="zip baixado da página do dataset")
    g.add_argument("--pasta", type=Path, help="pasta com os CSVs já extraídos")
    p.add_argument("--destino", type=Path, default=RAIZ / "data" / "retailrocket")
    p.add_argument("--sem-propriedades", action="store_true",
                   help="só os eventos de comportamento viram operação (o catálogo inicial continua sendo gerado)")
    a = p.parse_args()
    a.destino.mkdir(parents=True, exist_ok=True)

    if a.kaggle:
        cmd = ["kaggle", "datasets", "download", "-d", "retailrocket/ecommerce-dataset", "-p", str(a.destino), "--unzip"]
        print("$ " + " ".join(cmd))
        try:
            subprocess.run(cmd, check=True)
        except FileNotFoundError:
            raise SystemExit("CLI do Kaggle não encontrada: pip install kaggle e configure ~/.kaggle/kaggle.json")
    elif a.zip:
        with zipfile.ZipFile(a.zip) as z:
            for nome in z.namelist():
                base = Path(nome).name
                if base in ARQS_KAGGLE:
                    print(f"extraindo {nome} para {a.destino / base}")
                    with z.open(nome) as src, open(a.destino / base, "wb") as dst:
                        shutil.copyfileobj(src, dst, 1 << 20)
    elif a.pasta:
        for nome in ARQS_KAGGLE:
            origem = a.pasta / nome
            if origem.exists() and origem.resolve() != (a.destino / nome).resolve():
                print(f"copiando {origem} para {a.destino / nome}")
                shutil.copyfile(origem, a.destino / nome)
        if (a.pasta / MARCADOR_SINTETICO).exists():
            shutil.copyfile(a.pasta / MARCADOR_SINTETICO, a.destino / MARCADOR_SINTETICO)

    preparar(a.destino, com_propriedades=not a.sem_propriedades)
    if (a.destino / MARCADOR_SINTETICO).exists():
        print(f"ATENÇÃO: {a.destino} tem o marcador {MARCADOR_SINTETICO}; apague-o se esta pasta agora tem o dataset real")
    print("\nPronto. Garanta RETAILROCKET_DATA_DIR apontando para esta pasta no .env e suba o backend.")


if __name__ == "__main__":
    main()
