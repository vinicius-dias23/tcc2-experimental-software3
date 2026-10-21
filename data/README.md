# Dados

Coloque aqui, em `data/retailrocket/`, os quatro CSVs do dataset
[RetailRocket](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) (`events.csv`,
`item_properties_part1.csv`, `item_properties_part2.csv` e `category_tree.csv`) e rode a preparação,
que gera `operacoes.csv`, `itens.csv` e `categorias.csv` na mesma pasta. O jeito mais simples é
`python -m scripts.dados.preparar_retailrocket --zip <zip baixado>` ou `--kaggle`.
O conteúdo desta pasta não vai para o Git. Instruções completas no README principal (seção Dados).
