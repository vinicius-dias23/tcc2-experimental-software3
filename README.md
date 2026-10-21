# TCC: protótipo experimental 3 (RetailRocket)

Protótipos do experimento que compara **Domain Events** e **Change Data Capture** como modelos de
propagação de mudanças entre microsserviços sob falhas de disponibilidade. Este repositório usa o
**Dataset 3**, o [RetailRocket](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
(eventos de comportamento de visitantes de um e-commerce), e traz os scripts de carga e de injeção
de falha do **Cenário 1 (fila indisponível)**. A estrutura, os scripts e o painel seguem os
protótipos 1 ([Olist](https://github.com/vinicius-dias23/tcc2-experimental-software1)) e 2
([PaySim](https://github.com/vinicius-dias23/tcc2-experimental-software2)), para que os três
conjuntos de dados sejam medidos da mesma forma.

| | Software A: Domain Events | Software B: Change Data Capture |
|---|---|---|
| Quem propaga | o próprio `tracking-service`, logo após o commit | Debezium (Kafka Connect) lendo o WAL do PostgreSQL |
| Outbox | **não** (janela de dual write exposta de propósito) | não se aplica |
| Mensagens por operação | 1 evento de domínio | 1 por linha alterada: 3 num evento de comportamento (evento, visitante e item), 2 numa mudança de propriedade (evento e item) |
| Subir | `docker compose up backend-domain-events` | `docker compose up backend-change-data-capture` |

Todo o resto é idêntico nos dois: o mesmo binário Go (o modo vem de `PROPAGATION_MODE`), o mesmo
esquema, os mesmos endpoints, o mesmo consumidor, os mesmos bancos e o mesmo cluster Kafka.

**Por que o RetailRocket.** É o conjunto de "alta frequência e granularidade fina" da Seção 4.3:
2,7 milhões de eventos curtos (visualização, inclusão no carrinho e compra) de 1,4 milhão de
visitantes sobre 235 mil itens, em 4,5 meses. Cada operação é pequena e as operações chegam em
grande número, o que põe pressão no volume de mensagens e não no tamanho de cada uma. Visitantes e
itens se repetem muito (poucos itens concentram as visualizações, alguns visitantes geram milhares
de eventos), então há muitas operações concorrentes sobre os mesmos agregados e a ordem por
visitante e por item é mensurável.

## Modelo de domínio

```
                     Software A (domain-events)
                  ┌──────── publica após o commit ─────────┐
                  │                                        ▼
cliente ──HTTP──▶ tracking-service ──▶ postgres-origem   Kafka (3 brokers, KRaft) ──▶ analytics-service ──▶ postgres-destino
 (scripts/carga)  (Go, :8080)          (wal_level=logical)  ▲                         (Go, :8081)
                                              │             │
                                              └── WAL ──▶ Debezium/Kafka Connect (:8083)
                                                   Software B (cdc)
```

- **tracking-service** (origem, serviço de rastreamento da loja). Duas operações de negócio:
  - `POST /events`: evento de comportamento (`view`, `addtocart` ou `transaction`). Numa única
    transação do banco grava a linha em `events`, soma 1 ao contador do tipo e à versão do
    **visitante** e do **item** (criando-os se forem novos). Toca três linhas.
  - `POST /items/{id}/properties`: mudança de **categoria** (`categoryid`) ou de
    **disponibilidade** (`available`) do item. Grava a linha em `events` e atualiza o item,
    somando 1 à versão. Toca duas linhas.
  - Também `GET /events/{id}`, `GET /visitors/{id}`, `GET /items/{id}`, `GET /healthz` e
    `GET /metrics`. O cabeçalho `X-Correlation-ID` é gravado em todas as linhas tocadas e
    propagado até o destino.
- **analytics-service** (destino, serviço de análise de comportamento): consome a fila,
  materializa eventos, visitantes e itens e registra **cada entidade recebida** em
  `received_events`. É dali que saem perda, duplicação e ordem.
- **Categorias**: a árvore de categorias (`category_tree.csv`) é estática e entra nas duas bases
  pela carga inicial; não é propagada.
- **Kafka**: 3 brokers `apache/kafka:3.8.0` em modo KRaft, tópicos com 6 partições, fator de
  replicação 3 e `min.insync.replicas=2`. Tópicos: `retailrocket.domain-events.activity` (A) e
  `retailrocket.cdc.public.events` / `.visitors` / `.items` (B, chave = chave primária).
- **PostgreSQL 16**: um banco por serviço. `wal_level=logical` nos dois modos, para que a origem
  seja idêntica; o slot de replicação só existe no Software B.

Eventos de domínio do Software A: `ItemViewed`, `ItemAddedToCart`, `ItemPurchased` e
`ItemPropertyChanged`. Cada um carrega o evento e o estado dos agregados tocados depois da
mudança. A chave de partição é o **visitante** nos eventos de comportamento (o funil de cada
visitante chega em ordem) e o **item** nas mudanças de propriedade. Como o mesmo item recebe
eventos de visitantes diferentes, por partições diferentes, a ordem do item entre eventos não é
garantida pelo Kafka, só pela guarda de versão do consumidor; isso aparece como violação de ordem
(no PaySim, o mesmo acontecia com a conta de destino).

## Pré-requisitos

- Docker com Compose v2.20 ou mais novo (`docker compose version`). Reserve uns 6 GB de RAM.
- Python 3.10+ para os scripts:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r scripts/requirements.txt
```

## Dados

O dataset exige login no Kaggle, por isso não está no repositório. Os quatro CSVs (`events.csv`,
`item_properties_part1.csv`, `item_properties_part2.csv` e `category_tree.csv`, ~1 GB) vão em
`data/retailrocket/`. O script abaixo põe os arquivos no lugar e **prepara** o dataset (uma vez só;
leva alguns minutos no dataset inteiro):

```bash
python -m scripts.dados.preparar_retailrocket --kaggle                      # CLI do Kaggle (~/.kaggle/kaggle.json)
python -m scripts.dados.preparar_retailrocket --zip ~/Downloads/archive.zip # zip do botão Download
python -m scripts.dados.preparar_retailrocket --pasta ~/Downloads/rr        # CSVs já extraídos
```

**Preparação.** Gera três arquivos na mesma pasta, lidos pelo seed e pela carga:

- `operacoes.csv`: as operações de negócio em ordem de horário. O `events.csv` vem fora de ordem
  no Kaggle e é ordenado. O `item_properties` (20 milhões de linhas, fotografias semanais das
  propriedades) é filtrado para `categoryid` e `available`, as duas propriedades que o dataset
  publica em claro (as demais são hashes); só as observações que **mudam** o valor viram operação,
  porque repetir o mesmo valor na fotografia seguinte não é uma mudança de negócio. Com
  `--sem-propriedades`, só os eventos de comportamento entram.
- `itens.csv`: o catálogo inicial, com a primeira categoria e a primeira disponibilidade
  observadas de cada item.
- `categorias.csv`: a árvore de categorias.

O resumo da preparação (operações por tipo, itens, categorias) fica em `preparacao.json`.

**Carga inicial e carga de teste.** As operações recebem o identificador `E` + posição
(`E000000001` é a primeira). Das primeiras `RETAILROCKET_MAX_OPS` (padrão **1.000.000**; `0` usa
todas), os primeiros 70% (`SEED_FRACTION`) são gravados direto nas duas bases pelo serviço `seed`
quando o ambiente sobe, junto com o catálogo e as categorias, para que os testes não comecem com
bancos vazios. O restante é reproduzido pela API durante os testes, uma requisição por operação.
Quando esse conjunto acaba numa carga longa, ele é reaproveitado com `event_id` novos (visitantes e
itens são os mesmos, então as cópias movimentam agregados que já existem).

O limite de 1 milhão existe pelo mesmo motivo do PaySim: cada execução de cenário recria as bases
do zero (`down -v`). Com 1 milhão, sobram 300 mil operações para a carga (50 min a 100 ops/s antes
de começar a clonar). Para usar o dataset inteiro, `RETAILROCKET_MAX_OPS=0` no `.env`. O valor
usado fica em `seed_info` e nos resultados.

**Amostra sintética (só para validar o pipeline).** Sem o dataset, gere os quatro CSVs no mesmo
formato do Kaggle e prepare-os:

```bash
python -m scripts.dados.gerar_amostra_sintetica --eventos 300000 --saida data/retailrocket-sintetico
python -m scripts.dados.preparar_retailrocket --destino data/retailrocket-sintetico
echo "RETAILROCKET_DATA_DIR=./data/retailrocket-sintetico" > .env
```

A pasta leva um marcador `_SINTETICO`; o seed registra isso em `seed_info.synthetic` e os scripts
avisam no terminal. Não use esses dados no TCC.

## Subindo os protótipos

```bash
docker compose up backend-domain-events          # Software A
docker compose up backend-change-data-capture    # Software B
```

Acrescente `-d` para liberar o terminal. O serviço `backend-*` só fica pronto depois que os bancos
estão saudáveis, a carga inicial terminou, os tópicos existem e (no B) o conector Debezium está
`RUNNING`.

Os dois usam as mesmas portas, então **derrube um antes de subir o outro**. O jeito mais simples de
alternar, que também zera as bases:

```bash
python -m scripts.comum.ambiente cdc             # ou domain-events
# equivalente a:
docker compose --profile domain-events --profile cdc down -v
docker compose up -d --wait backend-change-data-capture
```

Teste rápido da API:

```bash
curl -s localhost:8080/healthz
curl -s -X POST localhost:8080/events -H 'Content-Type: application/json' -d '{
  "event_id": "teste-1", "timestamp": 1438000000000, "event": "addtocart",
  "visitor_id": 257597, "item_id": 355908}'
curl -s -X POST localhost:8080/items/355908/properties -H 'Content-Type: application/json' -d '{
  "event_id": "teste-2", "timestamp": 1438000000001, "property": "available", "value": "0"}'
curl -s localhost:8080/items/355908
curl -s localhost:8081/metrics
```

Portas no host: API `8080`, consumidor `8081`, Kafka Connect `8083` e vigia do conector `8086` (só no
B), banco de origem `5432` e banco de destino `5433` (usuário, senha e base: `retailrocket`). São as
mesmas dos protótipos 1 e 2, então os protótipos não sobem ao mesmo tempo na mesma máquina.

## Painel

Uma página no navegador com o diagrama dos serviços, o estado de cada um ao vivo, a vazão entre
eles e botões para injetar falhas e disparar os scripts. Roda na máquina, com o venv ativo, fora
do Docker Compose (assim sobrevive ao `down -v` que os scripts fazem entre execuções):

```bash
python -m scripts.painel            # abre em http://localhost:8090
```

- **Diagrama.** Mostra o software que estiver no ar (A ou B), com cada contêiner em verde (no ar),
  âmbar (iniciando, não saudável ou sem responder ao `/healthz`), roxo (congelado com `pause`),
  vermelho (parado) ou cinza (não criado). As setas mostram a vazão lida dos `/metrics`, as falhas de
  publicação no A e, no B, o estado da tarefa do Debezium e o atraso do slot de replicação. Clicar
  num serviço abre os detalhes e os botões **Parar** (`stop`), **Matar** (`kill`), **Congelar**
  (`pause`), **Religar** e **Reiniciar**.
- **Injeção rápida.** Atalhos para derrubar a fila inteira, tirar o quórum (2 brokers), derrubar
  1 broker, o consumidor, um dos bancos ou o Debezium, com o tipo de parada escolhido; e **Restaurar
  tudo**, que religa o que estiver parado ou congelado. São os mesmos comandos `docker compose` do
  Cenário 1.
- **Scripts.** Sobe ou troca o backend, gera carga (constante, rajada ou represamento) e executa o
  Cenário 1 com os parâmetros do formulário, mostrando a saída do script. Enquanto o cenário ou a troca
  de backend rodam, os botões de falha ficam desativados para não interferir na execução.
- **Gráfico e linha do tempo.** Vazão dos últimos 5 minutos, com faixas onde algum serviço estava
  fora do ar, e o registro de cada mudança de estado e de cada ação feita pelo painel.

O painel só escuta em `127.0.0.1`, porque controla o Docker. As falhas manuais servem para explorar
o comportamento; as medições do TCC continuam saindo dos scripts, que registram tudo em
`resultados/`.

## Testes de carga

Rodam contra o backend que estiver no ar (o modo é detectado sozinho):

```bash
# taxa constante: vazão, latência da API e atraso de propagação
python -m scripts.carga.teste_carga constante --taxa 200 --duracao 300

# rajada: N operações o mais rápido possível
python -m scripts.carga.teste_carga rajada --operacoes 50000 --concorrencia 300

# represamento: para o consumidor, enche a fila, religa e mede a drenagem do acúmulo
python -m scripts.carga.teste_carga represamento --operacoes 100000 --concorrencia 300
```

O `represamento` é o caso em que as filas têm muitas mensagens para sincronizar ao mesmo tempo: com
100 mil operações, ficam represadas 100 mil mensagens no A e perto de 300 mil no B (três por evento
de comportamento, duas por mudança de propriedade). A drenagem termina quando toda operação
commitada chegou ao destino e visitantes, itens e eventos tocados batem com a origem. A saída vai
para `resultados/carga/<execução>/` (`ledger.csv`, exportações das bases, `resumo.json` e
`resumo_carga.json` com vazão da API, tempo de drenagem e vazão de consumo). `--reset --modo cdc`
reinicializa o ambiente antes.

## Cenário 1: indisponibilidade da fila

```bash
# desenho completo (janelas de 5, 15, 30 e 60 min) para um software
python -m scripts.cenario1.fila_indisponivel --modo domain-events
python -m scripts.cenario1.fila_indisponivel --modo cdc

# os dois softwares, janelas escolhidas, 3 repetições
python -m scripts.cenario1.fila_indisponivel --modo ambos --janelas 5,15 --repeticoes 3

# ensaio rápido
python -m scripts.cenario1.fila_indisponivel --modo cdc --janela-segundos 60 --regime 30 --recuperacao 30
```

Cada execução:

1. reinicializa o ambiente (`down -v` e `up`), o que recarrega as bases, recria os tópicos e o slot;
2. **regime**: carga constante (`--taxa`, padrão 100 ops/s) por `--regime` segundos;
3. **falha**: `docker compose stop` nos três brokers pela duração da janela, com a carga continuando;
4. **recuperação**: sobe os brokers, mantém a carga por `--recuperacao` segundos, para a carga e
   acompanha até origem e destino convergirem ou até não chegar nada por `--sem-progresso` segundos.

Opções de injeção: `--parada kill` (SIGKILL), `--parada pause` (processo congelado e inalcançável) e
`--brokers kafka-2,kafka-3` (derruba só parte do cluster, deixando as partições sem quórum para
`acks=all`). No B, `--reiniciar-conector-falho` faz a intervenção manual: reinicia a tarefa do
Debezium que ficou em `FAILED` depois que o reinício automático desistiu, e registra a intervenção.
`--sem-recuperacao` desliga a recuperação automática dos dois softwares (ver abaixo).

**Por que `--sem-progresso` é 300 s aqui (era 60 s nos protótipos 1 e 2).** Quando os brokers
voltam, o Kafka Connect rebalanceia o grupo e para a tarefa do Debezium. A tarefa antiga fica presa
no produtor e só é encerrada à força uns 90 s depois (`Coordinator didn't stop in the expected
time`); só então a nova tarefa retoma a leitura do WAL a partir do último offset gravado. Nos ensaios
com a amostra sintética, a primeira mensagem do B chegou ao destino ~100 s depois da volta dos
brokers e as bases convergiram sem perda. Com o limite de 60 s, a execução terminava antes disso e
contava como perdido o que ainda estava retido no WAL. O tempo de retomada é medido
(`retomada_emissao_s`) e entra nos resultados.

### O que sai em `resultados/cenario1/<execução>/`

| Arquivo | Conteúdo |
|---|---|
| `ledger.csv` | uma linha por requisição: horário, `event_id`, tipo, visitante, item, agregados tocados, `correlation_id`, status HTTP, latência, versões do visitante e do item |
| `serie_temporal.csv` | amostra a cada `--intervalo` s: sucesso e p99 da API, escritas commitadas, mensagens entregues, lacuna de propagação, divergência entre as bases, falhas de publicação, eventos esperando retentativa, recuperados e declarados perdidos (A), estado do conector e reinícios automáticos (B), tamanho do banco, WAL e WAL retido pelo slot |
| `marcos.json` | instantes de início e fim de cada fase e os parâmetros |
| `origem_*.csv`, `destino_*.csv` | visitantes, itens e eventos tocados na execução e as mensagens recebidas, para refazer a análise offline |
| `logs/` | logs dos serviços, brokers e Kafka Connect |
| `resumo.json` | as métricas da Seção 4.5 calculadas para a execução |

`resultados/cenario1/consolidado.csv` acumula uma linha por execução, com as mesmas colunas dos
protótipos anteriores; as de divergência viram `div_eventos_ausentes`, `div_visitantes` e
`div_itens`. Para recalcular uma execução: `python -m scripts.comum.analise resultados/cenario1/<execução>`.

### Como as métricas são calculadas

- **Operação commitada**: resposta 2xx da API, que só responde depois do commit. Requisições sem
  resposta contam se o evento existe na origem com o mesmo `correlation_id`.
- **Perda de eventos**: operação commitada da qual nenhuma mensagem chegou ao destino (por
  `correlation_id`). Também é reportada a perda de mensagens, porque no CDC cada operação gera duas
  ou três mensagens.
- **Duplicação**: mensagens repetidas para a mesma `(correlation_id, entidade, chave)`.
- **Violação de ordem**: mensagem de visitante ou de item com versão menor do que outra já recebida
  do mesmo agregado, também separada por entidade (`violacao_ordem_por_entidade`).
- **Divergência final**: visitantes e itens tocados na execução cujo estado (versão e contadores;
  no item, também categoria e disponibilidade) difere entre origem e destino, e eventos da execução
  ausentes no destino.
- **Tempo até detecção**: primeira amostra após o início da falha com sinal observável. São
  reportados o sinal genérico (lacuna de propagação acima da linha de base), o erro de publicação no
  A e, no B, a tarefa do conector fora de `RUNNING` ou o atraso do slot acima de 16 MB.
- **Tempo até retomada**: da volta dos brokers até a primeira mensagem **emitida** (timestamp do
  Kafka) e até a primeira mensagem **entregue** ao destino.
- **Tempo até convergência**: da volta dos brokers até a primeira amostra com divergência zero.
- **Efeito colateral**: latência p99 e taxa de sucesso da API por fase; tamanho do banco, do WAL e
  do WAL retido pelo slot por fase.
- **Recuperação**: no A, eventos publicados por uma retentativa, eventos declarados perdidos e a
  **perda não declarada** (perda medida pelo oráculo menos os declarados perdidos), além dos
  confirmados com atraso pelo broker; no B, reinícios automáticos, se o vigia desistiu e quando.

## Recuperação automática

Igual aos protótipos 1 e 2. Os dois softwares tentam se recuperar sozinhos com **a mesma política**:
3 tentativas, a primeira 1 s depois da falha e cada uma o dobro da anterior (1 s, 2 s, 4 s). Esgotadas
as tentativas, a falha é declarada e contada.

| | Software A: retentativa da publicação | Software B: reinício da tarefa do Debezium |
|---|---|---|
| Quando age | a publicação síncrona falhou depois do commit | o conector ou a tarefa entrou em `FAILED` |
| O que faz | espera o resultado real da publicação; se o broker recusou, reenvia o evento a cada tentativa | pede `POST /connectors/<nome>/restart?includeTasks=true&onlyFailed=true` |
| Esgotado | o evento é **declarado perdido** (`events_declared_lost_total`) | o vigia **desiste** (`connector_restart_gave_up=1`): intervenção manual necessária |
| Onde fica | memória do `tracking-service` (até `PUBLISH_RETRY_BUFFER` eventos; acima disso, declarado perdido) | serviço `connect-watchdog`, papel que em produção é do operador do Connect (ex.: `autoRestart` do Strimzi) |
| Variáveis | `PUBLISH_RETRY_ATTEMPTS`, `PUBLISH_RETRY_BACKOFF`, `PUBLISH_RETRY_MULTIPLIER`, `PUBLISH_RETRY_BUFFER` | `CONNECT_RESTART_ATTEMPTS`, `CONNECT_RESTART_BACKOFF`, `CONNECT_RESTART_MULTIPLIER`, `CONNECT_RESTART_RESET_AFTER` |

A fila de retentativa do A é **só em memória**, de propósito: um registro durável do que falta
publicar seria um Outbox, que o Software A não usa. Os valores usados em cada execução ficam em
`marcos.json` (`parametros.recuperacao_automatica`). `--sem-recuperacao` nos scripts (ou
`PUBLISH_RETRY_ATTEMPTS=0` e `CONNECT_RESTART_ATTEMPTS=0`) volta ao comportamento sem retentativa.

### Disco limitado no banco de origem

No B, o slot de replicação segura o WAL enquanto a fila está fora. Para observar o ponto em que isso
esgota o disco e o banco passa a recusar escritas sem encher o disco da máquina, use a sobreposição
que coloca os dados da origem num tmpfs de tamanho fixo (precisa caber a carga inicial):

```bash
export COMPOSE_FILE=docker-compose.yml:docker-compose.disco-limitado.yml
export ORIGEM_DISCO_LIMITE=1g
python -m scripts.cenario1.fila_indisponivel --modo cdc --janelas 15 --taxa 400
```

## Decisões de implementação que afetam as medições

As mesmas dos protótipos anteriores (publicação síncrona no A com `PUBLISH_TIMEOUT` de 5 s e
resposta de sucesso depois do commit, falha de publicação decidida pelo resultado real do produtor,
`producer.delivery.timeout.ms` no máximo no Connect, `snapshot.mode=no_data`, consumidor com entrega
pelo menos uma vez e guarda de versão). As específicas do RetailRocket:

- **Duas operações, duas ou três linhas.** O evento de comportamento grava o evento e atualiza o
  visitante e o item; a mudança de propriedade grava o evento e atualiza o item. Os agregados são
  travados sempre na mesma ordem (visitante, depois item), para que operações concorrentes não
  entrem em impasse.
- **Chave do evento no A.** Visitante nos eventos de comportamento, item nas mudanças de
  propriedade. A ordem do item entre eventos de visitantes diferentes não é garantida pelo Kafka.
- **Mudanças de propriedade.** Só `categoryid` e `available`, e só quando o valor muda. A
  proporção delas na carga é a que o dataset tiver (ver `preparacao.json`); `--sem-propriedades`
  na preparação deixa só os eventos de comportamento.
- **Horário original.** Cada evento guarda o horário do dataset em `occurred_at`; a carga reproduz
  a ordem original, mas na taxa configurada, não no ritmo de 2015.
- **Subconjunto do dataset.** Por padrão, 1 milhão de operações (`RETAILROCKET_MAX_OPS`), pelo custo
  de recriar as bases em cada execução. É um parâmetro do experimento e entra nas ameaças à
  validade se for mantido.
- **Decomposição.** São dois serviços (origem e destino), o suficiente para o Cenário 1. Os demais
  cenários ainda não foram portados para este protótipo.

## Estrutura

```
cmd/tracking-service     serviço de negócio (origem)
cmd/analytics-service    consumidor (destino, análise de comportamento)
cmd/seed                 carga inicial a partir do dataset preparado
cmd/connect-watchdog     reinício automático da tarefa do Debezium (Software B)
internal/                modelo, publicação e retentativa no Kafka, política de retentativa, métricas, configuração
db/origem, db/destino    esquemas SQL
infra/kafka              criação dos tópicos
infra/debezium           configuração e registro do conector
scripts/carga            gerador de carga e testes de carga
scripts/cenario1         injeção de falha do Cenário 1
scripts/comum            dataset, controle do ambiente, coleta e análise
scripts/painel           painel web com o diagrama, as falhas e os scripts
scripts/dados            preparação do dataset real e amostra sintética
```
