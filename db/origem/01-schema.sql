-- Banco de ORIGEM: pertence ao tracking-service (serviço de negócio).
-- O esquema é idêntico nos dois protótipos (Software A e Software B).
-- Nenhuma tabela de outbox existe aqui: o Software A publica direto na fila
-- e o Software B depende apenas do WAL (wal_level=logical, ver docker-compose.yml).

SET TIME ZONE 'UTC';

-- Árvore de categorias do RetailRocket (category_tree.csv). Estática: só a carga inicial grava.
CREATE TABLE categories (
    category_id integer PRIMARY KEY,
    parent_id   integer
);

-- Catálogo de itens. category_id e available vêm de item_properties (as duas propriedades que
-- o dataset não anonimiza); os contadores são o resumo de comportamento de cada item.
--
-- version e last_correlation_id formam o oráculo do experimento: cada operação incrementa
-- version dos agregados que tocou e grava o identificador de correlação recebido na
-- requisição, que chega ao destino tanto pelo evento de domínio quanto pela linha do WAL.
CREATE TABLE items (
    item_id             bigint PRIMARY KEY,
    category_id         integer,
    available           boolean,
    views               bigint NOT NULL DEFAULT 0,
    add_to_carts        bigint NOT NULL DEFAULT 0,
    purchases           bigint NOT NULL DEFAULT 0,
    version             bigint NOT NULL DEFAULT 1,
    last_event_id       text,
    last_correlation_id uuid,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX items_updated_at_idx ON items (updated_at);

-- Visitantes da loja (visitorid do events.csv), com o resumo do funil de cada um.
CREATE TABLE visitors (
    visitor_id          bigint PRIMARY KEY,
    views               bigint NOT NULL DEFAULT 0,
    add_to_carts        bigint NOT NULL DEFAULT 0,
    purchases           bigint NOT NULL DEFAULT 0,
    version             bigint NOT NULL DEFAULT 1,
    last_event_id       text,
    last_correlation_id uuid,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX visitors_updated_at_idx ON visitors (updated_at);

-- Uma linha por operação de negócio:
--   view, addtocart, transaction : evento de comportamento (events.csv); toca o visitante e o item
--   property                     : mudança de categoria ou de disponibilidade do item
--                                  (item_properties); toca só o item
-- occurred_at é o horário original do dataset; created_at, o do commit na origem.
CREATE TABLE events (
    event_id       text PRIMARY KEY,
    occurred_at    timestamptz NOT NULL,
    event_type     text NOT NULL,
    visitor_id     bigint,
    item_id        bigint NOT NULL,
    transaction_id bigint,
    property       text,
    value          text,
    correlation_id uuid,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX events_created_at_idx ON events (created_at);

-- Registro da carga inicial (parcela do dataset pré-carregada).
CREATE TABLE seed_info (
    id                integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    seeded_at         timestamptz NOT NULL DEFAULT now(),
    seed_fraction     numeric NOT NULL,
    data_file         text NOT NULL,
    operations_total  integer NOT NULL,
    operations_seeded integer NOT NULL,
    visitors_seeded   integer NOT NULL,
    items_seeded      integer NOT NULL,
    categories_seeded integer NOT NULL,
    synthetic         boolean NOT NULL
);
