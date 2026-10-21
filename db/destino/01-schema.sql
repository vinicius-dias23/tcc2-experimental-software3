-- Banco de DESTINO: pertence ao analytics-service (consumidor, serviço de análise de
-- comportamento). Materializa eventos, visitantes e itens a partir das mensagens recebidas,
-- qualquer que seja o modelo de propagação, e registra cada mensagem recebida em received_events.

SET TIME ZONE 'UTC';

-- Cópia estática da árvore de categorias, gravada pela carga inicial.
CREATE TABLE categories (
    category_id integer PRIMARY KEY,
    parent_id   integer
);

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
    updated_at          timestamptz,
    applied_at          timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX items_applied_at_idx ON items (applied_at);

CREATE TABLE visitors (
    visitor_id          bigint PRIMARY KEY,
    views               bigint NOT NULL DEFAULT 0,
    add_to_carts        bigint NOT NULL DEFAULT 0,
    purchases           bigint NOT NULL DEFAULT 0,
    version             bigint NOT NULL DEFAULT 1,
    last_event_id       text,
    last_correlation_id uuid,
    updated_at          timestamptz,
    applied_at          timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX visitors_applied_at_idx ON visitors (applied_at);

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
    created_at     timestamptz,
    applied_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX events_created_at_idx ON events (created_at);

-- Uma linha por entidade recebida, inclusive as duplicadas e as fora de ordem.
-- É daqui que saem perda, duplicação e violação de ordem.
--   source     : 'domain-events' ou 'cdc'
--   entity     : 'event', 'visitor' ou 'item'
--   entity_key : event_id, visitor_id ou item_id
--   version    : versão do visitante ou do item (vazio para o evento)
--   applied    : se a entidade alterou a projeção (false = descartada pela guarda de versão
--                ou evento já existente)
-- No Domain Events uma mensagem carrega o evento e os agregados tocados, então gera até três
-- linhas com o mesmo (topic, kafka_partition, kafka_offset); no CDC cada mensagem gera uma.
CREATE TABLE received_events (
    id              bigserial PRIMARY KEY,
    received_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    source          text NOT NULL,
    topic           text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset    bigint NOT NULL,
    kafka_timestamp timestamptz,
    event_type      text NOT NULL,
    entity          text NOT NULL,
    entity_key      text NOT NULL,
    version         bigint,
    correlation_id  uuid,
    applied         boolean NOT NULL
);

CREATE INDEX received_events_correlation_idx ON received_events (correlation_id);
CREATE INDEX received_events_received_at_idx ON received_events (received_at);
