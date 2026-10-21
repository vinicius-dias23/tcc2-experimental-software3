// Package model define as entidades do domínio (dataset RetailRocket) e o envelope dos
// eventos de domínio publicados pelo Software A.
package model

import "time"

// Event espelha a tabela events: uma linha por operação de negócio. As tags JSON coincidem
// com os nomes das colunas, de modo que a mesma struct decodifica tanto o evento dentro do
// evento de domínio quanto a linha capturada pelo Debezium (após o ExtractNewRecordState).
type Event struct {
	EventID       string     `json:"event_id"`
	OccurredAt    time.Time  `json:"occurred_at"`
	EventType     string     `json:"event_type"`
	VisitorID     *int64     `json:"visitor_id"`
	ItemID        int64      `json:"item_id"`
	TransactionID *int64     `json:"transaction_id"`
	Property      *string    `json:"property"`
	Value         *string    `json:"value"`
	CorrelationID *string    `json:"correlation_id"`
	CreatedAt     *time.Time `json:"created_at"`
}

// Visitor espelha a tabela visitors: o funil de um visitante da loja.
type Visitor struct {
	VisitorID         int64      `json:"visitor_id"`
	Views             int64      `json:"views"`
	AddToCarts        int64      `json:"add_to_carts"`
	Purchases         int64      `json:"purchases"`
	Version           int64      `json:"version"`
	LastEventID       *string    `json:"last_event_id"`
	LastCorrelationID *string    `json:"last_correlation_id"`
	UpdatedAt         *time.Time `json:"updated_at"`
}

// Item espelha a tabela items: o item do catálogo, com categoria, disponibilidade e o resumo
// de comportamento.
type Item struct {
	ItemID            int64      `json:"item_id"`
	CategoryID        *int32     `json:"category_id"`
	Available         *bool      `json:"available"`
	Views             int64      `json:"views"`
	AddToCarts        int64      `json:"add_to_carts"`
	Purchases         int64      `json:"purchases"`
	Version           int64      `json:"version"`
	LastEventID       *string    `json:"last_event_id"`
	LastCorrelationID *string    `json:"last_correlation_id"`
	UpdatedAt         *time.Time `json:"updated_at"`
}

// Tipos de operação. Os três primeiros são os eventos de comportamento do events.csv; o
// último é a mudança de propriedade de item (item_properties), restrita às duas propriedades
// que o dataset publica em claro.
const (
	TypeView        = "view"
	TypeAddToCart   = "addtocart"
	TypeTransaction = "transaction"
	TypeProperty    = "property"

	PropertyCategory  = "categoryid"
	PropertyAvailable = "available"
)

// BehaviorTypes são os eventos de comportamento aceitos em POST /events.
var BehaviorTypes = map[string]bool{TypeView: true, TypeAddToCart: true, TypeTransaction: true}

// Tipos de evento de domínio do Software A, um por tipo de operação.
var DomainEventTypes = map[string]string{
	TypeView:        "ItemViewed",
	TypeAddToCart:   "ItemAddedToCart",
	TypeTransaction: "ItemPurchased",
	TypeProperty:    "ItemPropertyChanged",
}

// DomainEvent é o envelope publicado pelo tracking-service no Software A: um evento por
// operação de negócio, com o evento e os agregados tocados (visitante e item) após a mudança.
// No Software B a mesma operação gera uma mensagem por linha alterada: três num evento de
// comportamento (evento, visitante e item) e duas numa mudança de propriedade (evento e item).
type DomainEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	OccurredAt    time.Time `json:"occurred_at"`
	CorrelationID string    `json:"correlation_id"`
	AggregateID   string    `json:"aggregate_id"`
	Event         Event     `json:"event"`
	Visitor       *Visitor  `json:"visitor,omitempty"`
	Item          Item      `json:"item"`
}
