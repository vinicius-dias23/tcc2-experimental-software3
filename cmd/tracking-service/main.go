// tracking-service é o serviço de negócio (origem). Recebe os eventos de comportamento dos
// visitantes da loja do RetailRocket (visualização, inclusão no carrinho e compra) e as
// mudanças de propriedade dos itens (categoria e disponibilidade). Cada evento de
// comportamento grava a linha em events e atualiza o funil do visitante e o resumo do item,
// tudo na mesma transação do banco de origem; cada mudança de propriedade grava a linha em
// events e atualiza o item. Apenas no modo domain-events (Software A) publica o evento de
// domínio diretamente na fila logo após o commit. Se essa publicação falhar, o evento vai para
// uma fila de retentativa em memória e, esgotadas as tentativas, é declarado perdido.
//
// No modo cdc (Software B) o código é o mesmo, mas a publicação é desligada: quem propaga é o
// Debezium, lendo o WAL do banco de origem.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/events"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/metrics"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/model"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/pg"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/retry"
)

const eventColumns = `event_id, occurred_at, event_type, visitor_id, item_id, transaction_id, property,
	value, correlation_id::text, created_at`

const visitorColumns = `visitor_id, views, add_to_carts, purchases, version, last_event_id,
	last_correlation_id::text, updated_at`

const itemColumns = `item_id, category_id, available, views, add_to_carts, purchases, version,
	last_event_id, last_correlation_id::text, updated_at`

type server struct {
	mode string
	db   *pgxpool.Pool
	pub  events.Publisher
	// Retentativa em memória dos eventos cuja publicação síncrona falhou (Software A).
	retry *events.RetryQueue
	// Contexto do processo: as retentativas não dependem da requisição que as originou.
	ctx context.Context
	// Resposta ao cliente quando a publicação falha depois do commit:
	// "success" (padrão) responde 2xx, pois a mudança está commitada;
	// "error" responde 503, embora o commit já tenha ocorrido.
	failureResponse string
	// Tempo máximo que a requisição espera a confirmação do broker; esgotado, a publicação
	// é dada como falha na resposta e o resultado real fica com a fila de retentativa.
	publishTimeout time.Duration
	// Tempo máximo das operações no banco por requisição; esgotado, a API responde 503.
	dbTimeout time.Duration

	committed, httpErrors    *atomic.Int64
	published, publishFailed *atomic.Int64
	publishLatencyMs         *atomic.Int64
	failuresSinceLog         atomic.Int64
}

func main() {
	mode := config.Mode()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db := pg.Connect(ctx, config.String("DATABASE_URL", "postgres://retailrocket:retailrocket@postgres-origem:5432/retailrocket"),
		int32(config.Int("DB_MAX_CONNS", 30)))
	defer db.Close()

	reg := metrics.New()
	s := &server{
		mode:             mode,
		db:               db,
		ctx:              ctx,
		failureResponse:  config.String("PUBLISH_FAILURE_RESPONSE", "success"),
		dbTimeout:        config.Duration("DB_TIMEOUT", 10*time.Second),
		committed:        reg.Counter("operations_committed_total", "operações commitadas (eventos de comportamento e mudanças de propriedade)"),
		httpErrors:       reg.Counter("http_errors_total", "requisições respondidas com erro"),
		published:        reg.Counter("events_published_total", "eventos de domínio confirmados pelo broker"),
		publishFailed:    reg.Counter("events_publish_failed_total", "eventos de domínio cuja publicação síncrona falhou após o commit (vão para a retentativa)"),
		publishLatencyMs: reg.Counter("events_publish_latency_ms_sum", "soma da latência de publicação (ms)"),
	}

	if mode == config.ModeDomainEvents {
		p := events.NewKafkaPublisher(events.KafkaPublisherConfig{
			Brokers:     config.List("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
			Topic:       config.String("DOMAIN_EVENTS_TOPIC", "retailrocket.domain-events.activity"),
			MaxAttempts: config.Int("PUBLISH_MAX_ATTEMPTS", 10),
		})
		s.pub = p
		s.publishTimeout = config.Duration("PUBLISH_TIMEOUT", 5*time.Second)
		s.retry = events.NewRetryQueue(p, retry.FromEnv("PUBLISH_RETRY"),
			config.Int("PUBLISH_RETRY_BUFFER", 10000), reg)
		go s.retry.LogSummary(ctx)
		go s.logPublishFailures(ctx)
	} else {
		s.pub = events.NoopPublisher{}
	}
	defer s.pub.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.createEvent)
	mux.HandleFunc("POST /items/{id}/properties", s.changeProperty)
	mux.HandleFunc("GET /events/{id}", s.getEvent)
	mux.HandleFunc("GET /visitors/{id}", s.getVisitor)
	mux.HandleFunc("GET /items/{id}", s.getItem)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "mode": mode})
	})
	mux.Handle("GET /metrics", reg.Handler())

	addr := config.String("HTTP_ADDR", ":8080")
	httpSrv := &http.Server{Addr: addr, Handler: mux, ReadTimeout: 30 * time.Second, WriteTimeout: 120 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdown)
	}()
	log.Printf("tracking-service ouvindo em %s (modo %s)", addr, mode)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// createEventRequest é uma linha do events.csv. timestamp é o horário original do dataset,
// em milissegundos desde a época Unix.
type createEventRequest struct {
	EventID       string `json:"event_id"`
	Timestamp     int64  `json:"timestamp"`
	Event         string `json:"event"`
	VisitorID     int64  `json:"visitor_id"`
	ItemID        int64  `json:"item_id"`
	TransactionID *int64 `json:"transaction_id"`
}

func (req *createEventRequest) validate() string {
	// O dataset numera visitantes e itens a partir de 0, então 0 é um identificador válido.
	if req.EventID == "" || req.VisitorID < 0 || req.ItemID < 0 {
		return "event_id é obrigatório e visitor_id e item_id não podem ser negativos"
	}
	if !model.BehaviorTypes[req.Event] {
		return "event deve ser view, addtocart ou transaction"
	}
	if req.Event == model.TypeTransaction && req.TransactionID == nil {
		return "transaction exige transaction_id"
	}
	return ""
}

// changePropertyRequest é uma mudança de categoryid ou available do item_properties.
type changePropertyRequest struct {
	EventID   string `json:"event_id"`
	Timestamp int64  `json:"timestamp"`
	Property  string `json:"property"`
	Value     string `json:"value"`
}

// counterColumn é a coluna de contador que o tipo de evento incrementa no visitante e no item.
func counterColumn(eventType string) string {
	switch eventType {
	case model.TypeAddToCart:
		return "add_to_carts"
	case model.TypeTransaction:
		return "purchases"
	}
	return "views"
}

func (s *server) createEvent(w http.ResponseWriter, r *http.Request) {
	var req createEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.fail(w, http.StatusBadRequest, "corpo inválido: "+err.Error())
		return
	}
	if msg := req.validate(); msg != "" {
		s.fail(w, http.StatusBadRequest, "corpo inválido: "+msg)
		return
	}
	corr := correlationID(r)
	col := counterColumn(req.Event)

	ctx, cancel := context.WithTimeout(r.Context(), s.dbTimeout)
	defer cancel()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer tx.Rollback(context.Background())

	ev := model.Event{}
	if !s.insertEvent(ctx, w, tx, &ev, req.EventID, req.Timestamp, req.Event, &req.VisitorID, req.ItemID,
		req.TransactionID, nil, nil, corr) {
		return
	}
	// Os agregados são travados sempre na mesma ordem (visitante e depois item), e a mudança de
	// propriedade só trava o item, de modo que duas operações não entram em impasse.
	v := &model.Visitor{}
	err = tx.QueryRow(ctx, `
		INSERT INTO visitors (visitor_id, `+col+`, version, last_event_id, last_correlation_id, updated_at)
		VALUES ($1, 1, 1, $2, $3, now())
		ON CONFLICT (visitor_id) DO UPDATE SET
			`+col+` = visitors.`+col+` + 1, version = visitors.version + 1,
			last_event_id = $2, last_correlation_id = $3, updated_at = now()
		RETURNING `+visitorColumns,
		req.VisitorID, req.EventID, corr).Scan(scanVisitor(v)...)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	it := &model.Item{}
	err = tx.QueryRow(ctx, `
		INSERT INTO items (item_id, `+col+`, version, last_event_id, last_correlation_id, updated_at)
		VALUES ($1, 1, 1, $2, $3, now())
		ON CONFLICT (item_id) DO UPDATE SET
			`+col+` = items.`+col+` + 1, version = items.version + 1,
			last_event_id = $2, last_correlation_id = $3, updated_at = now()
		RETURNING `+itemColumns,
		req.ItemID, req.EventID, corr).Scan(scanItem(it)...)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if err := tx.Commit(ctx); err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.committed.Add(1)

	// A partir daqui a mudança está commitada. No Software A o evento é publicado fora da
	// transação: se a publicação falhar, nada durável registra que ele deveria existir.
	// A chave é o visitante, para que o funil de cada visitante chegue em ordem.
	published := s.publish(model.DomainEvent{
		EventType: model.DomainEventTypes[req.Event], CorrelationID: corr,
		AggregateID: strconv.FormatInt(req.VisitorID, 10), Event: ev, Visitor: v, Item: *it,
	})
	s.respondAfterCommit(w, http.StatusCreated, published, map[string]any{
		"event_id": ev.EventID, "correlation_id": corr,
		"visitor_version": v.Version, "item_version": it.Version,
	})
}

func (s *server) changeProperty(w http.ResponseWriter, r *http.Request) {
	itemID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || itemID < 0 {
		s.fail(w, http.StatusBadRequest, "identificador de item inválido")
		return
	}
	var req changePropertyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.fail(w, http.StatusBadRequest, "corpo inválido: "+err.Error())
		return
	}
	if req.EventID == "" {
		s.fail(w, http.StatusBadRequest, "corpo inválido: event_id é obrigatório")
		return
	}
	// Só as duas propriedades que o dataset publica em claro; as demais são hashes.
	var set string
	var arg any
	switch req.Property {
	case model.PropertyCategory:
		n, err := strconv.ParseInt(req.Value, 10, 32)
		if err != nil {
			s.fail(w, http.StatusBadRequest, "corpo inválido: categoryid deve ser inteiro")
			return
		}
		set, arg = "category_id", int32(n)
	case model.PropertyAvailable:
		if req.Value != "0" && req.Value != "1" {
			s.fail(w, http.StatusBadRequest, "corpo inválido: available deve ser 0 ou 1")
			return
		}
		set, arg = "available", req.Value == "1"
	default:
		s.fail(w, http.StatusBadRequest, "corpo inválido: property deve ser categoryid ou available")
		return
	}
	corr := correlationID(r)

	ctx, cancel := context.WithTimeout(r.Context(), s.dbTimeout)
	defer cancel()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer tx.Rollback(context.Background())

	ev := model.Event{}
	if !s.insertEvent(ctx, w, tx, &ev, req.EventID, req.Timestamp, model.TypeProperty, nil, itemID, nil,
		&req.Property, &req.Value, corr) {
		return
	}
	it := &model.Item{}
	err = tx.QueryRow(ctx, `
		INSERT INTO items (item_id, `+set+`, version, last_event_id, last_correlation_id, updated_at)
		VALUES ($1, $2, 1, $3, $4, now())
		ON CONFLICT (item_id) DO UPDATE SET
			`+set+` = $2, version = items.version + 1,
			last_event_id = $3, last_correlation_id = $4, updated_at = now()
		RETURNING `+itemColumns,
		itemID, arg, req.EventID, corr).Scan(scanItem(it)...)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if err := tx.Commit(ctx); err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.committed.Add(1)

	published := s.publish(model.DomainEvent{
		EventType: model.DomainEventTypes[model.TypeProperty], CorrelationID: corr,
		AggregateID: strconv.FormatInt(itemID, 10), Event: ev, Item: *it,
	})
	s.respondAfterCommit(w, http.StatusOK, published, map[string]any{
		"event_id": ev.EventID, "correlation_id": corr, "item_version": it.Version,
	})
}

// insertEvent grava a linha da operação em events. Devolve false (já tendo respondido) se
// falhar, inclusive com 409 se o event_id já existir.
func (s *server) insertEvent(ctx context.Context, w http.ResponseWriter, tx pgx.Tx, ev *model.Event, id string,
	tsMillis int64, typ string, visitor *int64, item int64, transaction *int64, property, value *string, corr string) bool {
	err := tx.QueryRow(ctx, `
		INSERT INTO events (event_id, occurred_at, event_type, visitor_id, item_id, transaction_id,
			property, value, correlation_id, created_at)
		VALUES ($1, to_timestamp($2::bigint / 1000.0), $3, $4, $5, $6, $7, $8, $9, now())
		RETURNING `+eventColumns,
		id, tsMillis, typ, visitor, item, transaction, property, value, corr,
	).Scan(scanEvent(ev)...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			s.fail(w, http.StatusConflict, "evento já existe")
			return false
		}
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return false
	}
	return true
}

func (s *server) getEvent(w http.ResponseWriter, r *http.Request) {
	var e model.Event
	err := s.db.QueryRow(r.Context(), `SELECT `+eventColumns+` FROM events WHERE event_id = $1`,
		r.PathValue("id")).Scan(scanEvent(&e)...)
	s.writeFound(w, err, e, "evento não encontrado")
}

func (s *server) getVisitor(w http.ResponseWriter, r *http.Request) {
	var v model.Visitor
	err := s.db.QueryRow(r.Context(), `SELECT `+visitorColumns+` FROM visitors WHERE visitor_id::text = $1`,
		r.PathValue("id")).Scan(scanVisitor(&v)...)
	s.writeFound(w, err, v, "visitante não encontrado")
}

func (s *server) getItem(w http.ResponseWriter, r *http.Request) {
	var it model.Item
	err := s.db.QueryRow(r.Context(), `SELECT `+itemColumns+` FROM items WHERE item_id::text = $1`,
		r.PathValue("id")).Scan(scanItem(&it)...)
	s.writeFound(w, err, it, "item não encontrado")
}

func (s *server) writeFound(w http.ResponseWriter, err error, v any, notFound string) {
	if errors.Is(err, pgx.ErrNoRows) {
		s.fail(w, http.StatusNotFound, notFound)
		return
	}
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// publish envia o evento no modo domain-events e devolve se ele foi confirmado na
// publicação síncrona; se não foi, o evento segue para a fila de retentativa.
// No modo cdc não faz nada e devolve true.
func (s *server) publish(ev model.DomainEvent) bool {
	if s.mode != config.ModeDomainEvents {
		return true
	}
	ev.EventID = uuid.NewString()
	ev.OccurredAt = time.Now().UTC()
	payload, err := json.Marshal(ev)
	if err != nil {
		log.Printf("erro serializando evento: %v", err)
		s.publishFailed.Add(1)
		return false
	}
	start := time.Now()
	headers := map[string]string{"correlation_id": ev.CorrelationID, "event_type": ev.EventType}
	// A publicação não é cancelada se o cliente desistir, nem quando a requisição para de
	// esperar: o kafka-go continua tentando, e o resultado real vai para a retentativa.
	result := make(chan error, 1)
	go func() { result <- s.pub.Publish(s.ctx, ev.AggregateID, payload, headers) }()
	timer := time.NewTimer(s.publishTimeout)
	defer timer.Stop()
	select {
	case err = <-result:
		if err != nil {
			s.retry.Enqueue(s.ctx, ev.AggregateID, payload, headers, nil)
		}
	case <-timer.C:
		err = errors.New("tempo de publicação esgotado")
		s.retry.Enqueue(s.ctx, ev.AggregateID, payload, headers, result)
	}
	s.publishLatencyMs.Add(time.Since(start).Milliseconds())
	if err != nil {
		s.publishFailed.Add(1)
		s.failuresSinceLog.Add(1)
		return false
	}
	s.published.Add(1)
	return true
}

func (s *server) respondAfterCommit(w http.ResponseWriter, status int, published bool, body map[string]any) {
	if !published && s.failureResponse == "error" {
		s.fail(w, http.StatusServiceUnavailable, "mudança commitada, mas o evento não foi publicado")
		return
	}
	writeJSON(w, status, body)
}

// logPublishFailures resume as falhas de publicação a cada 5 s, em vez de uma linha
// por requisição, para não inundar o log durante janelas longas de indisponibilidade.
func (s *server) logPublishFailures(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := s.failuresSinceLog.Swap(0); n > 0 {
				log.Printf("ERRO: %d eventos falharam na publicação síncrona nos últimos 5s e foram para a retentativa (total %d)", n, s.publishFailed.Load())
			}
		}
	}
}

func (s *server) fail(w http.ResponseWriter, status int, msg string) {
	s.httpErrors.Add(1)
	writeJSON(w, status, map[string]string{"error": msg})
}

func scanEvent(e *model.Event) []any {
	return []any{&e.EventID, &e.OccurredAt, &e.EventType, &e.VisitorID, &e.ItemID, &e.TransactionID,
		&e.Property, &e.Value, &e.CorrelationID, &e.CreatedAt}
}

func scanVisitor(v *model.Visitor) []any {
	return []any{&v.VisitorID, &v.Views, &v.AddToCarts, &v.Purchases, &v.Version, &v.LastEventID,
		&v.LastCorrelationID, &v.UpdatedAt}
}

func scanItem(it *model.Item) []any {
	return []any{&it.ItemID, &it.CategoryID, &it.Available, &it.Views, &it.AddToCarts, &it.Purchases,
		&it.Version, &it.LastEventID, &it.LastCorrelationID, &it.UpdatedAt}
}

func correlationID(r *http.Request) string {
	if v := r.Header.Get("X-Correlation-ID"); v != "" {
		if _, err := uuid.Parse(v); err == nil {
			return v
		}
	}
	return uuid.NewString()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
