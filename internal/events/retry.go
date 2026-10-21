package events

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/metrics"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/retry"
)

// RetryQueue é a retentativa do Software A para eventos cuja publicação síncrona falhou:
// cada evento é reenviado segundo a política (padrão: 3 tentativas, 1 s, 2 s e 4 s depois da
// falha anterior) e, esgotadas as tentativas, é declarado perdido.
//
// Só conta como falha o resultado real do produtor. Se a requisição desistiu de esperar
// (timeout) com a publicação ainda em andamento, a fila primeiro aguarda esse resultado: se
// o broker confirmar, o evento não é reenviado (o que evita duplicá-lo).
//
// A fila fica só na memória do processo, de propósito: um registro durável do que falta
// publicar seria um Outbox, que o Software A não usa. Se o processo morrer, o que estava
// esperando retentativa se perde sem ser contado.
type RetryQueue struct {
	pub    Publisher
	policy retry.Policy
	limit  int64

	Enqueued, LateConfirmed, Recovered, AttemptsFailed, DeclaredLost, Overflow, Pending *atomic.Int64
}

// NewRetryQueue cria a fila e registra seus contadores em reg (expostos em /metrics).
func NewRetryQueue(pub Publisher, policy retry.Policy, limit int, reg *metrics.Registry) *RetryQueue {
	return &RetryQueue{pub: pub, policy: policy, limit: int64(limit),
		Enqueued:       reg.Counter("events_retry_enqueued_total", "eventos que entraram na fila de retentativa"),
		LateConfirmed:  reg.Counter("events_late_confirmed_total", "eventos dados como falha pelo timeout da requisição, mas confirmados depois pelo broker"),
		Recovered:      reg.Counter("events_retry_recovered_total", "eventos publicados por uma retentativa"),
		AttemptsFailed: reg.Counter("events_retry_attempts_failed_total", "retentativas que falharam"),
		DeclaredLost:   reg.Counter("events_declared_lost_total", "eventos declarados perdidos (tentativas esgotadas ou fila cheia)"),
		Overflow:       reg.Counter("events_retry_overflow_total", "eventos recusados porque a fila de retentativa estava cheia"),
		Pending:        reg.Gauge("events_retry_pending", "eventos esperando retentativa na memória"),
	}
}

// Enqueue agenda as retentativas de um evento. inflight, se não for nil, é o resultado da
// publicação que a requisição deixou de esperar; a fila o aguarda antes de decidir. Sem
// tentativas configuradas, ou com a fila cheia, o evento que falhou é declarado perdido.
func (q *RetryQueue) Enqueue(ctx context.Context, key string, value []byte, headers map[string]string,
	inflight <-chan error) {
	if q.Pending.Add(1) > q.limit {
		q.Pending.Add(-1)
		q.Overflow.Add(1)
		q.DeclaredLost.Add(1)
		return
	}
	q.Enqueued.Add(1)
	go q.run(ctx, key, value, headers, inflight)
}

func (q *RetryQueue) run(ctx context.Context, key string, value []byte, headers map[string]string,
	inflight <-chan error) {
	defer q.Pending.Add(-1)
	if inflight != nil {
		select {
		case <-ctx.Done():
			return
		case err := <-inflight:
			if err == nil {
				q.LateConfirmed.Add(1)
				return
			}
		}
	}
	for n := 1; n <= q.policy.Attempts; n++ {
		select {
		case <-ctx.Done():
			return // processo encerrando: o evento se perde com a memória
		case <-time.After(q.policy.Delay(n)):
		}
		if err := q.pub.Publish(ctx, key, value, headers); err == nil {
			q.Recovered.Add(1)
			return
		}
		q.AttemptsFailed.Add(1)
	}
	q.DeclaredLost.Add(1)
}

// LogSummary resume a fila a cada 5 s enquanto houver atividade.
func (q *RetryQueue) LogSummary(ctx context.Context) {
	log.Printf("retentativa de publicação: %d tentativas (%s), até %d eventos na memória",
		q.policy.Attempts, q.policy.Schedule(), q.limit)
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	var last [4]int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cur := [4]int64{q.Enqueued.Load(), q.LateConfirmed.Load(), q.Recovered.Load(), q.DeclaredLost.Load()}
			if cur != last || q.Pending.Load() > 0 {
				log.Printf("retentativa: %d pendentes, %d confirmados com atraso, %d recuperados, %d declarados perdidos (total enfileirado %d)",
					q.Pending.Load(), cur[1], cur[2], cur[3], cur[0])
				last = cur
			}
		}
	}
}
