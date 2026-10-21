package events

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/metrics"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/retry"
)

// fakePublisher falha nas primeiras `failures` chamadas.
type fakePublisher struct {
	failures int64
	calls    atomic.Int64
}

func (f *fakePublisher) Publish(context.Context, string, []byte, map[string]string) error {
	if f.calls.Add(1) <= f.failures {
		return errors.New("broker fora")
	}
	return nil
}
func (f *fakePublisher) Close() error { return nil }

func newQueue(pub Publisher, attempts, limit int) *RetryQueue {
	return NewRetryQueue(pub, retry.Policy{Attempts: attempts, Initial: time.Millisecond, Multiplier: 2}, limit, metrics.New())
}

func waitIdle(t *testing.T, q *RetryQueue) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for q.Pending.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("a fila não esvaziou")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRetryQueueRecovers(t *testing.T) {
	pub := &fakePublisher{failures: 2}
	q := newQueue(pub, 3, 10)
	q.Enqueue(context.Background(), "k", nil, nil, nil)
	waitIdle(t, q)
	if q.Recovered.Load() != 1 || q.DeclaredLost.Load() != 0 || q.AttemptsFailed.Load() != 2 {
		t.Fatalf("recuperados=%d perdidos=%d tentativas falhas=%d",
			q.Recovered.Load(), q.DeclaredLost.Load(), q.AttemptsFailed.Load())
	}
}

func TestRetryQueueDeclaresLost(t *testing.T) {
	pub := &fakePublisher{failures: 100}
	q := newQueue(pub, 3, 10)
	q.Enqueue(context.Background(), "k", nil, nil, nil)
	waitIdle(t, q)
	if q.DeclaredLost.Load() != 1 || pub.calls.Load() != 3 {
		t.Fatalf("perdidos=%d chamadas=%d", q.DeclaredLost.Load(), pub.calls.Load())
	}
}

func TestRetryQueueOverflowAndDisabled(t *testing.T) {
	q := newQueue(&fakePublisher{failures: 100}, 3, 1)
	q.policy.Initial = time.Hour // mantém o primeiro evento na fila
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Enqueue(ctx, "a", nil, nil, nil)
	q.Enqueue(ctx, "b", nil, nil, nil)
	if q.Overflow.Load() != 1 || q.DeclaredLost.Load() != 1 || q.Enqueued.Load() != 1 {
		t.Fatalf("estouro=%d perdidos=%d enfileirados=%d", q.Overflow.Load(), q.DeclaredLost.Load(), q.Enqueued.Load())
	}

	off := newQueue(&fakePublisher{}, 0, 10)
	off.Enqueue(ctx, "c", nil, nil, nil)
	waitIdle(t, off)
	if off.DeclaredLost.Load() != 1 {
		t.Fatal("sem tentativas, o evento que falhou deveria ser declarado perdido")
	}
}

func TestRetryQueueWaitsInflight(t *testing.T) {
	pub := &fakePublisher{}
	q := newQueue(pub, 3, 10)
	ok := make(chan error, 1)
	ok <- nil
	q.Enqueue(context.Background(), "k", nil, nil, ok)
	waitIdle(t, q)
	if q.LateConfirmed.Load() != 1 || pub.calls.Load() != 0 {
		t.Fatalf("confirmados com atraso=%d chamadas=%d: não deveria reenviar", q.LateConfirmed.Load(), pub.calls.Load())
	}

	failed := make(chan error, 1)
	failed <- errors.New("broker fora")
	q.Enqueue(context.Background(), "k", nil, nil, failed)
	waitIdle(t, q)
	if q.Recovered.Load() != 1 || pub.calls.Load() != 1 {
		t.Fatalf("recuperados=%d chamadas=%d", q.Recovered.Load(), pub.calls.Load())
	}
}
