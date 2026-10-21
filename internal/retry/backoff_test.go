package retry

import (
	"testing"
	"time"
)

func TestDelay(t *testing.T) {
	p := Policy{Attempts: 3, Initial: time.Second, Multiplier: 2}
	for n, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second} {
		if got := p.Delay(n); got != want {
			t.Errorf("Delay(%d) = %s, quero %s", n, got, want)
		}
	}
	if got := p.Schedule(); got != "1s, 2s, 4s" {
		t.Errorf("Schedule() = %q", got)
	}
	if got := (Policy{}).Schedule(); got != "desligada" {
		t.Errorf("Schedule() sem tentativas = %q", got)
	}
}
