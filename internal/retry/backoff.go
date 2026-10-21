// Package retry contém a política de retentativa comum aos dois softwares: um número fixo
// de tentativas com intervalos crescentes (backoff exponencial). O Software A a usa para
// republicar eventos e o Software B, para reiniciar a tarefa do Debezium.
package retry

import (
	"time"

	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/config"
)

// Policy é a política de retentativa. Com Attempts zero, não há retentativa.
type Policy struct {
	Attempts   int
	Initial    time.Duration
	Multiplier float64
}

// FromEnv lê <prefixo>_ATTEMPTS, <prefixo>_BACKOFF e <prefixo>_MULTIPLIER.
// Padrão: 3 tentativas, a primeira 1 s depois da falha e cada uma o dobro da anterior.
func FromEnv(prefix string) Policy {
	return Policy{
		Attempts:   config.Int(prefix+"_ATTEMPTS", 3),
		Initial:    config.Duration(prefix+"_BACKOFF", time.Second),
		Multiplier: config.Float(prefix+"_MULTIPLIER", 2),
	}
}

// Delay é a espera antes da tentativa n (a partir de 1): Initial × Multiplier^(n-1).
func (p Policy) Delay(n int) time.Duration {
	d := float64(p.Initial)
	for i := 1; i < n; i++ {
		d *= p.Multiplier
	}
	return time.Duration(d)
}

// Schedule descreve as esperas da política, para os logs (ex.: "1s, 2s, 4s").
func (p Policy) Schedule() string {
	s := ""
	for n := 1; n <= p.Attempts; n++ {
		if n > 1 {
			s += ", "
		}
		s += p.Delay(n).String()
	}
	if s == "" {
		return "desligada"
	}
	return s
}
