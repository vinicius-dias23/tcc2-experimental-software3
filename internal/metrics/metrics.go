// Package metrics expõe contadores simples no formato de texto do Prometheus,
// lidos pelos scripts de coleta em Python (GET /metrics).
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

type Registry struct {
	mu       sync.Mutex
	counters map[string]*atomic.Int64
	help     map[string]string
	gauges   map[string]bool
}

func New() *Registry {
	return &Registry{counters: map[string]*atomic.Int64{}, help: map[string]string{}, gauges: map[string]bool{}}
}

// Counter devolve (criando se preciso) o contador com o nome dado.
func (r *Registry) Counter(name, help string) *atomic.Int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[name]
	if !ok {
		c = &atomic.Int64{}
		r.counters[name] = c
		r.help[name] = help
	}
	return c
}

// Gauge é um contador que também desce (ex.: itens pendentes); só muda o TYPE exposto.
func (r *Registry) Gauge(name, help string) *atomic.Int64 {
	c := r.Counter(name, help)
	r.mu.Lock()
	r.gauges[name] = true
	r.mu.Unlock()
	return c
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		names := make([]string, 0, len(r.counters))
		for n := range r.counters {
			names = append(names, n)
		}
		gauges := make(map[string]bool, len(r.gauges))
		for n := range r.gauges {
			gauges[n] = true
		}
		r.mu.Unlock()
		sort.Strings(names)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, n := range names {
			typ := "counter"
			if gauges[n] {
				typ = "gauge"
			}
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", n, r.help[n], n, typ, n, r.counters[n].Load())
		}
	})
}
