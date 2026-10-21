// connect-watchdog é o reinício automático da tarefa do Debezium no Software B, papel que
// em produção cabe ao operador do Kafka Connect (ex.: o autoRestart do Strimzi). O Connect
// não reinicia sozinho uma tarefa em FAILED.
//
// A cada CONNECT_WATCHDOG_INTERVAL o vigia lê o status do conector. Ao ver o conector ou uma
// tarefa em FAILED, pede o reinício segundo a mesma política de retentativa do Software A
// (padrão: 3 tentativas, 1 s, 2 s e 4 s depois da falha anterior). Esgotadas as tentativas,
// desiste e registra que é preciso intervenção manual. O contador de tentativas volta a zero
// depois que o conector fica CONNECT_RESTART_RESET_AFTER em RUNNING.
//
// Com o Connect fora do ar o vigia não faz nada: religar o processo não é papel dele.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/metrics"
	"github.com/vinicius-dias23/tcc2-experimental-software3/internal/retry"
)

type status struct {
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
		Trace string `json:"trace"`
	} `json:"tasks"`
}

// failed diz se o conector ou alguma tarefa está em FAILED, com a primeira linha do erro.
func (s status) failed() (bool, string) {
	if s.Connector.State == "FAILED" {
		return true, "conector em FAILED"
	}
	for _, t := range s.Tasks {
		if t.State == "FAILED" {
			trace, _, _ := strings.Cut(t.Trace, "\n")
			return true, fmt.Sprintf("tarefa %d em FAILED: %s", t.ID, trace)
		}
	}
	return false, ""
}

func (s status) running() bool {
	if s.Connector.State != "RUNNING" || len(s.Tasks) == 0 {
		return false
	}
	for _, t := range s.Tasks {
		if t.State != "RUNNING" {
			return false
		}
	}
	return true
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectURL := config.String("CONNECT_URL", "http://kafka-connect:8083")
	name := config.String("CONNECTOR_NAME", "retailrocket-origem")
	interval := config.Duration("CONNECT_WATCHDOG_INTERVAL", 2*time.Second)
	resetAfter := config.Duration("CONNECT_RESTART_RESET_AFTER", 60*time.Second)
	policy := retry.FromEnv("CONNECT_RESTART")

	reg := metrics.New()
	failures := reg.Counter("connector_failures_detected_total", "vezes em que o vigia viu o conector ou uma tarefa em FAILED (inclusive depois de um reinício que não resolveu)")
	restarts := reg.Counter("connector_auto_restarts_total", "reinícios pedidos pelo vigia")
	gaveUp := reg.Gauge("connector_restart_gave_up", "1 enquanto o vigia tiver desistido (intervenção manual necessária)")
	attempts := reg.Gauge("connector_restart_attempts", "tentativas de reinício usadas desde a última vez que o conector ficou estável")
	reachable := reg.Gauge("connect_reachable", "1 se o REST do Kafka Connect respondeu na última leitura")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"status":"ok"}`) })
	mux.Handle("GET /metrics", reg.Handler())
	go func() {
		if err := http.ListenAndServe(config.String("HTTP_ADDR", ":8086"), mux); err != nil {
			log.Fatal(err)
		}
	}()
	log.Printf("vigiando o conector %s em %s a cada %s; reinício: %d tentativas (%s)",
		name, connectURL, interval, policy.Attempts, policy.Schedule())

	client := &http.Client{Timeout: 5 * time.Second}
	var (
		inFailure    bool      // já contamos esta falha
		nextRestart  time.Time // quando pedir o próximo reinício
		runningSince time.Time // desde quando está RUNNING (zero se não está)
	)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		st, err := readStatus(ctx, client, connectURL, name)
		if err != nil {
			reachable.Store(0)
			runningSince = time.Time{}
			continue
		}
		reachable.Store(1)
		now := time.Now()

		if isFailed, why := st.failed(); isFailed {
			runningSince = time.Time{}
			if !inFailure {
				inFailure = true
				failures.Add(1)
				log.Printf("FALHA detectada: %s", why)
				if attempts.Load() < int64(policy.Attempts) {
					nextRestart = now.Add(policy.Delay(int(attempts.Load()) + 1))
				}
			}
			if attempts.Load() >= int64(policy.Attempts) {
				if gaveUp.Load() == 0 {
					gaveUp.Store(1)
					log.Printf("DESISTÊNCIA: %d tentativas de reinício esgotadas; intervenção manual necessária", policy.Attempts)
				}
				continue
			}
			if now.Before(nextRestart) {
				continue
			}
			n := attempts.Add(1)
			restarts.Add(1)
			if err := restart(ctx, client, connectURL, name); err != nil {
				log.Printf("reinício %d/%d falhou: %v", n, policy.Attempts, err)
			} else {
				log.Printf("reinício %d/%d pedido", n, policy.Attempts)
			}
			// A próxima falha observada (se o reinício não resolver) agenda a tentativa seguinte.
			inFailure = false
			continue
		}

		inFailure = false
		if !st.running() {
			runningSince = time.Time{}
			continue
		}
		if gaveUp.Load() == 1 {
			gaveUp.Store(0)
			log.Printf("conector voltou a RUNNING depois da desistência")
		}
		if runningSince.IsZero() {
			runningSince = now
		}
		if attempts.Load() > 0 && now.Sub(runningSince) >= resetAfter {
			log.Printf("conector estável há %s; contador de tentativas zerado (usadas: %d)", resetAfter, attempts.Load())
			attempts.Store(0)
		}
	}
}

func readStatus(ctx context.Context, c *http.Client, base, name string) (status, error) {
	var st status
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/connectors/"+name+"/status", nil)
	resp, err := c.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status HTTP %d", resp.StatusCode)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

func restart(ctx context.Context, c *http.Client, base, name string) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/connectors/"+name+"/restart?includeTasks=true&onlyFailed=true", nil)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status HTTP %d", resp.StatusCode)
	}
	return nil
}
