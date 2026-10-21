// Package config lê a configuração dos serviços a partir de variáveis de ambiente.
package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Modos de propagação (fator principal do experimento).
const (
	ModeDomainEvents = "domain-events"
	ModeCDC          = "cdc"
)

func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func Int(key string, def int) int {
	v := String(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("variável %s inválida (%q): %v", key, v, err)
	}
	return n
}

func Float(key string, def float64) float64 {
	v := String(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Fatalf("variável %s inválida (%q): %v", key, v, err)
	}
	return f
}

func Duration(key string, def time.Duration) time.Duration {
	v := String(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("variável %s inválida (%q): %v", key, v, err)
	}
	return d
}

func List(key, def string) []string {
	var out []string
	for _, p := range strings.Split(String(key, def), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Mode lê PROPAGATION_MODE e aborta se o valor não for um dos dois modelos.
func Mode() string {
	m := String("PROPAGATION_MODE", "")
	if m != ModeDomainEvents && m != ModeCDC {
		log.Fatalf("PROPAGATION_MODE deve ser %q ou %q, recebido %q", ModeDomainEvents, ModeCDC, m)
	}
	return m
}
