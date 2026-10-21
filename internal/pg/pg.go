// Package pg concentra a conexão com o PostgreSQL.
package pg

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect abre um pool e espera o banco aceitar conexões (até 60 s).
func Connect(ctx context.Context, dsn string, maxConns int32) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatalf("DSN inválido: %v", err)
	}
	cfg.MaxConns = maxConns
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"

	deadline := time.Now().Add(60 * time.Second)
	for {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			log.Fatalf("não foi possível conectar ao banco: %v", err)
		}
		log.Printf("aguardando o banco: %v", err)
		time.Sleep(2 * time.Second)
	}
}
