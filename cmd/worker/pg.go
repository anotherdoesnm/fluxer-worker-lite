package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// db is opened once, lazily, and shared by handlers. Nil means "no database
// configured": handlers that need it then fail loudly instead of silently
// doing nothing.
var db *sql.DB

func openDB() error {
	if db != nil {
		return nil
	}
	host := env("PGHOST", "")
	if host == "" {
		return fmt.Errorf("PGHOST is not set")
	}
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host,
		env("PGPORT", "5432"),
		env("PGUSER", "fluxer"),
		os.Getenv("PGPASSWORD"),
		env("PGDATABASE", "fluxer"),
		env("PGSSLMODE", "disable"),
	)
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	conn.SetMaxOpenConns(4)
	conn.SetMaxIdleConns(2)
	conn.SetConnMaxLifetime(30 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return err
	}
	db = conn
	log.Printf("postgres: connected to %s/%s as %s", host, env("PGDATABASE", "fluxer"), env("PGUSER", "fluxer"))
	return nil
}

// The KV prune itself lives in pg_kv.go together with the two legacy passes
// that share the task: see that file for the fluxer_kv layout and for why
// expires_at IS NULL is not the same thing as "never expires".
