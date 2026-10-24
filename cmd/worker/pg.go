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

// pruneExpiredKVRows is a straight port of pruneExpiredPostgresKvRows()
// (fluxer_api/src/api/database/PostgresKvQueryExecutor.ts): delete rows whose
// expires_at has passed, bounded by LIMIT, with SKIP LOCKED so a second worker
// can run it concurrently.
func pruneExpiredKVRows(ctx context.Context, batchSize int) (int64, error) {
	const q = `
	SELECT table_name, row_key
	FROM fluxer_kv
	WHERE expires_at IS NOT NULL AND expires_at <= now()
	ORDER BY expires_at
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
DELETE FROM fluxer_kv kv
USING expired
WHERE kv.table_name = expired.table_name AND kv.row_key = expired.row_key`
	res, err := db.ExecContext(ctx, q, batchSize)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// taskPrunePostgresKvTtl mirrors the TS handler: up to 20 batches of 5000,
// stopping as soon as a batch comes back short. The legacy-TTL sweeps from the
// original are not ported yet and are logged as such.
func taskPrunePostgresKvTtl(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("prunePostgresKvTtl: %w", err)
		}
	}
	const batchSize = 5000
	const maxBatches = 20
	var total int64
	for i := 0; i < maxBatches; i++ {
		n, err := pruneExpiredKVRows(ctx, batchSize)
		if err != nil {
			return fmt.Errorf("batch %d: %w", i, err)
		}
		total += n
		if n < batchSize {
			break
		}
	}
	if total > 0 {
		log.Printf("task prunePostgresKvTtl: deleted %d expired kv rows", total)
	} else if envInt("LOG_TASKS", 0) == 1 {
		log.Printf("task prunePostgresKvTtl: nothing to prune")
	}
	return nil
}
