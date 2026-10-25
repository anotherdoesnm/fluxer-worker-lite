package main

// Job ledger sweep, ported from src/api/worker/tasks/ExpireStaleJobs.ts and
// JobLedgerRepository.expireStaleActiveJobs (release 2026.1025.35621).
//
// The ledger is two KV tables over the same job id:
//
//	jobs_active         — what the queue still owns; a row here means "pending"
//	jobs_by_id          — the job record, carrying status/result/progress
//	jobs_by_day_bucket  — an index by day and creation time
//
// Both use the KV bigint row key, so the id lives inside the key as
// '{"__fluxer_type":"bigint","value":"<id>"}' and not in a column. A job that
// sat in jobs_active longer than JOB_STALE_AFTER_MS (8 days, the jobs stream's
// 7-day MaxAge plus a day) is not going to be delivered again: it is either
// deadlettered — so the UI stops showing "running" forever — or its jobs_active
// row is dropped when the job had already reached a terminal state.
//
// markTerminal() also re-stamps the jobs_by_id row with a 90-day expiry, which
// is why this handler must write expires_at: without it the ledger row would
// never be pruned (expires_at IS NULL never matches the prune predicate).

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	staleJobsPageSize   = 500
	staleJobsMaxCleared = 1000
	bigintKeyPrefix     = `{"__fluxer_type":"bigint","value":"`
	bigintKeySuffix     = `"}`
)

// snowflakeTime turns a Fluxer snowflake into its creation time:
// (id >> 22) + 1420070400000 ms.
func snowflakeTime(id int64) time.Time {
	ms := (id >> snowflakeTimeShift) + snowflakeEpoch
	return time.UnixMilli(ms).UTC()
}

const snowflakeTimeShift = 22

func bigintRowKey(id string) string {
	return bigintKeyPrefix + id + bigintKeySuffix
}

// parseBigintRowKey extracts the id from a KV bigint row key.
func parseBigintRowKey(key string) (string, bool) {
	if !strings.HasPrefix(key, bigintKeyPrefix) || !strings.HasSuffix(key, bigintKeySuffix) {
		return "", false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(key, bigintKeyPrefix), bigintKeySuffix)
	if id == "" {
		return "", false
	}
	if _, err := strconv.ParseInt(id, 10, 64); err != nil {
		return "", false
	}
	return id, true
}

// staleActivePage reads one keyset page of jobs_active ids after cursor.
func staleActivePage(ctx context.Context, cursor string, limit int) ([]string, error) {
	const q = `
SELECT kv.row_key
FROM ` + kvTableName + ` kv
WHERE kv.table_name = 'jobs_active' AND kv.row_key > $1
ORDER BY kv.row_key
LIMIT $2`
	rows, err := db.QueryContext(ctx, q, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}

// jobStatus reads the status field of a jobs_by_id row, if the row exists.
func jobStatus(ctx context.Context, key string) (string, bool, error) {
	var status sql.NullString
	err := db.QueryRowContext(ctx,
		`SELECT row_data ->> 'status' FROM `+kvTableName+` WHERE table_name = 'jobs_by_id' AND row_key = $1`,
		key).Scan(&status)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return status.String, true, nil
}

// markJobDeadletter mirrors markTerminal(jobId, "deadletter", {error_message}):
// patch the record with a 90-day expiry, then drop the active row.
func markJobDeadletter(ctx context.Context, key, reason string, completedAt time.Time) error {
	const patchQ = `
UPDATE ` + kvTableName + `
SET row_data = row_data || jsonb_build_object(
		'status', 'deadletter',
		'error_message', $2::text,
		'completed_at', jsonb_build_object('__fluxer_type', 'date', 'value', $3::text)),
	updated_at = now(),
	expires_at = now() + make_interval(secs => $4::double precision)
WHERE table_name = 'jobs_by_id' AND row_key = $1`
	if _, err := db.ExecContext(ctx, patchQ, key, reason, completedAt.UTC().Format(time.RFC3339Nano), jobLedgerTTLSeconds); err != nil {
		return err
	}
	return dropActiveJob(ctx, key)
}

func dropActiveJob(ctx context.Context, key string) error {
	_, err := db.ExecContext(ctx,
		`DELETE FROM `+kvTableName+` WHERE table_name = 'jobs_active' AND row_key = $1`, key)
	return err
}

// taskExpireStaleJobs mirrors ExpireStaleJobs.ts: page through jobs_active in
// row_key order, deadletter anything still queued or running past the stale
// window, drop the active row for everything else, and stop after 1000 jobs so
// one run cannot hold a lane for minutes. A run that hit the cap reports
// complete=false upstream, which is the difference between "nothing left" and
// "come back next tick".
func taskExpireStaleJobs(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("expireStaleJobs: %w", err)
		}
	}
	staleBefore := time.Now().Add(-time.Duration(jobStaleAfterMS) * time.Millisecond)
	completedAt := time.Now().UTC()
	var cleared, expired int
	cursor := ""
	for {
		keys, err := staleActivePage(ctx, cursor, staleJobsPageSize)
		if err != nil {
			return fmt.Errorf("page after %q: %w", cursor, err)
		}
		if len(keys) == 0 {
			break
		}
		for _, key := range keys {
			id, ok := parseBigintRowKey(key)
			if !ok {
				// A row we cannot date is not ours to delete: leave it and say so.
				log.Printf("task expireStaleJobs: unrecognised jobs_active row key %q (skipped)", key)
				continue
			}
			numericID, _ := strconv.ParseInt(id, 10, 64)
			if !snowflakeTime(numericID).Before(staleBefore) {
				continue
			}
			if cleared >= staleJobsMaxCleared {
				log.Printf("task expireStaleJobs: hit the per-run cap cleared=%d expired=%d (continuing next tick)", cleared, expired)
				return nil
			}
			status, found, err := jobStatus(ctx, key)
			if err != nil {
				return fmt.Errorf("read status of job %s: %w", id, err)
			}
			if found && (status == "queued" || status == "running") {
				if err := markJobDeadletter(ctx, key, expiredJobError, completedAt); err != nil {
					return fmt.Errorf("deadletter job %s: %w", id, err)
				}
				expired++
			} else {
				if err := dropActiveJob(ctx, key); err != nil {
					return fmt.Errorf("drop active job %s: %w", id, err)
				}
			}
			cleared++
		}
		cursor = keys[len(keys)-1]
	}
	if cleared > 0 || expired > 0 {
		log.Printf("task expireStaleJobs: cleared=%d deadlettered=%d", cleared, expired)
	} else if envInt("LOG_TASKS", 0) == 1 {
		log.Printf("task expireStaleJobs: nothing stale")
	}
	return nil
}
