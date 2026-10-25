package main

// KV-table semantics, ported from the TypeScript rather than guessed.
//
// fluxer_kv is a single table that stores every "table" of the application:
//
//	table_name, partition_key, row_key, row_data jsonb, expires_at, updated_at, sort_id
//
// with (table_name, row_key) as the primary key. Rows carry their own expiry,
// so pruning is "delete where expires_at <= now()", and the value is untrusted
// JSONB: the API reads it back into a typed row and re-stamps __fluxer_type
// markers.
//
// Two consequences that a naive port misses:
//
//  1. Prune is not the only job of the task. Upstream, prunePostgresKvTtl also
//     ends two one-time migrations for databases that predate per-row expiry:
//     KV rows written before tables carried a default TTL (they have
//     expires_at IS NULL and must get one, or be deleted if already too old),
//     and job-ledger rows written before the ledger TTL existed. Both are
//     guarded by a marker row in table_name = '__fluxer_schema_migrations'.
//     Without them an old row is immortal: expires_at IS NULL never matches the
//     prune predicate.
//
//  2. The default-TTL marker name embeds a hash of the table list
//     ("default_ttl_expiry_v1:<sha256 of sorted name:ttl lines>"). We recompute
//     the same string, so our marker and upstream's are the same row and the
//     pass runs once across both implementations.
//
// Every statement below is a direct translation of
// src/api/database/PostgresKvQueryExecutor.ts,
// src/api/database/PostgresKvDefaultTtlExpiry.ts and
// src/api/jobs/PostgresJobLedgerExpiry.ts from release 2026.1025.35621.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

const (
	kvTableName       = "fluxer_kv"
	kvMigrationTable  = "__fluxer_schema_migrations"
	kvPruneBatchSize  = 5000
	kvPruneMaxBatches = 20
	// Upstream gives the two legacy passes a two-minute budget per run and
	// resumes from a stored cursor on the next cron tick.
	kvLegacyBudget = 2 * time.Minute
	kvPageSize     = 2000
)

// kvDefaultTTLTables mirrors DEFAULT_TTL_TABLES: every table with a
// defaultTtlSeconds, minus the job tables that expire by their own rules.
// Order matters — the legacy pass walks the list in order and resumes by index.
var kvDefaultTTLTables = []struct {
	Name string
	TTL  int
}{
	{"attachment_upload_traces_by_attachment", 2592000},
	{"attachment_upload_traces_by_key", 2592000},
	{"auth_session_tombstones", 2592000},
	{"donor_magic_link_tokens", 900},
	{"donor_magic_link_tokens_by_email", 900},
	{"dsa_report_email_verifications", 600},
	{"dsa_report_tickets", 3600},
	{"email_revert_tokens", 172800},
	{"email_verification_tokens", 86400},
	{"enforcement_decision_evidence", 31536000},
	{"enforcement_decision_notes", 31536000},
	{"guild_audit_logs_v2", 3888000},
	{"guild_audit_logs_v2_by_action", 3888000},
	{"guild_audit_logs_v2_by_user", 3888000},
	{"guild_audit_logs_v2_by_user_action", 3888000},
	{"guild_report_submissions_by_reporter", 86400},
	{"guild_visits_by_guild", 1209600},
	{"guild_visits_by_user", 1209600},
	{"ip_authorization_tokens", 1800},
	{"message_report_submissions_by_reporter", 31536000},
	{"oauth2_access_tokens", 604800},
	{"oauth2_access_tokens_by_user", 604800},
	{"oauth2_authorization_codes", 600},
	{"oauth2_refresh_tokens", 2592000},
	{"oauth2_refresh_tokens_by_user", 2592000},
	{"password_reset_tokens", 86400},
	{"password_reset_tokens_by_user_id", 86400},
	{"push_subscriptions", 7776000},
	{"recent_mentions", 604800},
	{"recent_mentions_by_guild", 604800},
	{"user_report_submissions_by_reporter", 86400},
	{"voice_p2p_connection_reports", 15552000},
}

// Snowflake and ledger constants (packages/snowflake/src/Snowflake.ts,
// src/api/jobs/JobLedgerRetention.ts).
const (
	snowflakeEpoch        int64 = 1420070400000
	snowflakeTimestampBit       = 22
	jobLedgerTTLSeconds         = 90 * 24 * 60 * 60
	jobStaleAfterMS             = 8 * 24 * 60 * 60 * 1000
	expiredJobError             = "Expired from the job queue"
	jobLedgerMarker             = "job_ledger_expiry_v1"
)

// defaultTTLMarker reproduces the TS marker name exactly:
//
//	`default_ttl_expiry_v1:${sha256(sorted "name:ttl" lines).hex.slice(0,16)}`
func defaultTTLMarker() string {
	lines := make([]string, 0, len(kvDefaultTTLTables))
	for _, t := range kvDefaultTTLTables {
		lines = append(lines, fmt.Sprintf("%s:%d", t.Name, t.TTL))
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return "default_ttl_expiry_v1:" + hex.EncodeToString(sum[:])[:16]
}

func defaultTTLResumeKey() string { return defaultTTLMarker() + "_resume" }

// kvPassRecorded reports whether a one-time migration already finished. The
// marker lives inside the KV table itself, under table_name
// '__fluxer_schema_migrations'.
func kvPassRecorded(ctx context.Context, marker string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM `+kvTableName+` WHERE table_name = $1 AND row_key = $2`,
		kvMigrationTable, marker).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// recordKvCleanPass writes the marker row. jsonb_build_object('applied_at',
// now()) is what upstream stores, so a marker written by us is indistinguishable
// from one written by the Node worker.
func recordKvCleanPass(ctx context.Context, marker string) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO `+kvTableName+` (table_name, partition_key, row_key, row_data)
VALUES ($1, $2, $2, jsonb_build_object('applied_at', now()))
ON CONFLICT (table_name, row_key) DO UPDATE SET row_data = EXCLUDED.row_data, updated_at = now()`,
		kvMigrationTable, marker)
	return err
}

type kvResumePoint struct {
	Table     string `json:"table"`
	RowKey    string `json:"row_key"`
	Unsettled int64  `json:"unset"`
}

func readResumePoint(ctx context.Context, key string) (*kvResumePoint, error) {
	var raw []byte
	err := db.QueryRowContext(ctx,
		`SELECT row_data FROM `+kvTableName+` WHERE table_name = $1 AND row_key = $2`,
		kvMigrationTable, key).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p kvResumePoint
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, nil
	}
	if p.Table == "" || p.RowKey == "" {
		return nil, nil
	}
	return &p, nil
}

func writeResumePoint(ctx context.Context, key string, p *kvResumePoint) error {
	if p == nil {
		_, err := db.ExecContext(ctx,
			`DELETE FROM `+kvTableName+` WHERE table_name = $1 AND row_key = $2`,
			kvMigrationTable, key)
		return err
	}
	_, err := db.ExecContext(ctx,
		`INSERT INTO `+kvTableName+` (table_name, partition_key, row_key, row_data)
VALUES ($1, $2, $2, jsonb_build_object('table', $3::text, 'row_key', $4::text, 'unset', $5::bigint))
ON CONFLICT (table_name, row_key) DO UPDATE SET row_data = EXCLUDED.row_data, updated_at = now()`,
		kvMigrationTable, key, p.Table, p.RowKey, p.Unsettled)
	return err
}

// pruneExpiredKVRows is the plain prune: rows that already carried an expiry.
func pruneExpiredKVRows(ctx context.Context, batchSize int) (int64, error) {
	const q = `
WITH expired AS (
	SELECT table_name, row_key
	FROM ` + kvTableName + `
	WHERE expires_at IS NOT NULL AND expires_at <= now()
	ORDER BY expires_at
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
DELETE FROM ` + kvTableName + ` kv
USING expired
WHERE kv.table_name = expired.table_name AND kv.row_key = expired.row_key`
	res, err := db.ExecContext(ctx, q, batchSize)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

const kvDefaultTTLPageSQL = `
WITH page AS (
	SELECT kv.row_key, kv.expires_at IS NULL AS unset
	FROM %s kv
	WHERE kv.table_name = $1 AND kv.row_key > $2
	ORDER BY kv.row_key
	LIMIT $3
), removed AS (
	DELETE FROM %s kv
	USING page
	WHERE kv.table_name = $1 AND kv.row_key = page.row_key AND kv.expires_at IS NULL
		AND kv.updated_at + make_interval(secs => $4::double precision) <= now()
	RETURNING 1
), expiring AS (
	UPDATE %s kv
	SET expires_at = kv.updated_at + make_interval(secs => $4::double precision)
	FROM page
	WHERE kv.table_name = $1 AND kv.row_key = page.row_key AND kv.expires_at IS NULL
		AND kv.updated_at + make_interval(secs => $4::double precision) > now()
	RETURNING 1
)
SELECT
	(SELECT max(row_key) FROM page) AS last_row_key,
	(SELECT count(*) FROM page WHERE unset) AS unset,
	(SELECT count(*) FROM removed) AS deleted,
	(SELECT count(*) FROM expiring) AS expiring`

// expireLegacyDefaultTTLRows gives rows that predate per-table default TTLs an
// expiry, and deletes the ones already past it. It is a one-time pass: it
// records a marker and returns nil on every later run.
func expireLegacyDefaultTTLRows(ctx context.Context, deadline time.Time) (deleted, expiring int64, err error) {
	marker := defaultTTLMarker()
	recorded, err := kvPassRecorded(ctx, marker)
	if err != nil {
		return 0, 0, err
	}
	if recorded {
		return 0, 0, nil
	}
	resumeKey := defaultTTLResumeKey()
	resume, err := readResumePoint(ctx, resumeKey)
	if err != nil {
		return 0, 0, err
	}
	resumeIndex := -1
	var unset int64
	cursor := ""
	if resume != nil {
		for i, t := range kvDefaultTTLTables {
			if t.Name == resume.Table {
				resumeIndex = i
				cursor = resume.RowKey
				break
			}
		}
		unset = resume.Unsettled
	}
	sqlText := fmt.Sprintf(kvDefaultTTLPageSQL, kvTableName, kvTableName, kvTableName)
	start := resumeIndex
	if start < 0 {
		start = 0
	}
	for idx := start; idx < len(kvDefaultTTLTables); idx++ {
		target := kvDefaultTTLTables[idx]
		if idx != resumeIndex {
			cursor = ""
		}
		for {
			if time.Now().After(deadline) {
				if werr := writeResumePoint(ctx, resumeKey, &kvResumePoint{Table: target.Name, RowKey: cursor, Unsettled: unset}); werr != nil {
					return deleted, expiring, werr
				}
				return deleted, expiring, nil
			}
			var lastRowKey sql.NullString
			var pageUnset, pageDeleted, pageExpiring int64
			row := db.QueryRowContext(ctx, sqlText, target.Name, cursor, kvPageSize, target.TTL)
			if scanErr := row.Scan(&lastRowKey, &pageUnset, &pageDeleted, &pageExpiring); scanErr != nil {
				return deleted, expiring, scanErr
			}
			if !lastRowKey.Valid || lastRowKey.String == "" {
				break
			}
			unset += pageUnset
			deleted += pageDeleted
			expiring += pageExpiring
			cursor = lastRowKey.String
		}
	}
	if err := writeResumePoint(ctx, resumeKey, nil); err != nil {
		return deleted, expiring, err
	}
	if unset == 0 {
		if err := recordKvCleanPass(ctx, marker); err != nil {
			return deleted, expiring, err
		}
		log.Printf("task prunePostgresKvTtl: default-TTL backfill finished (%d rows expired, %d scheduled to expire)", deleted, expiring)
	}
	return deleted, expiring, nil
}

// bigintKeyExpr unwraps the KV row key of a job ledger row:
// '{"__fluxer_type":"bigint","value":"<id>"}'.
func bigintKeyExpr(key string) string {
	const prefix = `{"__fluxer_type":"bigint","value":"`
	const suffix = `"}`
	plen := len(prefix)
	slen := len(suffix)
	return fmt.Sprintf(`(CASE WHEN left(%s, %d) = '%s' AND right(%s, %d) = '%s' THEN substr(%s, %d, length(%s) - %d)::numeric END)`,
		key, plen, prefix, key, slen, suffix, key, plen+1, key, plen+slen)
}

type ledgerTarget struct {
	Name                string
	JobIDExpr           string
	DeleteStale         bool
	MarkStaleDeadletter bool
}

var ledgerTables = func() []ledgerTarget {
	return []ledgerTarget{
		{Name: "jobs_active", JobIDExpr: bigintKeyExpr("kv.row_key"), DeleteStale: true},
		{Name: "jobs_by_id", JobIDExpr: bigintKeyExpr("kv.row_key"), MarkStaleDeadletter: true},
		{Name: "jobs_by_day_bucket", JobIDExpr: bigintKeyExpr("split_part(kv.row_key, chr(31), 3)")},
	}
}()

func ledgerPageSQL(t ledgerTarget) string {
	staleRemovable := "(c.created_at < $7 AND c.system_job)"
	if t.DeleteStale {
		staleRemovable = "c.created_at < $7"
	}
	removable := "(c.job_id IS NULL OR c.created_at < $6 OR " + staleRemovable + ")"
	rowData := "kv.row_data"
	if t.MarkStaleDeadletter {
		rowData = `CASE WHEN c.unfinished THEN kv.row_data || jsonb_build_object('status', 'deadletter', 'error_message', $9::text, 'completed_at', jsonb_build_object('__fluxer_type', 'date', 'value', $10::text)) ELSE kv.row_data END`
	}
	return `
WITH page AS (
	SELECT kv.row_key, kv.row_data, ` + t.JobIDExpr + ` AS job_id
	FROM ` + kvTableName + ` kv
	WHERE kv.table_name = $1 AND kv.expires_at IS NULL AND kv.row_key > $2
	ORDER BY kv.row_key
	LIMIT $3
), classified AS (
	SELECT
		page.row_key,
		page.job_id,
		to_timestamp(((div(page.job_id, $4::numeric) + $5::numeric) / 1000)::double precision) AS created_at,
		COALESCE(page.row_data -> 'requested_by_user_id', 'null'::jsonb) = 'null'::jsonb AS system_job,
		COALESCE(page.row_data ->> 'status' IN ('queued', 'running'), false) AS unfinished
	FROM page
), removed AS (
	DELETE FROM ` + kvTableName + ` kv
	USING classified c
	WHERE kv.table_name = $1 AND kv.row_key = c.row_key AND kv.expires_at IS NULL AND (` + removable + `)
	RETURNING kv.row_key
), expiring AS (
	UPDATE ` + kvTableName + ` kv
	SET expires_at = c.created_at + make_interval(secs => $8::double precision), updated_at = now(), row_data = ` + rowData + `
	FROM classified c
	WHERE kv.table_name = $1 AND kv.row_key = c.row_key AND kv.expires_at IS NULL AND c.created_at < $7 AND NOT (` + removable + `)
	RETURNING kv.row_key
)
SELECT
	(SELECT max(row_key) FROM page) AS last_row_key,
	(SELECT count(*) FROM page) AS scanned,
	(SELECT count(*) FROM removed) AS deleted,
	(SELECT count(*) FROM expiring) AS expiring`
}

// expireLegacyJobLedgerRows is the job-ledger counterpart: jobs written before
// the ledger carried its own TTL get one now, old finished jobs are deleted, and
// jobs that sat queued past the stale window are flipped to 'deadletter' so the
// UI stops showing them as pending.
func expireLegacyJobLedgerRows(ctx context.Context, deadline time.Time) (deleted, expiring int64, err error) {
	recorded, err := kvPassRecorded(ctx, jobLedgerMarker)
	if err != nil {
		return 0, 0, err
	}
	if recorded {
		return 0, 0, nil
	}
	now := time.Now()
	retentionCutoff := now.Add(-time.Duration(jobLedgerTTLSeconds) * time.Second)
	staleCutoff := now.Add(-time.Duration(jobStaleAfterMS) * time.Millisecond)
	completedAt := now.UTC().Format(time.RFC3339Nano)
	var scanned int64
	for _, target := range ledgerTables {
		sqlText := ledgerPageSQL(target)
		deadletterArgs := []any{}
		if target.MarkStaleDeadletter {
			deadletterArgs = []any{expiredJobError, completedAt}
		}
		cursor := ""
		for {
			if time.Now().After(deadline) {
				return deleted, expiring, nil
			}
			args := []any{target.Name, cursor, kvPageSize, snowflakeTimestampBit, snowflakeEpoch, retentionCutoff, staleCutoff, jobLedgerTTLSeconds}
			args = append(args, deadletterArgs...)
			var lastRowKey sql.NullString
			var pageScanned, pageDeleted, pageExpiring int64
			row := db.QueryRowContext(ctx, sqlText, args...)
			if scanErr := row.Scan(&lastRowKey, &pageScanned, &pageDeleted, &pageExpiring); scanErr != nil {
				return deleted, expiring, scanErr
			}
			if !lastRowKey.Valid || lastRowKey.String == "" {
				break
			}
			scanned += pageScanned
			deleted += pageDeleted
			expiring += pageExpiring
			cursor = lastRowKey.String
		}
	}
	if scanned == 0 {
		if err := recordKvCleanPass(ctx, jobLedgerMarker); err != nil {
			return deleted, expiring, err
		}
	}
	return deleted, expiring, nil
}

// taskPrunePostgresKvTtl mirrors PrunePostgresKvTtl.ts: two bounded legacy
// passes, then up to 20 prune batches of 5000, stopping on the first short one.
func taskPrunePostgresKvTtl(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("prunePostgresKvTtl: %w", err)
		}
	}
	deadline := time.Now().Add(kvLegacyBudget)
	if deleted, expiring, err := expireLegacyJobLedgerRows(ctx, deadline); err != nil {
		log.Printf("task prunePostgresKvTtl: job ledger pass: %v (continuing)", err)
	} else if deleted > 0 || expiring > 0 {
		log.Printf("task prunePostgresKvTtl: job ledger pass deleted=%d expiring=%d", deleted, expiring)
	}
	if deleted, expiring, err := expireLegacyDefaultTTLRows(ctx, deadline); err != nil {
		log.Printf("task prunePostgresKvTtl: default TTL pass: %v (continuing)", err)
	} else if deleted > 0 || expiring > 0 {
		log.Printf("task prunePostgresKvTtl: default TTL pass deleted=%d expiring=%d", deleted, expiring)
	}
	var total int64
	for i := 0; i < kvPruneMaxBatches; i++ {
		n, err := pruneExpiredKVRows(ctx, kvPruneBatchSize)
		if err != nil {
			return fmt.Errorf("batch %d: %w", i, err)
		}
		total += n
		if n < kvPruneBatchSize {
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
