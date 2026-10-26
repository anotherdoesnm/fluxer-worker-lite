package main

// flushUserActivityBuffer: turn the two valkey activity buffers into row updates.
//
// The api does not write activity straight into Postgres. On every authenticated
// request it does HSET into `user_activity:pending` (field = user id, value =
// {"ts":…,"ip":…}) and, debounced by a five minute `auth_session_activity:touched:`
// key, into `auth_session_activity:pending` (field = base64url of the session id
// hash, value = {"ts":…}). This task drains both hashes and flushes them: the user
// entries become `last_active_at` / `last_active_ip` on the users row, the session
// entries become `approx_last_used_at` on auth_sessions.
//
// Draining is HGETALL + DEL in one transaction: if the read and the delete were not
// atomic, two workers (or a retry) could flush the same batch twice, or worse, lose
// a batch that arrived between the two calls. Whatever was drained is written
// afterwards with best effort - a failed write is logged and dropped, because stale
// activity data is not worth replaying and the next request will buffer it again.
//
// Session entries are filtered against the live rows before writing: a session can
// be revoked between the request that buffered the touch and this flush, and
// patching a row that no longer exists would resurrect it (putKV-style merge) or
// simply churn. Upstream does the same existence read.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	userActivityKey        = "user_activity:pending"
	authSessionActivityKey = "auth_session_activity:pending"
)

// pendingActivity is one buffered touch. ip is only present for user entries.
type pendingActivity struct {
	TS int64   `json:"ts"`
	IP *string `json:"ip"`
}

// parsePendingActivity is shared by both buffers; a malformed value is skipped rather
// than failing the whole flush.
func parsePendingActivity(raw string) (pendingActivity, bool) {
	var e pendingActivity
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return pendingActivity{}, false
	}
	if e.TS <= 0 {
		return pendingActivity{}, false
	}
	return e, true
}

// drainHash reads and deletes a whole hash atomically, so a concurrent writer either
// lands in this batch or in the next one, never in neither.
func drainHash(ctx context.Context, key string) (map[string]string, error) {
	var cmds []redis.Cmder
	_, err := valkey().TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		cmds = append(cmds, pipe.HGetAll(ctx, key), pipe.Del(ctx, key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	pending, ok := cmds[0].(*redis.MapStringStringCmd)
	if !ok {
		return nil, fmt.Errorf("drain %s: unexpected read command", key)
	}
	entries, err := pending.Result()
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// sessionHashFromBase64URL turns the buffer's field name back into the exact value
// stored in auth_sessions.session_id_hash: the api encodes with base64url, the KV
// column holds standard base64.
func sessionHashFromBase64URL(field string) (string, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(field)
	if err != nil || len(raw) == 0 {
		return "", false
	}
	return base64.StdEncoding.EncodeToString(raw), true
}

func flushUserActivity(ctx context.Context) (users, sessions int, err error) {
	entries, err := drainHash(ctx, userActivityKey)
	if err != nil {
		return 0, 0, fmt.Errorf("drain user activity: %w", err)
	}
	for userID, raw := range entries {
		entry, ok := parsePendingActivity(raw)
		if !ok {
			continue
		}
		written, err := writeUserActivity(ctx, userID, entry)
		if err != nil {
			log.Printf("flushUserActivityBuffer: %s: %v", userID, err)
			continue
		}
		if written {
			users++
		}
	}

	sessionEntries, err := drainHash(ctx, authSessionActivityKey)
	if err != nil {
		return users, sessions, fmt.Errorf("drain session activity: %w", err)
	}
	for field, raw := range sessionEntries {
		entry, ok := parsePendingActivity(raw)
		if !ok {
			continue
		}
		hash, ok := sessionHashFromBase64URL(field)
		if !ok {
			continue
		}
		if err := writeSessionActivity(ctx, hash, entry.TS); err != nil {
			log.Printf("flushUserActivityBuffer: session %s: %v", field, err)
			continue
		}
		sessions++
	}
	return users, sessions, nil
}

// deletedAccountFlag keeps activity from being written back into an account that was
// anonymised in the meantime: a buffered touch can arrive after the deletion ran, and
// resurrecting last_active_at (and with it the IP) would leak what deletion removed.
const deletedAccountFlag = 17179869184

// writeUserActivity reports whether a row was actually updated: a deleted account is
// skipped by the guard, and counting it as flushed would make the log lie.
func writeUserActivity(ctx context.Context, userID string, entry pendingActivity) (bool, error) {
	at := dateValue(time.UnixMilli(entry.TS).UTC())
	if entry.IP == nil || *entry.IP == "" {
		// No IP in the buffer means "unknown", and upstream clears the column in
		// that case rather than leaving a stale address behind.
		res, err := db.ExecContext(ctx, `
UPDATE fluxer_kv SET
	row_data = (row_data - 'last_active_ip') || jsonb_build_object('last_active_at', $2::jsonb),
	updated_at = now()
WHERE table_name = 'users' AND row_data->'user_id'->>'value' = $1
  AND (coalesce((row_data->'flags'->>'value')::bigint, 0) & $3) = 0`, userID, string(mustJSON(at)), deletedAccountFlag)
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		return n > 0, nil
	}
	res, err := db.ExecContext(ctx, `
UPDATE fluxer_kv SET
	row_data = row_data || jsonb_build_object('last_active_at', $2::jsonb, 'last_active_ip', $3::jsonb),
	updated_at = now()
WHERE table_name = 'users' AND row_data->'user_id'->>'value' = $1
  AND (coalesce((row_data->'flags'->>'value')::bigint, 0) & $4) = 0`, userID, string(mustJSON(at)), string(mustJSON(*entry.IP)), deletedAccountFlag)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func writeSessionActivity(ctx context.Context, sessionHash string, ts int64) error {
	at := dateValue(time.UnixMilli(ts).UTC())
	// The row key is the hashed token in the KV buffer encoding, and the same value
	// lives in session_id_hash, so matching on it also proves the session still
	// exists - a revoked session must not be written back.
	res, err := db.ExecContext(ctx, `
UPDATE fluxer_kv SET
	row_data = row_data || jsonb_build_object('approx_last_used_at', $2::jsonb),
	updated_at = now()
WHERE table_name = 'auth_sessions' AND row_data->'session_id_hash'->>'value' = $1`,
		sessionHash, string(mustJSON(at)))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// Stale touch for a session that is gone: nothing to do, not an error.
		return nil
	}
	return nil
}

func taskFlushUserActivityBuffer(ctx context.Context, _ map[string]any) error {
	if err := openDB(); err != nil {
		return fmt.Errorf("flushUserActivityBuffer: %w", err)
	}
	users, sessions, err := flushUserActivity(ctx)
	if err != nil {
		return fmt.Errorf("flushUserActivityBuffer: %w", err)
	}
	if users > 0 || sessions > 0 {
		log.Printf("flushUserActivityBuffer: users=%d sessions=%d", users, sessions)
	}
	return nil
}
