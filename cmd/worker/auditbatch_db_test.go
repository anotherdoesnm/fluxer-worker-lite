package main

// Integration test for the SQL half of batchGuildAuditLogMessageDeletes.
//
// It needs a database it is allowed to write to and it must never be the live
// one, so it only runs when FLUXER_TEST_PG_DSN points at a scratch copy of
// fluxer_infra (a restored dump is enough) and skips otherwise. Rows are seeded
// under a synthetic guild id and deleted again at the end.

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"
)

const testGuildID = "1999999999999999999"

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLUXER_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLUXER_TEST_PG_DSN is not set: skipping the database half of the batching task")
	}
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := conn.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	previous := db
	db = conn
	t.Cleanup(func() {
		db = previous
		_ = conn.Close()
	})
	return conn
}

func seedAuditLog(t *testing.T, entry auditLogEntry, options [][2]string) {
	t.Helper()
	row := map[string]any{
		"log_id":      bigintValue(entry.LogID),
		"guild_id":    bigintValue(testGuildID),
		"user_id":     bigintValue(entry.UserID),
		"target_id":   nil,
		"action_type": entry.ActionType,
		"reason":      nil,
		"options":     auditOptionsValue(options),
		"changes":     nil,
	}
	expires := time.Now().Add(45 * 24 * time.Hour)
	keys := auditLogKeys(testGuildID, entry)
	for _, key := range keys {
		if _, err := db.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, key.table, key.rowKey); err != nil {
			t.Fatalf("pre-clean %s: %v", key.table, err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, key := range keys {
		if err := insertAuditLogTx(context.Background(), tx, key.table, key.partition, key.rowKey, row, expires); err != nil {
			t.Fatalf("seed %s: %v", key.table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seeding: %v", err)
	}
}

func rowCount(t *testing.T, table, rowKey string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, table, rowKey).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestBatchConsecutiveMessageDeletesAgainstDB(t *testing.T) {
	openTestDB(t)
	t.Cleanup(func() {
		if _, err := db.Exec(`DELETE FROM fluxer_kv WHERE row_key LIKE $1 || '%'`, bigintSeg(testGuildID)); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	})

	const user = "1888888888888888888"
	const channel = "1777777777777777777"

	// Two runs, separated by an unrelated action: ids are equal width so that
	// byte order of the row keys equals numeric order, like real snowflakes.
	seedAuditLog(t, del("1004", user, channel), [][2]string{{"channel_id", channel}})
	seedAuditLog(t, del("1003", user, channel), [][2]string{{"channel_id", channel}})
	seedAuditLog(t, del("1002", user, channel), [][2]string{{"channel_id", channel}})
	seedAuditLog(t, auditLogEntry{LogID: "1001", UserID: user, ActionType: 30}, nil)
	seedAuditLog(t, del("1000", user, channel), [][2]string{{"channel_id", channel}})

	deletes, err := listGuildAuditLogs(context.Background(), testGuildID, auditActionMessageDelete, auditLogListLimit)
	if err != nil {
		t.Fatalf("list deletes: %v", err)
	}
	if len(deletes) != 4 {
		t.Fatalf("got %d deletions, want 4", len(deletes))
	}
	all, err := listGuildAuditLogs(context.Background(), testGuildID, 0, auditLogListLimit)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("got %d entries, want 5", len(all))
	}
	if all[0].LogID != "1004" {
		t.Fatalf("listing is not log_id DESC: first is %s", all[0].LogID)
	}

	groups := groupConsecutiveMessageDeletes(all)
	if len(groups) != 1 {
		t.Fatalf("got %d groups, want 1 (the singleton and the action row split the rest)", len(groups))
	}
	if len(groups[0].Logs) != 3 {
		t.Fatalf("group has %d logs, want 3", len(groups[0].Logs))
	}

	if err := batchOneDeleteGroup(context.Background(), nil, testGuildID, "1005", groups[0]); err != nil {
		t.Fatalf("batch: %v", err)
	}

	batched := auditLogEntry{LogID: "1005", UserID: user, ActionType: auditActionMessageBulkDelete}
	for _, key := range auditLogKeys(testGuildID, batched) {
		if rowCount(t, key.table, key.rowKey) != 1 {
			t.Fatalf("%s: the batched entry was not stored", key.table)
		}
	}
	for _, id := range []string{"1004", "1003", "1002"} {
		for _, key := range auditLogKeys(testGuildID, del(id, user, channel)) {
			if rowCount(t, key.table, key.rowKey) != 0 {
				t.Fatalf("%s: original entry %s survived", key.table, id)
			}
		}
	}
	// Untouched neighbours must stay.
	if rowCount(t, auditLogsTable, bigintSeg(testGuildID)+kvSep+bigintSeg("1001")) != 1 {
		t.Fatal("the unrelated action entry was deleted")
	}
	if rowCount(t, auditLogsTable, bigintSeg(testGuildID)+kvSep+bigintSeg("1000")) != 1 {
		t.Fatal("the singleton deletion was deleted")
	}

	stored, err := listGuildAuditLogs(context.Background(), testGuildID, auditActionMessageBulkDelete, 10)
	if err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if len(stored) != 1 || stored[0].ChannelID != channel {
		t.Fatalf("batched entry lost its channel: %+v", stored)
	}

	var raw string
	if err := db.QueryRow(`SELECT row_data::text FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		auditLogsTable, bigintSeg(testGuildID)+kvSep+bigintSeg("1005")).Scan(&raw); err != nil {
		t.Fatalf("read batched row: %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		t.Fatalf("decode batched row: %v", err)
	}
	if count, ok := auditOption(data["options"], "count"); !ok || count != "3" {
		t.Fatalf("count option: got %q/%v, want \"3\"", count, ok)
	}
	if action, ok := data["action_type"].(float64); !ok || int64(action) != auditActionMessageBulkDelete {
		t.Fatalf("action_type: got %v", data["action_type"])
	}

	var expires time.Time
	if err := db.QueryRow(`SELECT expires_at FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		auditLogsTable, bigintSeg(testGuildID)+kvSep+bigintSeg("1005")).Scan(&expires); err != nil {
		t.Fatalf("read expires_at: %v", err)
	}
	wantTTL := time.Now().Add(time.Duration(auditLogTTLSeconds) * time.Second)
	if diff := expires.Sub(wantTTL); diff > time.Minute || diff < -time.Minute {
		t.Fatalf("expires_at is %v, want about %v", expires, wantTTL)
	}
}
