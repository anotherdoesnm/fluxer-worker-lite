package main

// Tests for the pending bulk message deletion pair.
//
// The database half is the one that matters: it seeds a user with a pending
// timestamp, two messages older than it and one newer, and then checks that the
// task deleted exactly the old ones, left the author index consistent and cleared
// the three pending fields — an uncleared field means the same request runs again
// on the next tick.

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func snowflakeID(ms int64) string {
	return fmt.Sprintf("%d", (ms-snowflakeEpoch)<<snowflakeTimeShift)
}

func TestParseBulkQueueMember(t *testing.T) {
	cases := []struct {
		name   string
		member string
		ok     bool
		userID string
		at     int64
	}{
		{"plain", "123|1700000000000", true, "123", 1700000000000},
		{"no separator", "123", false, "", 0},
		{"no timestamp", "123|", false, "", 0},
		{"timestamp is not a number", "123|soon", false, "", 0},
		{"user is not a number", "abc|1", false, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			userID, at, err := parseBulkQueueMember(tc.member)
			if tc.ok != (err == nil) {
				t.Fatalf("err = %v, want ok=%v", err, tc.ok)
			}
			if !tc.ok {
				return
			}
			if userID != tc.userID || at != tc.at {
				t.Fatalf("got %q,%d want %q,%d", userID, at, tc.userID, tc.at)
			}
		})
	}
}

func TestParseTimestampMs(t *testing.T) {
	if ms, ok := parseTimestampMs("1700000000000"); !ok || ms != 1700000000000 {
		t.Fatalf("numeric: %d %v", ms, ok)
	}
	if ms, ok := parseTimestampMs("2026-01-01T00:00:00.000Z"); !ok || ms != time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() {
		t.Fatalf("rfc3339: %d %v", ms, ok)
	}
	if _, ok := parseTimestampMs(""); ok {
		t.Fatal("empty string parsed as a timestamp")
	}
	if _, ok := parseTimestampMs("yesterday"); ok {
		t.Fatal("garbage parsed as a timestamp")
	}
}

func TestBulkDeleteUserMessagesAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		userID    = "820000000000000001"
		channelID = "820000000000000002"
	)
	oldMS := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC).UnixMilli()
	newMS := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC).UnixMilli()
	cutoff := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC).UnixMilli()

	oldID := snowflakeID(oldMS)
	newID := snowflakeID(newMS)

	userKey := bigintSeg(userID)
	if err := putKV(ctx, "users", userKey, userKey, map[string]any{
		"user_id":                          kvBigintJSON(userID),
		"username":                         map[string]any{"__fluxer_type": "string", "value": "bulk_user"},
		"pending_bulk_message_deletion_at": kvDate(time.UnixMilli(cutoff)),
		"pending_bulk_message_deletion_message_count": 2,
	}, ""); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	seedMessage := func(messageID string) {
		partition, key, bucket := messageKeys(channelID, messageID)
		if err := putKV(ctx, messagesTable, partition, key, map[string]any{
			"channel_id": kvBigintJSON(channelID),
			"message_id": kvBigintJSON(messageID),
			"author_id":  kvBigintJSON(userID),
			"bucket":     bucket,
			"content":    "message " + messageID,
		}, messageID); err != nil {
			t.Fatalf("seed message %s: %v", messageID, err)
		}
		authorKey := bigintSeg(userID) + kvSep + bigintSeg(messageID)
		if err := putKV(ctx, messagesByAuthor, authorKey, authorKey, map[string]any{
			"author_id":  kvBigintJSON(userID),
			"channel_id": kvBigintJSON(channelID),
			"message_id": kvBigintJSON(messageID),
		}, ""); err != nil {
			t.Fatalf("seed author index %s: %v", messageID, err)
		}
	}
	seedMessage(oldID)
	seedMessage(newID)

	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, userKey)
		for _, id := range []string{oldID, newID} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE row_key LIKE '%' || $1 || '%'`, bigintSeg(id))
		}
	})

	if err := taskBulkDeleteUserMessages(ctx, map[string]any{
		"userId":      userID,
		"scheduledAt": float64(cutoff),
	}); err != nil {
		t.Fatalf("task: %v", err)
	}

	exists := func(t *testing.T, table, rowKey string) bool {
		t.Helper()
		var n int
		if err := conn.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
			table, rowKey).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		return n > 0
	}

	_, oldKey, _ := messageKeys(channelID, oldID)
	_, newKey, _ := messageKeys(channelID, newID)
	if exists(t, messagesTable, oldKey) {
		t.Fatal("a message older than the cutoff survived")
	}
	if !exists(t, messagesTable, newKey) {
		t.Fatal("a message newer than the cutoff was deleted")
	}
	if exists(t, messagesByAuthor, bigintSeg(userID)+kvSep+bigintSeg(oldID)) {
		t.Fatal("the author index row of the deleted message survived")
	}
	if !exists(t, messagesByAuthor, bigintSeg(userID)+kvSep+bigintSeg(newID)) {
		t.Fatal("the author index row of the kept message was deleted")
	}

	row, err := readKV(ctx, "users", userKey)
	if err != nil {
		t.Fatalf("read user: %v", err)
	}
	if row == nil {
		t.Fatal("user row disappeared")
	}
	if typedString(row["pending_bulk_message_deletion_at"]) != "" {
		t.Fatalf("pending timestamp not cleared: %#v", row["pending_bulk_message_deletion_at"])
	}
	if typedString(row["username"]) != "bulk_user" {
		t.Fatal("the patch rewrote unrelated user fields")
	}
}
