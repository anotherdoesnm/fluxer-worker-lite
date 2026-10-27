package main

// Tests for the thread auto-archive sweep.
//
// The due time is the interesting part: a thread that spoke recently must not be
// archived even if the queue says it is due, and a thread whose deadline moved
// must be re-scored rather than archived. The test drives the pure function and
// the state transition, not the Redis queue (no valkey here).

import (
	"context"
	"testing"
	"time"
)

func TestThreadAutoArchiveDueAt(t *testing.T) {
	now := time.Now().UTC()
	created := now.Add(-72 * time.Hour)
	recentMessage := snowflakeID(now.Add(-10 * time.Minute).UnixMilli())
	oldMessage := snowflakeID(now.Add(-72 * time.Hour).UnixMilli())

	state := &threadState{AutoArchiveMins: 1440, CreatedAt: kvDateString(created)} // 24h window
	due, err := threadAutoArchiveDueAt(state, recentMessage)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	// Last activity (10 minutes ago) + 24h is in the future.
	if due <= now.UnixMilli() {
		t.Fatalf("due %v is not in the future", time.UnixMilli(due))
	}

	// With only an old message, the deadline has passed.
	due, err = threadAutoArchiveDueAt(state, oldMessage)
	if err != nil {
		t.Fatalf("due: %v", err)
	}
	if due > now.UnixMilli() {
		t.Fatalf("due %v is unexpectedly in the future", time.UnixMilli(due))
	}
}

func TestArchiveOneThreadAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		guildID  = "990000000000002001"
		parentID = "990000000000002002"
		threadID = "990000000000002003"
	)
	stateKey := bigintSeg(threadID)
	if err := putKV(ctx, threadStateTable, stateKey, stateKey, map[string]any{
		"thread_id":             kvBigintJSON(threadID),
		"guild_id":              kvBigintJSON(guildID),
		"parent_id":             kvBigintJSON(parentID),
		"type":                  11,
		"archived":              false,
		"auto_archive_duration": 1440,
		"created_at":            kvDate(kvNow().Add(-72 * time.Hour)),
	}, ""); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	activeKey := bigintSeg(guildID) + kvSep + bigintSeg(threadID)
	if err := putKV(ctx, activeThreadsTable, activeKey, activeKey, map[string]any{
		"guild_id": kvBigintJSON(guildID), "thread_id": kvBigintJSON(threadID),
	}, ""); err != nil {
		t.Fatalf("seed active: %v", err)
	}
	t.Cleanup(func() {
		for _, table := range []string{threadStateTable, activeThreadsTable, threadsByParentTable, archivedThreadsTable} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	state, err := readThreadState(ctx, threadID)
	if err != nil || state == nil {
		t.Fatalf("read state: %v", err)
	}
	now := time.Now().UTC()
	if err := archiveOneThread(ctx, *state, now); err != nil {
		t.Fatalf("archive: %v", err)
	}

	row, _ := readKV(ctx, threadStateTable, stateKey)
	if archived, _ := row["archived"].(bool); !archived {
		t.Fatal("the state was not marked archived")
	}
	if typedString(row["archive_timestamp"]) == "" {
		t.Fatal("the archive timestamp was not written")
	}
	if row, _ := readKV(ctx, activeThreadsTable, activeKey); row != nil {
		t.Fatal("the active row survived archiving")
	}
	if row, _ := readKV(ctx, threadsByParentTable, bigintSeg(parentID)+kvSep+bigintSeg(threadID)); row == nil {
		t.Fatal("the parent index row disappeared")
	}
}

// kvDateString renders a date the way kvDate does, for the pure test.
func kvDateString(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
