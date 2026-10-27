package main

// Tests for the thread index repair.
//
// The one thing that must not go wrong: an archived thread loses its "active"
// row, because a guild that still lists an archived thread as active shows it in
// the wrong place until the next lifecycle change.

import (
	"context"
	"testing"
)

func TestRepairThreadIndexesAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		guildID       = "990000000000000001"
		parentID      = "990000000000000002"
		activeThread  = "990000000000000003"
		archivedThred = "990000000000000004"
	)
	seedState := func(threadID string, archived bool, archiveTS any) {
		key := bigintSeg(threadID)
		row := map[string]any{
			"thread_id":  kvBigintJSON(threadID),
			"guild_id":   kvBigintJSON(guildID),
			"parent_id":  kvBigintJSON(parentID),
			"type":       11,
			"archived":   archived,
			"created_at": kvDate(kvNow()),
		}
		if archiveTS != nil {
			row["archive_timestamp"] = archiveTS
		}
		if err := putKV(ctx, threadStateTable, key, key, row, ""); err != nil {
			t.Fatalf("seed thread %s: %v", threadID, err)
		}
	}
	seedState(activeThread, false, nil)
	seedState(archivedThred, true, kvDate(kvNow().Add(-24*60*60*1e9)))
	// A stale active row for the archived thread: the repair must remove it.
	staleKey := bigintSeg(guildID) + kvSep + bigintSeg(archivedThred)
	if err := putKV(ctx, activeThreadsTable, staleKey, staleKey, map[string]any{
		"guild_id": kvBigintJSON(guildID), "thread_id": kvBigintJSON(archivedThred),
	}, ""); err != nil {
		t.Fatalf("seed stale active row: %v", err)
	}
	t.Cleanup(func() {
		for _, table := range []string{threadStateTable, activeThreadsTable, threadsByParentTable, archivedThreadsTable} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	if err := taskRepairThreadIndexes(ctx, map[string]any{"threadIds": []any{activeThread, archivedThred}}); err != nil {
		t.Fatalf("task: %v", err)
	}

	activeKey := bigintSeg(guildID) + kvSep + bigintSeg(activeThread)
	if row, _ := readKV(ctx, activeThreadsTable, activeKey); row == nil {
		t.Fatal("the active thread has no active row")
	}
	if row, _ := readKV(ctx, activeThreadsTable, staleKey); row != nil {
		t.Fatal("the archived thread still has an active row")
	}
	parentKey := bigintSeg(parentID) + kvSep + bigintSeg(activeThread)
	if row, _ := readKV(ctx, threadsByParentTable, parentKey); row == nil {
		t.Fatal("the parent index row is missing")
	}
	parentKeyArchived := bigintSeg(parentID) + kvSep + bigintSeg(archivedThred)
	if row, _ := readKV(ctx, threadsByParentTable, parentKeyArchived); row == nil {
		t.Fatal("the parent index row for the archived thread is missing")
	}

	// A thread id with no state row is skipped, not an error.
	if err := taskRepairThreadIndexes(ctx, map[string]any{"threadIds": []any{"990000000000000099"}}); err != nil {
		t.Fatalf("unknown thread: %v", err)
	}
}
