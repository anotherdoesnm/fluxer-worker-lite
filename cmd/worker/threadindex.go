package main

// taskRepairThreadIndexes ports RepairThreadIndexes.ts.
//
// Thread indexes are denormalised twice: `active_threads_by_guild` lists the
// threads a guild shows as active, `threads_by_parent` lists the threads under a
// channel, and an archived thread additionally gets a row in
// `archived_threads_by_parent`. They are written on every thread lifecycle change,
// and this task is the repair path for when a write was lost: it re-derives all
// three rows from the thread's own state row, which is the source of truth.
//
// It never dispatches anything. The api's caller reindexes, and clients learn about
// it from the next thread list fetch; inventing a gateway event here would mean
// inventing a thread payload this worker does not build.
//
// Known encoding risk: for an archived thread the row key contains the archive
// timestamp. The api's segment encoding for that column is not derivable from the
// bundle, so the ISO string form is used here and an archived row written by this
// task may not match the one the api would write. Active threads — the case this
// instance has — are unaffected.

import (
	"context"
	"fmt"
	"log"
)

const (
	threadStateTable         = "thread_state"
	activeThreadsTable       = "active_threads_by_guild"
	threadsByParentTable     = "threads_by_parent"
	archivedThreadsTable     = "archived_threads_by_parent"
	channelTypePrivateThread = 12
)

type threadState struct {
	ThreadID  string
	GuildID   string
	ParentID  string
	Type      int
	Archived  bool
	ArchiveTS string
	IsPrivate bool
}

func taskRepairThreadIndexes(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("repairThreadIndexes: %w", err)
		}
	}
	threadIDs := stringSliceField(payload, "threadIds")
	if len(threadIDs) == 0 {
		return fmt.Errorf("repairThreadIndexes: threadIds is empty")
	}
	repaired, skipped, failed := 0, 0, 0
	for _, threadID := range threadIDs {
		if !isDigits(threadID) {
			failed++
			log.Printf("repairThreadIndexes: skipping malformed thread id %q", threadID)
			continue
		}
		state, err := readThreadState(ctx, threadID)
		if err != nil {
			failed++
			log.Printf("repairThreadIndexes: %s: %v", threadID, err)
			continue
		}
		if state == nil {
			skipped++
			continue
		}
		if err := writeThreadIndexes(ctx, *state); err != nil {
			failed++
			log.Printf("repairThreadIndexes: %s: %v", threadID, err)
			continue
		}
		repaired++
	}
	log.Printf("repairThreadIndexes: requested=%d repaired=%d skipped=%d failed=%d",
		len(threadIDs), repaired, skipped, failed)
	if failed > 0 {
		return fmt.Errorf("repairThreadIndexes: %d threads could not be repaired", failed)
	}
	return nil
}

func readThreadState(ctx context.Context, threadID string) (*threadState, error) {
	raw, err := readKV(ctx, threadStateTable, bigintSeg(threadID))
	if err != nil || raw == nil {
		return nil, err
	}
	state := &threadState{
		ThreadID:  threadID,
		GuildID:   typedString(raw["guild_id"]),
		ParentID:  typedString(raw["parent_id"]),
		ArchiveTS: typedString(raw["archive_timestamp"]),
	}
	state.Type = intOrZero(raw["type"])
	state.IsPrivate = state.Type == channelTypePrivateThread
	if archived, ok := raw["archived"].(bool); ok {
		state.Archived = archived
	}
	return state, nil
}

// writeThreadIndexes re-derives the three index rows for one thread.
func writeThreadIndexes(ctx context.Context, state threadState) error {
	activeKey := bigintSeg(state.GuildID) + kvSep + bigintSeg(state.ThreadID)
	parentKey := bigintSeg(state.ParentID) + kvSep + bigintSeg(state.ThreadID)

	if state.Archived {
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
			activeThreadsTable, activeKey); err != nil {
			return fmt.Errorf("drop active row: %w", err)
		}
		if state.ArchiveTS != "" {
			archivedKey := bigintSeg(state.ParentID) + kvSep + boolSeg(state.IsPrivate) + kvSep +
				stringSeg(state.ArchiveTS) + kvSep + bigintSeg(state.ThreadID)
			if err := putKV(ctx, archivedThreadsTable, archivedKey, archivedKey, map[string]any{
				"parent_id":         bigintValue(state.ParentID),
				"is_private":        state.IsPrivate,
				"archive_timestamp": state.ArchiveTS,
				"thread_id":         bigintValue(state.ThreadID),
				"guild_id":          bigintValue(state.GuildID),
			}, ""); err != nil {
				return fmt.Errorf("archived row: %w", err)
			}
		}
	} else {
		if err := putKV(ctx, activeThreadsTable, activeKey, activeKey, map[string]any{
			"guild_id":  bigintValue(state.GuildID),
			"thread_id": bigintValue(state.ThreadID),
			"parent_id": bigintValue(state.ParentID),
			"type":      state.Type,
		}, ""); err != nil {
			return fmt.Errorf("active row: %w", err)
		}
	}
	return putKV(ctx, threadsByParentTable, parentKey, parentKey, map[string]any{
		"parent_id": bigintValue(state.ParentID),
		"thread_id": bigintValue(state.ThreadID),
		"guild_id":  bigintValue(state.GuildID),
		"type":      state.Type,
	}, "")
}
