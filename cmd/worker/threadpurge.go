package main

// taskDeleteChannelThreads ports DeleteChannelThreads.ts.
//
// When a channel is deleted, its threads have to go with it: each thread is a
// channel of its own (messages, attachment lookups, members) plus five index rows
// (active by guild, archived by parent, by parent, stats, state) plus an entry in
// the auto-archive queue. Doing this lazily would leave orphan channels that the
// guild keeps listing.
//
// Not ported: search documents (no backend) and the auto-archive bookkeeping that
// decides *when* a thread is due. The queue entry itself is removed here — a
// deleted thread left in `thread_archive:<guild>` would be picked up by the
// archive sweep and resurrected as an archived thread.

import (
	"context"
	"fmt"
	"log"
)

const threadArchiveQueuePrefix = "thread_archive:"

func taskDeleteChannelThreads(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("deleteChannelThreads: %w", err)
		}
	}
	guildID := stringField(payload, "guildId")
	parentID := stringField(payload, "parentId")
	if !isDigits(guildID) || !isDigits(parentID) {
		return fmt.Errorf("deleteChannelThreads: guildId and parentId are required")
	}

	threadIDs, err := threadIDsByParent(ctx, parentID)
	if err != nil {
		return fmt.Errorf("deleteChannelThreads: list threads: %w", err)
	}
	deleted, failed := 0, 0
	for _, threadID := range threadIDs {
		if err := purgeThread(ctx, guildID, parentID, threadID); err != nil {
			failed++
			log.Printf("deleteChannelThreads: thread %s: %v", threadID, err)
			continue
		}
		deleted++
	}
	log.Printf("deleteChannelThreads: guild=%s parent=%s threads=%d purged=%d failed=%d",
		guildID, parentID, len(threadIDs), deleted, failed)
	if failed > 0 {
		return fmt.Errorf("deleteChannelThreads: %d threads could not be purged", failed)
	}
	return nil
}

// threadIDsByParent reads threads_by_parent, whose row key is
// "<parent>\x1f<thread>".
func threadIDsByParent(ctx context.Context, parentID string) ([]string, error) {
	prefix := bigintSeg(parentID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'thread_id'->>'value' FROM fluxer_kv
WHERE table_name = $1 AND left(row_key, length($2)) = $2`, threadsByParentTable, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var threadID *string
		if err := rows.Scan(&threadID); err != nil {
			return nil, err
		}
		if threadID != nil && isDigits(*threadID) {
			out = append(out, *threadID)
		}
	}
	return out, rows.Err()
}

// purgeThread removes one thread: messages first (they are the bulk), then
// members, then the indexes, then the channel row and the state.
func purgeThread(ctx context.Context, guildID, parentID, threadID string) error {
	state, err := readThreadState(ctx, threadID)
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	if state != nil {
		if state.ParentID != "" {
			parentID = state.ParentID
		}
		if state.GuildID != "" {
			guildID = state.GuildID
		}
	}

	if err := deleteAllMessagesInChannel(ctx, threadID); err != nil {
		return fmt.Errorf("messages: %w", err)
	}
	if err := purgeThreadMembers(ctx, threadID); err != nil {
		return fmt.Errorf("members: %w", err)
	}

	// Indexes and bookkeeping.
	deletes := []struct{ table, key string }{
		{activeThreadsTable, bigintSeg(guildID) + kvSep + bigintSeg(threadID)},
		{threadsByParentTable, bigintSeg(parentID) + kvSep + bigintSeg(threadID)},
		{"thread_stats", bigintSeg(threadID)},
		{"thread_state", bigintSeg(threadID)},
		{"channels", bigintSeg(threadID) + kvSep + boolSeg(false)},
	}
	if state != nil && state.Archived && state.ArchiveTS != "" {
		deletes = append(deletes, struct{ table, key string }{
			archivedThreadsTable,
			bigintSeg(parentID) + kvSep + boolSeg(state.IsPrivate) + kvSep + stringSeg(state.ArchiveTS) + kvSep + bigintSeg(threadID),
		})
	}
	for _, d := range deletes {
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, d.table, d.key); err != nil {
			return fmt.Errorf("delete %s: %w", d.table, err)
		}
	}
	// The forum pin, if it pointed at this thread, goes with it.
	_, _ = db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'forum_pinned_thread' AND row_key = $1 AND row_data->'thread_id'->>'value' = $2`,
		bigintSeg(parentID), threadID)
	// The auto-archive queue entry, so a deleted thread is not picked up later.
	if err := valkey().ZRem(ctx, threadArchiveQueuePrefix+guildID, threadID).Err(); err != nil {
		log.Printf("deleteChannelThreads: thread %s: archive queue: %v (rows removed)", threadID, err)
	}
	return nil
}

// deleteAllMessagesInChannel walks a channel's messages and deletes each through
// the shared bundle. Threads are small; a bucket scan is enough here.
func deleteAllMessagesInChannel(ctx context.Context, channelID string) error {
	prefix := bigintSeg(channelID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'message_id'->>'value' FROM fluxer_kv
WHERE table_name = $1 AND left(partition_key, length($2)) = $2`, messagesTable, prefix)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id *string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if id != nil && isDigits(*id) {
			ids = append(ids, *id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := deleteMessageRow(ctx, channelID, id); err != nil {
			return fmt.Errorf("message %s: %w", id, err)
		}
	}
	return nil
}

// purgeThreadMembers removes the membership rows in both directions.
func purgeThreadMembers(ctx context.Context, threadID string) error {
	prefix := bigintSeg(threadID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'user_id'->>'value' FROM fluxer_kv
WHERE table_name = 'thread_members' AND left(row_key, length($1)) = $1`, prefix)
	if err != nil {
		return err
	}
	var userIDs []string
	for rows.Next() {
		var userID *string
		if err := rows.Scan(&userID); err != nil {
			rows.Close()
			return err
		}
		if userID != nil && isDigits(*userID) {
			userIDs = append(userIDs, *userID)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'thread_members' AND left(row_key, length($1)) = $1`,
		prefix); err != nil {
		return err
	}
	// thread_members_by_user is keyed (user, guild, parent, is_private, thread), so
	// the thread segment is the last one.
	for _, userID := range userIDs {
		if _, err := db.ExecContext(ctx, `
DELETE FROM fluxer_kv
WHERE table_name = 'thread_members_by_user'
  AND left(row_key, length($1)) = $1
  AND right(row_key, length($2)) = $2`,
			bigintSeg(userID)+kvSep, kvSep+bigintSeg(threadID)); err != nil {
			return err
		}
	}
	return nil
}
