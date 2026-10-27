package main

// taskArchiveInactiveThreads ports ArchiveInactiveThreads.ts.
//
// A thread archives itself once it has been quiet for its `auto_archive_duration`
// (minutes). The api keeps a sorted set per guild, `thread_archive:<guild>`,
// scored by that due time, plus a set of guilds that have anything queued. This
// sweep walks the due threads, recomputes the deadline from the thread's own state
// and its channel's last message, and flips `archived` when the deadline has
// passed.
//
// The queue is a *hint*: the due time is recomputed from state and channel, so a
// stale score cannot archive a thread that spoke recently, and a thread that moved
// is re-scored instead of archived.
//
// Not ported: the THREAD_UPDATE dispatch, which needs the thread view/channel
// response mapper this worker does not have. Clients see the archived state on
// their next thread list fetch; the row itself is correct immediately.

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	threadArchiveGuildsKey = "thread_archive_guilds"
	threadArchivePerTick   = 200
	threadArchiveQueuePfx  = threadArchiveQueuePrefix
)

func taskArchiveInactiveThreads(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("archiveInactiveThreads: %w", err)
		}
	}
	guilds, err := valkey().SMembers(ctx, threadArchiveGuildsKey).Result()
	if err != nil {
		return fmt.Errorf("archiveInactiveThreads: list guilds: %w", err)
	}
	now := time.Now().UTC()
	budget := threadArchivePerTick
	archivedTotal := 0
	for _, guildID := range guilds {
		if budget <= 0 {
			break
		}
		if !isDigits(guildID) {
			_ = valkey().SRem(ctx, threadArchiveGuildsKey, guildID).Err()
			continue
		}
		archived, err := archiveDueThreads(ctx, guildID, now, budget)
		if err != nil {
			log.Printf("archiveInactiveThreads: guild %s: %v", guildID, err)
			continue
		}
		budget -= archived
		archivedTotal += archived
	}
	if archivedTotal > 0 {
		log.Printf("archiveInactiveThreads: archived=%d", archivedTotal)
	}
	return nil
}

// archiveDueThreads handles one guild's queue and returns how many threads it
// archived (the sweep's budget is spent on archives, not on re-scores).
func archiveDueThreads(ctx context.Context, guildID string, now time.Time, limit int) (int, error) {
	key := threadArchiveQueuePfx + guildID
	due, err := valkey().ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", now.UnixMilli()), Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return 0, err
	}
	if len(due) == 0 {
		return 0, nil
	}
	archived := 0
	for _, threadID := range due {
		if !isDigits(threadID) {
			_ = valkey().ZRem(ctx, key, threadID).Err()
			continue
		}
		state, err := readThreadState(ctx, threadID)
		if err != nil {
			return archived, fmt.Errorf("thread %s: read state: %w", threadID, err)
		}
		channel, err := readChannel(ctx, threadID)
		if err != nil {
			return archived, fmt.Errorf("thread %s: read channel: %w", threadID, err)
		}
		if state == nil || channel == nil || state.Archived {
			if err := removeFromArchiveQueue(ctx, guildID, threadID); err != nil {
				return archived, err
			}
			continue
		}
		pinned, err := isForumPinned(ctx, state.ParentID, threadID)
		if err != nil {
			return archived, err
		}
		if pinned {
			if err := removeFromArchiveQueue(ctx, guildID, threadID); err != nil {
				return archived, err
			}
			continue
		}
		dueAt, err := threadAutoArchiveDueAt(state, typedString(channel["last_message_id"]))
		if err != nil {
			return archived, fmt.Errorf("thread %s: due: %w", threadID, err)
		}
		if dueAt > now.UnixMilli() {
			if err := valkey().ZAdd(ctx, key, redis.Z{Score: float64(dueAt), Member: threadID}).Err(); err != nil {
				return archived, err
			}
			continue
		}
		if err := archiveOneThread(ctx, *state, now); err != nil {
			return archived, fmt.Errorf("thread %s: archive: %w", threadID, err)
		}
		if err := removeFromArchiveQueue(ctx, guildID, threadID); err != nil {
			return archived, err
		}
		archived++
	}
	return archived, nil
}

// threadAutoArchiveDueAt is the api's rule: the later of the thread's last message
// and its creation/archival, plus its auto-archive window.
func threadAutoArchiveDueAt(state *threadState, lastMessageID string) (int64, error) {
	var lastActivity int64
	if lastMessageID != "" {
		id, err := parseInt64(lastMessageID)
		if err == nil {
			lastActivity = snowflakeTime(id).UnixMilli()
		}
	}
	anchor := int64(0)
	if state.ArchiveTS != "" {
		if ms, ok := parseTimestampMs(state.ArchiveTS); ok {
			anchor = ms
		}
	}
	if anchor == 0 && state.CreatedAt != "" {
		if ms, ok := parseTimestampMs(state.CreatedAt); ok {
			anchor = ms
		}
	}
	if lastActivity > anchor {
		anchor = lastActivity
	}
	return anchor + int64(state.AutoArchiveMins)*60*1000, nil
}

// archiveOneThread flips the state, moves the indexes and stamps the archive time.
func archiveOneThread(ctx context.Context, state threadState, now time.Time) error {
	stamp := now.Format("2006-01-02T15:04:05.000Z")
	for field, value := range map[string]any{
		"archived":          true,
		"archive_timestamp": kvDate(now),
	} {
		if err := patchRowField(ctx, threadStateTable, bigintSeg(state.ThreadID), field, value); err != nil {
			return err
		}
	}
	state.Archived = true
	state.ArchiveTS = stamp
	return writeThreadIndexes(ctx, state)
}

func isForumPinned(ctx context.Context, parentID, threadID string) (bool, error) {
	if parentID == "" {
		return false, nil
	}
	row, err := readKV(ctx, "forum_pinned_thread", bigintSeg(parentID))
	if err != nil || row == nil {
		return false, err
	}
	return typedString(row["thread_id"]) == threadID, nil
}

// removeFromArchiveQueue drops the member and, when the guild's queue empties,
// the guild from the set the sweep walks.
func removeFromArchiveQueue(ctx context.Context, guildID, threadID string) error {
	key := threadArchiveQueuePfx + guildID
	if err := valkey().ZRem(ctx, key, threadID).Err(); err != nil {
		return err
	}
	remaining, err := valkey().ZCard(ctx, key).Result()
	if err != nil {
		return err
	}
	if remaining == 0 {
		return valkey().SRem(ctx, threadArchiveGuildsKey, guildID).Err()
	}
	return nil
}
