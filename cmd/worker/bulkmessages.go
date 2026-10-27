package main

// Pending bulk message deletion: the scheduler and the worker half.
//
// A person can ask the instance to delete their own messages ("bulk message
// deletion"). The api does not delete anything then: it stamps the user row with
// pending_bulk_message_deletion_at and puts a member into the valkey sorted set
// bulk_message_deletion_queue. Two tasks finish the job:
//
//	processPendingBulkMessageDeletions  cron; picks due members and enqueues
//	bulkDeleteUserMessages              deletes the messages, clears the fields
//
// Both halves exist here. The queue shape is the same one userProcessPendingDeletions
// already uses, with different keys: a member "<userId>|<scheduledAt>", a companion
// string `bulk_message_deletion_queue:<userId>`, a state-version marker and a
// rebuild lock. When the marker is missing or a day old the set is rebuilt from
// the users table, because valkey is a cache and the rows are the truth.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	bulkQueueKey      = "bulk_message_deletion_queue"
	bulkQueueStateKey = "bulk_message_deletion_queue:state_version"
	bulkQueueLockKey  = "bulk_message_deletion_queue:rebuild_lock"
	bulkQueueByUser   = "bulk_message_deletion_queue:"

	bulkQueueBatch    = 100
	bulkQueueStateAge = 24 * time.Hour
	bulkQueueLockTTL  = 300 * time.Second
	bulkQueuePageSize = 500
	bulkDeleteBatch   = 100
	bulkScanPageSize  = 500
)

// bulkQueueMember renders the sorted-set member. scheduledAt is epoch
// milliseconds, the same unit as the score, so a re-queue does not have to parse
// a date format.
func bulkQueueMember(userID string, scheduledAt int64) string {
	return userID + "|" + strconv.FormatInt(scheduledAt, 10)
}

func parseBulkQueueMember(member string) (userID string, scheduledAt int64, err error) {
	idx := strings.LastIndex(member, "|")
	if idx <= 0 || idx == len(member)-1 {
		return "", 0, fmt.Errorf("unreadable bulk member %q", member)
	}
	userID = member[:idx]
	scheduledAt, err = strconv.ParseInt(member[idx+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("unreadable bulk member %q: %w", member, err)
	}
	if !isDigits(userID) {
		return "", 0, fmt.Errorf("unreadable bulk member %q: user id is not numeric", member)
	}
	return userID, scheduledAt, nil
}

func scheduleBulkDeletion(ctx context.Context, userID string, at time.Time) error {
	member := bulkQueueMember(userID, at.UnixMilli())
	if err := valkey().ZAdd(ctx, bulkQueueKey, redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: member,
	}).Err(); err != nil {
		return err
	}
	return valkey().Set(ctx, bulkQueueByUser+userID, member, 0).Err()
}

func removeBulkDeletion(ctx context.Context, userID string) error {
	member, err := valkey().Get(ctx, bulkQueueByUser+userID).Result()
	if err == redis.Nil {
		// No companion key (a rebuild may have been interrupted): find the member
		// by its user prefix. Removing the whole set here would be a data loss
		// bug waiting for the first missing key.
		members, zerr := valkey().ZRange(ctx, bulkQueueKey, 0, -1).Result()
		if zerr != nil {
			return zerr
		}
		prefix := userID + "|"
		for _, m := range members {
			if strings.HasPrefix(m, prefix) {
				if err := valkey().ZRem(ctx, bulkQueueKey, m).Err(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err != nil {
		return err
	}
	if err := valkey().ZRem(ctx, bulkQueueKey, member).Err(); err != nil {
		return err
	}
	return valkey().Del(ctx, bulkQueueByUser+userID).Err()
}

// bulkQueueStale reports whether the set has to be rebuilt from the users table.
func bulkQueueStale(ctx context.Context) (bool, error) {
	raw, err := valkey().Get(ctx, bulkQueueStateKey).Result()
	if err == redis.Nil {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return true, nil
	}
	return time.Since(time.UnixMilli(ms)) > bulkQueueStateAge, nil
}

// rebuildBulkQueue repopulates the set from every user that carries a pending
// bulk deletion timestamp.
func rebuildBulkQueue(ctx context.Context) (queued int, err error) {
	if err := valkey().Del(ctx, bulkQueueKey, bulkQueueStateKey).Err(); err != nil {
		return 0, err
	}
	cursor := ""
	for {
		page, last, err := pendingBulkUsersPage(ctx, cursor, bulkQueuePageSize)
		if err != nil {
			return queued, err
		}
		for _, user := range page {
			at, ok := parseTimestampMs(user.PendingAt)
			if !ok {
				log.Printf("processPendingBulkMessageDeletions: user %s has an unreadable pending timestamp %q (skipped)",
					user.UserID, user.PendingAt)
				continue
			}
			if err := scheduleBulkDeletion(ctx, user.UserID, time.UnixMilli(at).UTC()); err != nil {
				return queued, err
			}
			queued++
		}
		if len(page) < bulkQueuePageSize {
			break
		}
		cursor = last
	}
	if err := valkey().Set(ctx, bulkQueueStateKey,
		strconv.FormatInt(time.Now().UnixMilli(), 10), 0).Err(); err != nil {
		return queued, err
	}
	return queued, nil
}

func ensureBulkQueue(ctx context.Context) (bool, error) {
	stale, err := bulkQueueStale(ctx)
	if err != nil || !stale {
		return err == nil, err
	}
	token, err := randomToken()
	if err != nil {
		return false, fmt.Errorf("rebuild lock token: %w", err)
	}
	ok, err := valkey().SetNX(ctx, bulkQueueLockKey, token, bulkQueueLockTTL).Result()
	if err != nil {
		return false, err
	}
	if !ok {
		log.Printf("processPendingBulkMessageDeletions: another worker is rebuilding the bulk queue")
		return false, nil
	}
	defer func() {
		if err := valkey().Del(ctx, bulkQueueLockKey).Err(); err != nil {
			log.Printf("processPendingBulkMessageDeletions: release rebuild lock: %v", err)
		}
	}()
	queued, err := rebuildBulkQueue(ctx)
	if err != nil {
		return false, fmt.Errorf("rebuild bulk queue: %w", err)
	}
	log.Printf("processPendingBulkMessageDeletions: rebuilt queue with %d members", queued)
	return true, nil
}

type pendingBulkUser struct {
	UserID    string
	PendingAt string
}

// pendingBulkUsersPage walks the users table in row_key order.
func pendingBulkUsersPage(ctx context.Context, cursor string, limit int) ([]pendingBulkUser, string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_key, COALESCE(row_data->'pending_bulk_message_deletion_at'->>'value', '')
FROM fluxer_kv
WHERE table_name = 'users'
  AND row_data->'pending_bulk_message_deletion_at'->>'value' IS NOT NULL
  AND ($1 = '' OR row_key > $1)
ORDER BY row_key
LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []pendingBulkUser
	for rows.Next() {
		var rowKey, pendingAt string
		if err := rows.Scan(&rowKey, &pendingAt); err != nil {
			return nil, "", err
		}
		userID, ok := parseBigintRowKey(rowKey)
		if !ok {
			continue
		}
		out = append(out, pendingBulkUser{UserID: userID, PendingAt: pendingAt})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(out) == 0 {
		return nil, "", nil
	}
	return out, rowsKeyOf(out), nil
}

func rowsKeyOf(page []pendingBulkUser) string {
	last := page[len(page)-1].UserID
	return bigintSeg(last)
}

// parseTimestampMs accepts the two shapes the api writes for dates: the typed
// wrapper value (RFC3339) and a bare number of milliseconds.
func parseTimestampMs(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return ms, true
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UnixMilli(), true
	}
	return 0, false
}

// taskProcessPendingBulkMessageDeletions is the cron half: due members become
// bulkDeleteUserMessages jobs.
func taskProcessPendingBulkMessageDeletions(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("processPendingBulkMessageDeletions: %w", err)
		}
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return fmt.Errorf("processPendingBulkMessageDeletions: jetstream: %w", err)
	}
	ready, err := ensureBulkQueue(ctx)
	if err != nil {
		return fmt.Errorf("processPendingBulkMessageDeletions: %w", err)
	}
	if !ready {
		return nil
	}
	now := time.Now().UTC()
	members, err := valkey().ZRangeByScore(ctx, bulkQueueKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Offset: 0, Count: bulkQueueBatch,
	}).Result()
	if err != nil {
		return fmt.Errorf("processPendingBulkMessageDeletions: read queue: %w", err)
	}
	var scheduled, postponed, dropped, bad int
	for _, member := range members {
		userID, scheduledAt, parseErr := parseBulkQueueMember(member)
		if parseErr != nil {
			bad++
			log.Printf("processPendingBulkMessageDeletions: dropping unreadable member %q: %v", member, parseErr)
			_ = valkey().ZRem(ctx, bulkQueueKey, member).Err()
			continue
		}
		row, err := readKV(ctx, "users", bigintSeg(userID))
		if err != nil {
			log.Printf("processPendingBulkMessageDeletions: %s: read user: %v", userID, err)
			continue
		}
		if row == nil {
			dropped++
			_ = removeBulkDeletion(ctx, userID)
			continue
		}
		pendingAt, ok := parseTimestampMs(typedString(row["pending_bulk_message_deletion_at"]))
		if !ok {
			dropped++
			_ = removeBulkDeletion(ctx, userID)
			continue
		}
		if pendingAt > now.UnixMilli() {
			postponed++
			if err := scheduleBulkDeletion(ctx, userID, time.UnixMilli(pendingAt).UTC()); err != nil {
				log.Printf("processPendingBulkMessageDeletions: %s: postpone: %v", userID, err)
			}
			continue
		}
		payload := map[string]any{"userId": userID, "scheduledAt": scheduledAt}
		if err := publishJob(js, jobPrefixForPublish(), "bulkDeleteUserMessages", payload, time.Time{}); err != nil {
			log.Printf("processPendingBulkMessageDeletions: %s: enqueue: %v", userID, err)
			continue
		}
		scheduled++
		if err := removeBulkDeletion(ctx, userID); err != nil {
			log.Printf("processPendingBulkMessageDeletions: %s: dequeue after enqueue: %v", userID, err)
		}
	}
	log.Printf("processPendingBulkMessageDeletions: ready=%d scheduled=%d postponed=%d dropped=%d unreadable=%d",
		len(members), scheduled, postponed, dropped, bad)
	return nil
}

// taskBulkDeleteUserMessages is the worker half: delete the person's messages up
// to the moment they asked for, then clear the three pending fields so the same
// request cannot run twice.
func taskBulkDeleteUserMessages(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("bulkDeleteUserMessages: %w", err)
		}
	}
	userID := stringField(payload, "userId")
	if !isDigits(userID) {
		return fmt.Errorf("bulkDeleteUserMessages: userId is required")
	}
	rowKey := bigintSeg(userID)
	row, err := readKV(ctx, "users", rowKey)
	if err != nil {
		return fmt.Errorf("bulkDeleteUserMessages: read user %s: %w", userID, err)
	}
	if row == nil {
		return fmt.Errorf("bulkDeleteUserMessages: user %s does not exist", userID)
	}
	pendingAt, ok := parseTimestampMs(typedString(row["pending_bulk_message_deletion_at"]))
	if !ok {
		log.Printf("bulkDeleteUserMessages: %s has no pending bulk deletion, nothing to do", userID)
		return nil
	}
	// scheduledAt from the payload wins: it is the moment the person asked for,
	// and the row may have been re-scheduled since the job was written.
	if ms := intField(payload, "scheduledAt"); ms > 0 {
		pendingAt = ms
	}

	deleted, err := deleteUserMessagesBefore(ctx, userID, pendingAt)
	if err != nil {
		return fmt.Errorf("bulkDeleteUserMessages: %w", err)
	}

	for _, field := range []string{
		"pending_bulk_message_deletion_at",
		"pending_bulk_message_deletion_channel_count",
		"pending_bulk_message_deletion_message_count",
	} {
		if err := patchRowField(ctx, "users", rowKey, field, nil); err != nil {
			return fmt.Errorf("bulkDeleteUserMessages: clear %s: %w", field, err)
		}
	}
	log.Printf("bulkDeleteUserMessages: user=%s deleted=%d before=%s",
		userID, deleted, time.UnixMilli(pendingAt).UTC().Format(time.RFC3339))
	return nil
}

// deleteUserMessagesBefore walks the author index and deletes every message sent
// before the cutoff. The index is the only place that knows all of a person's
// messages, which is why the api keeps it in step with the messages table.
func deleteUserMessagesBefore(ctx context.Context, userID string, beforeMS int64) (int, error) {
	refs, err := listMessagesByAuthor(ctx, userID)
	if err != nil {
		return 0, err
	}
	byChannel := make(map[string][]string)
	for _, ref := range refs {
		id, err := strconv.ParseInt(ref.MessageID, 10, 64)
		if err != nil {
			continue
		}
		if beforeMS > 0 && snowflakeTime(id).UnixMilli() >= beforeMS {
			continue
		}
		byChannel[ref.ChannelID] = append(byChannel[ref.ChannelID], ref.MessageID)
	}
	deleted := 0
	for channelID, ids := range byChannel {
		for start := 0; start < len(ids); start += bulkDeleteBatch {
			end := start + bulkDeleteBatch
			if end > len(ids) {
				end = len(ids)
			}
			deletedHere := 0
			gone := make([]string, 0, end-start)
			for _, id := range ids[start:end] {
				existed, err := deleteMessageRow(ctx, channelID, id)
				if err != nil {
					return deleted, fmt.Errorf("delete %s/%s: %w", channelID, id, err)
				}
				if existed {
					deletedHere++
					gone = append(gone, id)
				}
			}
			if deletedHere == 0 {
				continue
			}
			deleted += deletedHere
			dispatchBulkDelete(ctx, channelID, gone)
		}
	}
	return deleted, nil
}

type authorMessageRef struct {
	ChannelID string
	MessageID string
}

// listMessagesByAuthor pages the author index. Row keys are
// "<author>\x1f<message>", so ordering by row_key is ordering by message id,
// which is what makes the cursor work.
func listMessagesByAuthor(ctx context.Context, userID string) ([]authorMessageRef, error) {
	prefix := bigintSeg(userID) + kvSep
	cursor := prefix
	var out []authorMessageRef
	for {
		rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'messages_by_author_id_v2'
  AND left(row_key, length($1)) = $1
  AND row_key > $2
ORDER BY row_key
LIMIT $3`, prefix, cursor, bulkScanPageSize)
		if err != nil {
			return nil, err
		}
		var page []authorMessageRef
		var lastKey string
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				rows.Close()
				return nil, err
			}
			var row map[string]any
			if err := json.Unmarshal([]byte(raw), &row); err != nil {
				continue
			}
			channelID := typedString(row["channel_id"])
			messageID := typedString(row["message_id"])
			if channelID == "" || messageID == "" {
				continue
			}
			page = append(page, authorMessageRef{ChannelID: channelID, MessageID: messageID})
			lastKey = prefix + bigintSeg(messageID)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
		out = append(out, page...)
		if len(page) < bulkScanPageSize {
			break
		}
		cursor = lastKey
	}
	return out, nil
}
