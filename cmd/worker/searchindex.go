package main

// The search indexing tasks: message history per channel, and the two thread
// tasks. The rest of the search lane (guild members, users, discovery, the admin
// reindex) follows the same shape and lives in searchindex2.go.
//
// Every handler writes documents straight into the search tables through
// searchWriter; nothing here speaks HTTP. The shim in searchapi.go is the reader.

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"regexp"
	"strings"
	"time"
)

const (
	guildThreadStateTable = "guild_thread_state"
	messagesSearchBatch   = 5000
	threadBackfillBatch   = 100
	// Message types that must not be indexed: the thread system messages carry no
	// content of their own (isMessageSearchIndexable in MessageSearchService.ts).
	messageTypeThreadCreated      = 18
	messageTypeThreadStarter      = 21
	channelCompletionKeyTTLSecond = 24 * 60 * 60
)

// taskIndexChannelMessages ports IndexChannelMessages.ts: walk a channel's
// history in id order, upsert a search document per indexable message, then mark
// the channel as indexed. When the caller passed a completion key, the channel is
// added to a set and the last one to finish triggers the index refresh.
func taskIndexChannelMessages(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return err
		}
	}
	channelID := stringField(payload, "channelId")
	if !isDigits(channelID) {
		return errors.New("indexChannelMessages: channelId is required")
	}
	completionKey := stringField(payload, "completionKey")
	channelCount := int(int64OrZero(payload, "channelCount"))

	guildID := ""
	if channel, err := readKV(ctx, "channels", bigintSeg(channelID)); err == nil && channel != nil {
		guildID = typedString(channel["guild_id"])
	}

	writer := searchWriterFor()
	indexed, err := indexChannelHistory(ctx, writer, channelID, guildID)
	if err != nil {
		return err
	}
	if err := patchChannelIndexedAt(ctx, channelID); err != nil {
		// Upstream treats this as part of the task, but a stale indexed_at is not
		// worth failing an otherwise complete index for.
		log.Printf("indexChannelMessages: %s: patch indexed_at: %v", channelID, err)
	}
	if completionKey != "" && channelCount > 0 {
		if err := recordChannelCompletion(ctx, completionKey, channelID, channelCount); err != nil {
			log.Printf("indexChannelMessages: completion key %s: %v", completionKey, err)
		}
	}
	log.Printf("indexChannelMessages: channel=%s indexed=%d", channelID, indexed)
	return nil
}

// indexChannelHistory is the paging loop: messages are partitioned by channel, so
// a row-key range on that partition is the same order the API's listMessages
// returns and the cursor is the last row key seen.
func indexChannelHistory(ctx context.Context, writer searchWriter, channelID, guildID string) (int, error) {
	partitionPrefix := bigintSeg(channelID) + kvSep
	cursor := ""
	total := 0
	for {
		rows, err := readMessageBatch(ctx, partitionPrefix, cursor, messagesSearchBatch)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			break
		}
		docs := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			if messageTypeIsSystem(row.data) {
				continue
			}
			docs = append(docs, messageSearchDoc(row.data, guildID))
		}
		if len(docs) > 0 {
			if _, err := writer.upsert(ctx, searchIndexMessages, docs); err != nil {
				return total, err
			}
		}
		total += len(rows)
		cursor = rows[len(rows)-1].rowKey
		if len(rows) < messagesSearchBatch {
			break
		}
	}
	return total, nil
}

type messageRow struct {
	rowKey string
	data   map[string]any
}

func readMessageBatch(ctx context.Context, partitionPrefix, afterRowKey string, limit int) ([]messageRow, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_key, row_data
FROM fluxer_kv
WHERE table_name = 'messages'
  AND left(partition_key, length($1)) = $1
  AND row_key > $2
ORDER BY row_key
LIMIT $3`, partitionPrefix, afterRowKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []messageRow
	for rows.Next() {
		var rowKey string
		var raw []byte
		if err := rows.Scan(&rowKey, &raw); err != nil {
			return nil, err
		}
		data := map[string]any{}
		if err := json.Unmarshal(raw, &data); err != nil {
			continue
		}
		out = append(out, messageRow{rowKey: rowKey, data: data})
	}
	return out, rows.Err()
}

func messageTypeIsSystem(row map[string]any) bool {
	switch intOrZero(row["type"]) {
	case messageTypeThreadCreated, messageTypeThreadStarter:
		return true
	default:
		return false
	}
}

// patchChannelIndexedAt stamps the channel row, which is what the API's
// reindex scheduler looks at to decide a channel needs indexing again.
func patchChannelIndexedAt(ctx context.Context, channelID string) error {
	_, err := db.ExecContext(ctx, `
UPDATE fluxer_kv
SET row_data = row_data || jsonb_build_object('indexed_at', $2::jsonb), updated_at = now()
WHERE table_name = 'channels' AND row_key = $1`, bigintSeg(channelID), string(mustJSON(kvDate(time.Now().UTC()))))
	return err
}

// recordChannelCompletion mirrors the completion-set half of the task: the set
// counts finished channels, expires after a day, and the last one triggers the
// refresh. The refresh itself is a no-op here (documents are already visible),
// which is why nothing waits on it.
func recordChannelCompletion(ctx context.Context, key, channelID string, channelCount int) error {
	if valkey() == nil {
		return nil
	}
	if err := valkey().SAdd(ctx, key, channelID).Err(); err != nil {
		return err
	}
	if err := valkey().Expire(ctx, key, time.Duration(channelCompletionKeyTTLSecond)*time.Second).Err(); err != nil {
		return err
	}
	completed, err := valkey().SCard(ctx, key).Result()
	if err != nil {
		return err
	}
	if int(completed) >= channelCount {
		log.Printf("indexChannelMessages: all %d channels indexed, clearing %s", completed, key)
		return valkey().Del(ctx, key).Err()
	}
	return nil
}

// taskSyncThreadSearchDocument ports syncThreadSearchDocument: one thread either
// gets a fresh document or loses it when the thread is gone.
func taskSyncThreadSearchDocument(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return err
		}
	}
	threadID := stringField(payload, "threadId")
	if !isDigits(threadID) {
		return errors.New("syncThreadSearchDocument: threadId is required")
	}
	writer := searchWriterFor()
	state, err := readKV(ctx, threadStateTable, bigintSeg(threadID))
	if err != nil {
		return err
	}
	if state == nil {
		_, err := writer.deleteIDs(ctx, searchIndexThreads, []string{threadID})
		return err
	}
	channel, err := readKV(ctx, "channels", bigintSeg(threadID))
	if err != nil {
		return err
	}
	if channel == nil {
		_, err := writer.deleteIDs(ctx, searchIndexThreads, []string{threadID})
		return err
	}
	_, err = writer.upsert(ctx, searchIndexThreads, []map[string]any{threadSearchDoc(channel, state)})
	return err
}

// taskBackfillThreadSearch ports backfillThreadSearch: index every thread of a
// guild once, then stamp the guild's marker so the next attempt is a no-op.
func taskBackfillThreadSearch(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return err
		}
	}
	guildID := stringField(payload, "guildId")
	if !isDigits(guildID) {
		return errors.New("backfillThreadSearch: guildId is required")
	}
	markerKey := bigintSeg(guildID)
	marker, err := readKV(ctx, guildThreadStateTable, markerKey)
	if err != nil {
		return err
	}
	if marker != nil && typedString(marker["search_backfilled_at"]) != "" {
		return nil
	}

	threadIDs, err := listGuildThreadIDs(ctx, guildID)
	if err != nil {
		return err
	}
	writer := searchWriterFor()
	indexed := 0
	for start := 0; start < len(threadIDs); start += threadBackfillBatch {
		end := start + threadBackfillBatch
		if end > len(threadIDs) {
			end = len(threadIDs)
		}
		batch := threadIDs[start:end]
		docs := make([]map[string]any, 0, len(batch))
		for _, threadID := range batch {
			state, err := readKV(ctx, threadStateTable, bigintSeg(threadID))
			if err != nil || state == nil {
				continue
			}
			channel, err := readKV(ctx, "channels", bigintSeg(threadID))
			if err != nil || channel == nil {
				continue
			}
			docs = append(docs, threadSearchDoc(channel, state))
		}
		if len(docs) > 0 {
			if _, err := writer.upsert(ctx, searchIndexThreads, docs); err != nil {
				return err
			}
			indexed += len(docs)
		}
	}
	if err := markGuildSearchBackfilled(ctx, markerKey); err != nil {
		return err
	}
	log.Printf("backfillThreadSearch: guild=%s indexed=%d", guildID, indexed)
	return nil
}

// listGuildThreadIDs reads the thread state table for one guild. Upstream asks
// the thread index for the ids; the state row is the source of truth here, so a
// thread whose index row was lost is still backfilled.
func listGuildThreadIDs(ctx context.Context, guildID string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'thread_id'->>'value'
FROM fluxer_kv
WHERE table_name = $1 AND row_data->'guild_id'->>'value' = $2
ORDER BY row_key`, threadStateTable, guildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id *string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != nil && *id != "" {
			out = append(out, *id)
		}
	}
	return out, rows.Err()
}

func markGuildSearchBackfilled(ctx context.Context, markerKey string) error {
	value := string(mustJSON(kvDate(time.Now().UTC())))
	if _, err := db.ExecContext(ctx, `
INSERT INTO fluxer_kv (table_name, partition_key, row_key, row_data)
VALUES ($1, $2, $2, jsonb_build_object('guild_id', $3::jsonb, 'search_backfilled_at', $4::jsonb))
ON CONFLICT (table_name, row_key) DO UPDATE
SET row_data = fluxer_kv.row_data || jsonb_build_object('search_backfilled_at', $4::jsonb), updated_at = now()`,
		guildThreadStateTable, markerKey, string(mustJSON(kvBigint(strings.TrimSpace(markerKey)))), value); err != nil {
		return err
	}
	return nil
}

// --- regexes from MessageSearchSerializer.ts -------------------------------

var (
	linkRegex              = regexp.MustCompile(`https?://[^\s<>"']+`)
	hasLinkRegex           = regexp.MustCompile(`https?://[^\s/]+`)
	trailingURLPunctuation = regexp.MustCompile("[.,;:!?)\\]}\"'»“”]+$")
)

func urlHostname(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	idx := strings.Index(rawURL, "://")
	if idx < 0 {
		return ""
	}
	rest := rawURL[idx+3:]
	for _, cut := range []string{"/", "?", "#"} {
		if i := strings.Index(rest, cut); i >= 0 {
			rest = rest[:i]
		}
	}
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	if host, _, ok := strings.Cut(rest, ":"); ok {
		rest = host
	}
	return strings.ToLower(rest)
}
