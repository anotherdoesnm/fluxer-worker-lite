package main

// taskCrosspostMessage ports CrosspostMessage.ts — the fan-out half of crossposting.
//
// Publishing a message in an announcement channel fans it out to every guild that
// follows the channel. The followers are webhook rows; the fan-out itself is split
// into chunk jobs so one guild's slow endpoint cannot hold the rest, and a channel
// with more followers than one page continues with an `afterWebhookId` cursor.
//
// This task enqueues; `crosspostMessageChunk` does the delivery. That split is the
// api's, and it is the reason this half is portable on its own: it needs the source
// message and the follower index, nothing else.
//
// The chunk task is not ported yet, so the jobs it enqueues are parked (they carry
// the api's payload shape and job keys, so they will be picked up the moment the
// handler exists). That is recorded in OMISSIONS.md rather than left implicit.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
)

const (
	crosspostFanoutPageSize = 500
	crosspostFanoutChunk    = 25
	messageFlagCrossposted  = 1 << 0
)

func taskCrosspostMessage(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("crosspostMessage: %w", err)
		}
	}
	channelID := stringField(payload, "channelId")
	messageID := stringField(payload, "messageId")
	afterWebhookID := stringField(payload, "afterWebhookId")
	if !isDigits(channelID) || !isDigits(messageID) {
		return fmt.Errorf("crosspostMessage: channelId and messageId are required")
	}

	source, err := readMessage(ctx, channelID, messageID)
	if err != nil {
		return fmt.Errorf("crosspostMessage: read source: %w", err)
	}
	if source == nil || intOrZero(source["flags"])&messageFlagCrossposted == 0 {
		// Not a published message: nothing to fan out. The api logs the same and
		// returns, because an edit or a delete can race the copy.
		log.Printf("crosspostMessage: %s/%s is not a published crosspost source, skipping", channelID, messageID)
		return nil
	}

	followers, err := followerWebhooks(ctx, channelID, afterWebhookID, crosspostFanoutPageSize)
	if err != nil {
		return fmt.Errorf("crosspostMessage: followers: %w", err)
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return fmt.Errorf("crosspostMessage: jetstream: %w", err)
	}

	chunks := chunkStrings(followers, crosspostFanoutChunk)
	for _, chunk := range chunks {
		ids := make([]any, 0, len(chunk))
		for _, id := range chunk {
			ids = append(ids, id)
		}
		jobPayload := map[string]any{
			"channelId":  channelID,
			"messageId":  messageID,
			"webhookIds": ids,
			"attempt":    0,
		}
		key := crosspostChunkJobKey(messageID, chunk[0], 0)
		if err := publishJobWithID(js, jobPrefixForPublish(), "crosspostMessageChunk", jobPayload, key); err != nil {
			return fmt.Errorf("crosspostMessage: enqueue chunk: %w", err)
		}
	}

	// A full page means there may be more followers; continue with a cursor.
	if len(followers) == crosspostFanoutPageSize {
		last := followers[len(followers)-1]
		nextPayload := map[string]any{
			"channelId":      channelID,
			"messageId":      messageID,
			"afterWebhookId": last,
		}
		if err := publishJobWithID(js, jobPrefixForPublish(), "crosspostMessage", nextPayload,
			crosspostFanoutJobKey(messageID, last)); err != nil {
			return fmt.Errorf("crosspostMessage: enqueue next page: %w", err)
		}
	}
	log.Printf("crosspostMessage: message=%s followers=%d chunks=%d", messageID, len(followers), len(chunks))
	return nil
}

// followerWebhooks reads webhooks_by_source_channel_id in row-key order, so the
// cursor is stable while the fan-out runs.
func followerWebhooks(ctx context.Context, channelID, afterWebhookID string, limit int) ([]string, error) {
	prefix := bigintSeg(channelID) + kvSep
	cursor := prefix
	if afterWebhookID != "" {
		cursor = prefix + bigintSeg(afterWebhookID)
	}
	rows, err := db.QueryContext(ctx, `
SELECT row_key, row_data::text FROM fluxer_kv
WHERE table_name = 'webhooks_by_source_channel_id'
  AND left(row_key, length($1)) = $1
  AND row_key > $2
ORDER BY row_key
LIMIT $3`, prefix, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var rowKey, raw string
		if err := rows.Scan(&rowKey, &raw); err != nil {
			return nil, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		if id := typedString(row["webhook_id"]); id != "" {
			out = append(out, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// crosspostChunkJobKey is the api's key: message, first webhook of the chunk, attempt.
func crosspostChunkJobKey(messageID, firstWebhookID string, attempt int) string {
	return fmt.Sprintf("crosspost-chunk:%s:%s:%d", messageID, firstWebhookID, attempt)
}

// crosspostFanoutJobKey is the api's key for a continued page.
func crosspostFanoutJobKey(messageID, afterWebhookID string) string {
	if afterWebhookID == "" {
		return "crosspost:" + messageID
	}
	return fmt.Sprintf("crosspost:%s:%s", messageID, afterWebhookID)
}

func chunkStrings(in []string, size int) [][]string {
	if size <= 0 || len(in) == 0 {
		return nil
	}
	out := make([][]string, 0, (len(in)+size-1)/size)
	for start := 0; start < len(in); start += size {
		end := start + size
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[start:end])
	}
	return out
}
