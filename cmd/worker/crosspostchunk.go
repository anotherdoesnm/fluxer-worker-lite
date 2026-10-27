package main

// taskCrosspostMessageChunk ports CrosspostMessageChunk.ts — the delivery half.
//
// For each follower webhook in the chunk: read the webhook, check it is still a
// follower of this channel and has a target channel, skip what was already
// delivered (the link row is the source of truth), write the copy, and announce it
// in the target channel.
//
// A failed delivery is retried by re-queueing only the failed webhooks with the
// api's backoff schedule and job key, so one bad endpoint does not hold the rest.
// When the source is gone, the copies it left behind are removed instead — that is
// what a delete of a crosspost source does: the copies go with it.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	webhookTypeChannelFollower = 2
	crosspostChunkConcurrency  = 5
)

func taskCrosspostMessageChunk(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("crosspostMessageChunk: %w", err)
		}
	}
	channelID := stringField(payload, "channelId")
	messageID := stringField(payload, "messageId")
	webhookIDs := stringSliceField(payload, "webhookIds")
	attempt := int(int64OrZero(payload, "attempt"))
	if !isDigits(channelID) || !isDigits(messageID) || len(webhookIDs) == 0 {
		return fmt.Errorf("crosspostMessageChunk: channelId, messageId and webhookIds are required")
	}

	source, err := readMessage(ctx, channelID, messageID)
	if err != nil {
		return fmt.Errorf("crosspostMessageChunk: read source: %w", err)
	}
	if source == nil {
		// Source deleted between fan-out and delivery: remove whatever copies exist.
		removed := 0
		for _, webhookID := range webhookIDs {
			if ok, err := removeCrosspostCopy(ctx, messageID, webhookID); err != nil {
				log.Printf("crosspostMessageChunk: %s/%s: remove copy: %v", messageID, webhookID, err)
			} else if ok {
				removed++
			}
		}
		log.Printf("crosspostMessageChunk: source %s gone, removed %d copies", messageID, removed)
		return nil
	}
	if intOrZero(source["flags"])&messageFlagCrossposted == 0 {
		log.Printf("crosspostMessageChunk: %s is no longer a crosspost source, skipping", messageID)
		return nil
	}

	channel, err := readChannel(ctx, channelID)
	if err != nil {
		return fmt.Errorf("crosspostMessageChunk: read source channel: %w", err)
	}
	sourceGuildID := ""
	if channel != nil {
		sourceGuildID = typedString(channel["guild_id"])
	}
	fingerprint := messageFingerprint(source)

	var failed []string
	for _, webhookID := range webhookIDs {
		delivered, err := deliverToFollower(ctx, webhookID, channelID, messageID, sourceGuildID, source, fingerprint)
		if err != nil {
			failed = append(failed, webhookID)
			log.Printf("crosspostMessageChunk: webhook %s: %v", webhookID, err)
			continue
		}
		_ = delivered
	}
	if len(failed) == 0 {
		return nil
	}
	// Only the failed webhooks are re-queued, with the api's backoff.
	delays := []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute}
	if attempt >= len(delays) {
		log.Printf("crosspostMessageChunk: message=%s dropping %d deliveries after the last retry: %v",
			messageID, len(failed), failed)
		return nil
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return fmt.Errorf("crosspostMessageChunk: jetstream: %w", err)
	}
	ids := make([]any, 0, len(failed))
	for _, id := range failed {
		ids = append(ids, id)
	}
	next := map[string]any{
		"channelId":  channelID,
		"messageId":  messageID,
		"webhookIds": ids,
		"attempt":    attempt + 1,
	}
	key := crosspostChunkJobKey(messageID, failed[0], attempt+1)
	if err := publishJobWithID(js, jobPrefixForPublish(), "crosspostMessageChunk", next, key); err != nil {
		return fmt.Errorf("crosspostMessageChunk: requeue: %w", err)
	}
	log.Printf("crosspostMessageChunk: message=%s requeued %d deliveries (attempt %d)", messageID, len(failed), attempt+1)
	return nil
}

// deliverToFollower writes one copy unless it is already there.
func deliverToFollower(ctx context.Context, webhookID, sourceChannelID, sourceMessageID, sourceGuildID string, source map[string]any, fingerprint string) (bool, error) {
	webhook, err := readWebhook(ctx, webhookID)
	if err != nil {
		return false, fmt.Errorf("read webhook: %w", err)
	}
	if webhook == nil {
		return false, nil // follower row gone: nothing to deliver to
	}
	if t := intOrZero(webhook["type"]); t != webhookTypeChannelFollower {
		return false, nil
	}
	if got := typedString(webhook["source_channel_id"]); got != sourceChannelID {
		return false, nil
	}
	targetChannelID := typedString(webhook["channel_id"])
	if targetChannelID == "" {
		return false, nil
	}
	link, err := readCrosspostLink(ctx, sourceMessageID, webhookID)
	if err != nil {
		return false, err
	}
	if link != nil && typedString(link["state"]) == "delivered" {
		return false, nil
	}
	targetChannel, err := readChannel(ctx, targetChannelID)
	if err != nil {
		return false, err
	}
	if targetChannel == nil {
		return false, nil
	}
	targetGuildID := typedString(targetChannel["guild_id"])

	if _, err := insertCrosspostCopy(ctx, crosspostCopySpec{
		SourceGuildID:   sourceGuildID,
		SourceChannelID: sourceChannelID,
		SourceMessageID: sourceMessageID,
		TargetGuildID:   targetGuildID,
		TargetChannelID: targetChannelID,
		WebhookID:       webhookID,
		Source:          source,
		Fingerprint:     fingerprint,
	}); err != nil {
		return false, err
	}
	return true, nil
}

// readWebhook loads a webhook row; the primary key is (webhook_id, token), so the
// row is addressed by prefix.
func readWebhook(ctx context.Context, webhookID string) (map[string]any, error) {
	var raw string
	err := db.QueryRowContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'webhooks' AND left(row_key, length($1)) = $1
LIMIT 1`, bigintSeg(webhookID)+kvSep).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		return nil, err
	}
	return row, nil
}

// removeCrosspostCopy deletes a copy and its link row (source deleted).
func removeCrosspostCopy(ctx context.Context, sourceMessageID, webhookID string) (bool, error) {
	link, err := readCrosspostLink(ctx, sourceMessageID, webhookID)
	if err != nil || link == nil {
		return false, err
	}
	targetChannelID := typedString(link["target_channel_id"])
	targetMessageID := typedString(link["target_message_id"])
	if targetChannelID != "" && targetMessageID != "" {
		if _, err := deleteMessageRow(ctx, targetChannelID, targetMessageID); err != nil {
			return false, err
		}
	}
	_, key := crosspostLinkKeys(sourceMessageID, webhookID)
	if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'crossposted_messages' AND row_key = $1`, key); err != nil {
		return false, err
	}
	return true, nil
}
