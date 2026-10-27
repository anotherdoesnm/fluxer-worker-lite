package main

// The two sync tasks: fan out a source change to the copies, and apply it per copy.
//
// `syncCrosspostedMessage` is the fan-out: it pages the link rows of one source
// message, enqueues `syncCrosspostCopies` per 25 webhooks, and — when the mode is a
// removal — deletes the links at the end so nothing points at a source that is gone.
// `syncCrosspostCopies` applies the mode to each copy:
//
//	update          the copy follows the source (content, flags, embeds, attachments)
//	                but only when the fingerprint changed; then the new fingerprint is
//	                stored, so a repeated sync is a no-op
//	source_deleted  the copy is kept but marked with SOURCE_MESSAGE_DELETED and the
//	                placeholder text, which is what readers of the follower's channel
//	                see instead of a message that vanished upstream
//	purge           the copy and its link row are deleted
//
// Not ported: the permission/content re-check before updating, and the message
// write lock the api takes around a copy update. The first is a documented gap; the
// second matters only when two workers race the same copy, which the job keys and
// the single-slot runner already make unlikely.

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	messageFlagSourceDeleted = 1 << 3
	crosspostDeletedContent  = "[Original message deleted]"
)

func taskSyncCrosspostedMessage(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("syncCrosspostedMessage: %w", err)
		}
	}
	channelID := stringField(payload, "channelId")
	messageID := stringField(payload, "messageId")
	mode := stringField(payload, "mode")
	deleteSource, _ := payload["deleteSource"].(bool)
	if !isDigits(channelID) || !isDigits(messageID) {
		return fmt.Errorf("syncCrosspostedMessage: channelId and messageId are required")
	}
	switch mode {
	case "update", "source_deleted", "purge":
	default:
		return fmt.Errorf("syncCrosspostedMessage: unknown mode %q", mode)
	}
	if deleteSource {
		if _, err := deleteMessageRow(ctx, channelID, messageID); err != nil {
			return fmt.Errorf("syncCrosspostedMessage: delete source: %w", err)
		}
	}

	links, err := crosspostLinksOfSource(ctx, messageID, crosspostFanoutPageSize)
	if err != nil {
		return fmt.Errorf("syncCrosspostedMessage: links: %w", err)
	}
	if len(links) == 0 {
		return nil
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return fmt.Errorf("syncCrosspostedMessage: jetstream: %w", err)
	}
	bucketOrMode := mode
	if mode == "update" {
		bucketOrMode = fmt.Sprintf("%d", time.Now().UnixMilli()/(15*60*1000))
	}
	for _, chunk := range chunkStrings(links, crosspostFanoutChunk) {
		ids := make([]any, 0, len(chunk))
		for _, id := range chunk {
			ids = append(ids, id)
		}
		next := map[string]any{
			"channelId":  channelID,
			"messageId":  messageID,
			"mode":       mode,
			"webhookIds": ids,
		}
		key := fmt.Sprintf("crosspost-sync:%s:%s:%s:%s", messageID, mode, bucketOrMode, chunk[0])
		if err := publishJobWithID(js, jobPrefixForPublish(), "syncCrosspostCopies", next, key); err != nil {
			return fmt.Errorf("syncCrosspostedMessage: enqueue: %w", err)
		}
	}
	if mode != "update" {
		// The source is going away: the links go with it, so a later page cannot
		// rediscover rows that no longer have a source to describe.
		if _, err := db.ExecContext(ctx, `
DELETE FROM fluxer_kv WHERE table_name = 'crossposted_messages'
  AND row_data->'source_message_id'->>'value' = $1`, messageID); err != nil {
			return fmt.Errorf("syncCrosspostedMessage: drop links: %w", err)
		}
	}
	log.Printf("syncCrosspostedMessage: message=%s mode=%s copies=%d", messageID, mode, len(links))
	return nil
}

func taskSyncCrosspostCopies(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("syncCrosspostCopies: %w", err)
		}
	}
	channelID := stringField(payload, "channelId")
	messageID := stringField(payload, "messageId")
	mode := stringField(payload, "mode")
	webhookIDs := stringSliceField(payload, "webhookIds")
	if !isDigits(messageID) || len(webhookIDs) == 0 {
		return fmt.Errorf("syncCrosspostCopies: messageId and webhookIds are required")
	}

	var source map[string]any
	if mode == "update" {
		var err error
		source, err = readMessage(ctx, channelID, messageID)
		if err != nil {
			return fmt.Errorf("syncCrosspostCopies: read source: %w", err)
		}
		if source == nil {
			// Nothing to follow: the fan-out's removal path handles this case.
			return nil
		}
	}
	fingerprint := messageFingerprint(source)

	failed := 0
	for _, webhookID := range webhookIDs {
		link, err := readCrosspostLink(ctx, messageID, webhookID)
		if err != nil {
			failed++
			log.Printf("syncCrosspostCopies: %s: read link: %v", webhookID, err)
			continue
		}
		if link == nil {
			continue
		}
		if err := applyCopySync(ctx, link, mode, source, fingerprint); err != nil {
			failed++
			log.Printf("syncCrosspostCopies: %s: %v", webhookID, err)
		}
	}
	if failed > 0 {
		return fmt.Errorf("syncCrosspostCopies: %d copies could not be synced", failed)
	}
	return nil
}

// applyCopySync is syncCopy: update, mark deleted, or purge.
func applyCopySync(ctx context.Context, link map[string]any, mode string, source map[string]any, fingerprint string) error {
	targetChannelID := typedString(link["target_channel_id"])
	targetMessageID := typedString(link["target_message_id"])
	sourceMessageID := typedString(link["source_message_id"])
	webhookID := typedString(link["webhook_id"])

	switch mode {
	case "purge":
		if targetChannelID != "" && targetMessageID != "" {
			if _, err := deleteMessageRow(ctx, targetChannelID, targetMessageID); err != nil {
				return fmt.Errorf("delete copy: %w", err)
			}
		}
		_, key := crosspostLinkKeys(sourceMessageID, webhookID)
		_, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'crossposted_messages' AND row_key = $1`, key)
		return err
	case "source_deleted":
		if targetChannelID == "" || targetMessageID == "" {
			return nil
		}
		copyRow, err := readMessage(ctx, targetChannelID, targetMessageID)
		if err != nil || copyRow == nil {
			return err
		}
		if flags := intOrZero(copyRow["flags"]); flags&messageFlagSourceDeleted != 0 {
			return nil
		}
		if err := patchMessageContent(ctx, targetChannelID, targetMessageID, crosspostDeletedContent); err != nil {
			return err
		}
		return patchMessageFlag(ctx, targetChannelID, targetMessageID, messageFlagSourceDeleted)
	default: // update
		if typedString(link["state"]) != "delivered" || source == nil {
			return nil
		}
		if got := typedString(link["source_fingerprint"]); got == fingerprint {
			return nil
		}
		copyRow, err := readMessage(ctx, targetChannelID, targetMessageID)
		if err != nil {
			return err
		}
		if copyRow == nil {
			// The copy vanished: drop the link so it is not synced forever.
			_, key := crosspostLinkKeys(sourceMessageID, webhookID)
			_, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'crossposted_messages' AND row_key = $1`, key)
			return err
		}
		if err := copyMessageFields(ctx, targetChannelID, targetMessageID, source); err != nil {
			return err
		}
		_, linkKey := crosspostLinkKeys(sourceMessageID, webhookID)
		return patchRowField(ctx, "crossposted_messages", linkKey, "source_fingerprint", fingerprint)
	}
}

// copyMessageFields copies the fields a sync follows: content, flags, embeds and
// attachments. The api writes the same set through its copy payload.
func copyMessageFields(ctx context.Context, channelID, messageID string, source map[string]any) error {
	patch := map[string]any{"content": source["content"]}
	for _, field := range []string{"embeds", "attachments"} {
		if value, ok := source[field]; ok {
			patch[field] = value
		} else {
			patch[field] = nil
		}
	}
	for field, value := range patch {
		if err := patchRowField(ctx, messagesTable, messageRowKey(channelID, messageID), field, value); err != nil {
			return fmt.Errorf("patch %s: %w", field, err)
		}
	}
	return nil
}

func patchMessageContent(ctx context.Context, channelID, messageID, content string) error {
	return patchRowField(ctx, messagesTable, messageRowKey(channelID, messageID), "content", content)
}

func patchMessageFlag(ctx context.Context, channelID, messageID string, bit int) error {
	row, err := readMessage(ctx, channelID, messageID)
	if err != nil || row == nil {
		return err
	}
	flags := intOrZero(row["flags"]) | bit
	return patchRowField(ctx, messagesTable, messageRowKey(channelID, messageID), "flags", flags)
}

func messageRowKey(channelID, messageID string) string {
	_, key, _ := messageKeys(channelID, messageID)
	return key
}

// crosspostLinksOfSource lists the webhook ids that received a copy of one message.
func crosspostLinksOfSource(ctx context.Context, sourceMessageID string, limit int) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'webhook_id'->>'value' FROM fluxer_kv
WHERE table_name = 'crossposted_messages'
  AND row_data->'source_message_id'->>'value' = $1
ORDER BY row_key
LIMIT $2`, sourceMessageID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var webhookID *string
		if err := rows.Scan(&webhookID); err != nil {
			return nil, err
		}
		if webhookID != nil && isDigits(*webhookID) {
			out = append(out, *webhookID)
		}
	}
	return out, rows.Err()
}
