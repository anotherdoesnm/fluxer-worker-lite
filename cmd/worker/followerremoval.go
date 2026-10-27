package main

// taskRemoveChannelFollowers ports RemoveChannelFollowers.ts.
//
// A channel that is no longer a source for crossposting loses its follower
// webhooks: each follower is a webhook row plus three indexes (by guild, by
// channel, by source channel), and leaving them means the next crosspost fan-out
// still targets a channel that stopped publishing.
//
// The api also requeues a sync job per already-crossposted message so copies are
// removed or refreshed; that is not ported, because the crosspost consumer itself
// is not ported. The follower rows going away is what stops new fan-out.
//
// The "converted" reason has one guard: if the channel is an announcement channel
// again, its followers are kept — this task is called when a channel *stops* being
// a source, and that path can fire for a channel that was converted back.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

const channelTypeAnnouncement = 5

func taskRemoveChannelFollowers(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("removeChannelFollowers: %w", err)
		}
	}
	sourceChannelID := stringField(payload, "sourceChannelId")
	reason := stringField(payload, "reason")
	if !isDigits(sourceChannelID) {
		return fmt.Errorf("removeChannelFollowers: sourceChannelId is required")
	}
	if reason == "converted" {
		channel, err := readChannel(ctx, sourceChannelID)
		if err != nil {
			return fmt.Errorf("removeChannelFollowers: read channel: %w", err)
		}
		if channel != nil && intOrZero(channel["type"]) == channelTypeAnnouncement {
			log.Printf("removeChannelFollowers: channel %s is an announcement channel again, keeping followers", sourceChannelID)
			return nil
		}
	}

	webhookIDs, err := followerWebhooksOfChannel(ctx, sourceChannelID)
	if err != nil {
		return fmt.Errorf("removeChannelFollowers: list followers: %w", err)
	}
	removed := 0
	for _, webhookID := range webhookIDs {
		if err := deleteWebhookRows(ctx, webhookID, sourceChannelID); err != nil {
			return fmt.Errorf("removeChannelFollowers: webhook %s: %w", webhookID, err)
		}
		removed++
	}
	log.Printf("removeChannelFollowers: channel=%s reason=%s followers=%d removed=%d",
		sourceChannelID, reason, len(webhookIDs), removed)
	return nil
}

// followerWebhooksOfChannel reads webhooks_by_source_channel_id, whose rows carry
// the webhook id.
func followerWebhooksOfChannel(ctx context.Context, sourceChannelID string) ([]string, error) {
	prefix := bigintSeg(sourceChannelID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_key, row_data::text FROM fluxer_kv
WHERE table_name = 'webhooks_by_source_channel_id' AND left(row_key, length($1)) = $1`, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var rowKey, raw string
		if err := rows.Scan(&rowKey, &raw); err != nil {
			return nil, err
		}
		if id := webhookIDFromRow(raw); id != "" {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

// deleteWebhookRows removes the webhook and every index that points at it.
// webhookIDFromRow reads the webhook id out of an index row.
func webhookIDFromRow(raw string) string {
	var row map[string]any
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		return ""
	}
	return typedString(row["webhook_id"])
}

func deleteWebhookRows(ctx context.Context, webhookID, sourceChannelID string) error {
	// The main row is keyed (webhook_id, webhook_token), so it goes by prefix.
	if _, err := db.ExecContext(ctx, `
DELETE FROM fluxer_kv WHERE table_name = 'webhooks' AND left(row_key, length($1)) = $1`,
		bigintSeg(webhookID)+kvSep); err != nil {
		return fmt.Errorf("webhooks: %w", err)
	}
	for _, table := range []string{"webhooks_by_guild_id", "webhooks_by_channel_id"} {
		if _, err := db.ExecContext(ctx, `
DELETE FROM fluxer_kv WHERE table_name = $1 AND row_data->'webhook_id'->>'value' = $2`, table, webhookID); err != nil {
			return fmt.Errorf("%s: %w", table, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
DELETE FROM fluxer_kv WHERE table_name = 'webhooks_by_source_channel_id' AND row_data->'webhook_id'->>'value' = $1`,
		webhookID); err != nil {
		return fmt.Errorf("webhooks_by_source_channel_id: %w", err)
	}
	return nil
}
