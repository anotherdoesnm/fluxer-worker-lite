package main

// Tests for crosspost follower removal.
//
// The "converted" guard is the case that matters: the task fires when a channel
// stops being a crosspost source, but the same path runs when a channel is
// converted *back* to an announcement channel, and in that case its followers must
// survive.

import (
	"context"
	"testing"
)

func TestRemoveChannelFollowersAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		sourceChannel = "990000000000008001"
		webhookID     = "990000000000008002"
		guildID       = "990000000000008003"
		targetChannel = "990000000000008004"
	)
	seed := func(table, key string, row map[string]any) {
		if err := putKV(ctx, table, key, key, row, ""); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
	seed("webhooks", bigintSeg(webhookID)+kvSep+stringSeg("token123"), map[string]any{
		"webhook_id": kvBigintJSON(webhookID),
		"guild_id":   kvBigintJSON(guildID),
		"channel_id": kvBigintJSON(targetChannel),
	})
	seed("webhooks_by_guild_id", bigintSeg(guildID)+kvSep+bigintSeg(webhookID), map[string]any{
		"guild_id": kvBigintJSON(guildID), "webhook_id": kvBigintJSON(webhookID),
	})
	seed("webhooks_by_channel_id", bigintSeg(targetChannel)+kvSep+bigintSeg(webhookID), map[string]any{
		"channel_id": kvBigintJSON(targetChannel), "webhook_id": kvBigintJSON(webhookID),
	})
	seed("webhooks_by_source_channel_id", bigintSeg(sourceChannel)+kvSep+bigintSeg(webhookID), map[string]any{
		"source_channel_id": kvBigintJSON(sourceChannel), "webhook_id": kvBigintJSON(webhookID),
	})
	t.Cleanup(func() {
		for _, table := range []string{"webhooks", "webhooks_by_guild_id", "webhooks_by_channel_id", "webhooks_by_source_channel_id", "channels"} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	// When the channel is an announcement channel again, followers stay.
	channelKey := bigintSeg(sourceChannel) + kvSep + boolSeg(false)
	seed("channels", channelKey, map[string]any{
		"channel_id": kvBigintJSON(sourceChannel),
		"type":       channelTypeAnnouncement,
	})
	if err := taskRemoveChannelFollowers(ctx, map[string]any{"sourceChannelId": sourceChannel, "reason": "converted"}); err != nil {
		t.Fatalf("converted run: %v", err)
	}
	if row, _ := readKV(ctx, "webhooks_by_source_channel_id", bigintSeg(sourceChannel)+kvSep+bigintSeg(webhookID)); row == nil {
		t.Fatal("followers were removed while the channel is an announcement channel")
	}

	// With any other reason they go, together with all their indexes.
	if err := taskRemoveChannelFollowers(ctx, map[string]any{"sourceChannelId": sourceChannel, "reason": "deleted"}); err != nil {
		t.Fatalf("deleted run: %v", err)
	}
	for _, c := range []struct{ table, key string }{
		{"webhooks", bigintSeg(webhookID) + kvSep + stringSeg("token123")},
		{"webhooks_by_guild_id", bigintSeg(guildID) + kvSep + bigintSeg(webhookID)},
		{"webhooks_by_channel_id", bigintSeg(targetChannel) + kvSep + bigintSeg(webhookID)},
		{"webhooks_by_source_channel_id", bigintSeg(sourceChannel) + kvSep + bigintSeg(webhookID)},
	} {
		if row, _ := readKV(ctx, c.table, c.key); row != nil {
			t.Fatalf("%s survived: %s", c.table, c.key)
		}
	}
}
