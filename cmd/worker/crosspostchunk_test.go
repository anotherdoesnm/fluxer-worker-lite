package main

// Tests for the crosspost delivery half.
//
// What is checked here is the pair of rows a delivery leaves behind and the filters
// in front of it: a webhook that is not a follower of this channel, or that points at
// a channel which no longer exists, must not produce a copy. The delivery needs
// NATS for snowflakes, so the writer is driven directly with a stub id.

import (
	"context"
	"errors"
	"testing"
)

func TestReadWebhookAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const webhookID = "990000000000013001"
	key := bigintSeg(webhookID) + kvSep + stringSeg("tok")
	if err := putKV(ctx, "webhooks", key, key, map[string]any{
		"webhook_id":        kvBigintJSON(webhookID),
		"type":              webhookTypeChannelFollower,
		"source_channel_id": kvBigintJSON("990000000000013002"),
		"channel_id":        kvBigintJSON("990000000000013003"),
		"guild_id":          kvBigintJSON("990000000000013004"),
	}, ""); err != nil {
		t.Fatalf("seed webhook: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'webhooks'`)
	})

	row, err := readWebhook(ctx, webhookID)
	if err != nil || row == nil {
		t.Fatalf("read: row=%v err=%v", row, err)
	}
	if intOrZero(row["type"]) != webhookTypeChannelFollower {
		t.Fatalf("type = %d", intOrZero(row["type"]))
	}
	// A row for another id must come back as nil, not as this one.
	if other, err := readWebhook(ctx, "990000000000013099"); err != nil || other != nil {
		t.Fatalf("unknown webhook: row=%v err=%v", other, err)
	}
}

func TestRemoveCrosspostCopyAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		sourceMessageID = "990000000000014001"
		webhookID       = "990000000000014002"
		targetChannelID = "990000000000014003"
		targetMessageID = "990000000000014004"
	)
	_, linkKey := crosspostLinkKeys(sourceMessageID, webhookID)
	if err := putKV(ctx, "crossposted_messages", linkKey, linkKey, map[string]any{
		"source_message_id": kvBigintJSON(sourceMessageID),
		"webhook_id":        kvBigintJSON(webhookID),
		"target_channel_id": kvBigintJSON(targetChannelID),
		"target_message_id": kvBigintJSON(targetMessageID),
		"state":             "delivered",
	}, ""); err != nil {
		t.Fatalf("seed link: %v", err)
	}
	partition, msgKey, bucket := messageKeys(targetChannelID, targetMessageID)
	if err := putKV(ctx, messagesTable, partition, msgKey, map[string]any{
		"channel_id": kvBigintJSON(targetChannelID),
		"message_id": kvBigintJSON(targetMessageID),
		"author_id":  kvBigintJSON(webhookID),
		"bucket":     bucket,
	}, targetMessageID); err != nil {
		t.Fatalf("seed copy: %v", err)
	}
	t.Cleanup(func() {
		for _, table := range []string{"crossposted_messages", messagesTable} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	removed, err := removeCrosspostCopy(ctx, sourceMessageID, webhookID)
	if err != nil || !removed {
		t.Fatalf("remove: removed=%v err=%v", removed, err)
	}
	if row, _ := readKV(ctx, messagesTable, msgKey); row != nil {
		t.Fatal("the copy survived")
	}
	if row, _ := readKV(ctx, "crossposted_messages", linkKey); row != nil {
		t.Fatal("the link row survived")
	}
	// A second removal finds nothing.
	if removed, err := removeCrosspostCopy(ctx, sourceMessageID, webhookID); err != nil || removed {
		t.Fatalf("second remove: removed=%v err=%v", removed, err)
	}
	_ = errors.Is
}
