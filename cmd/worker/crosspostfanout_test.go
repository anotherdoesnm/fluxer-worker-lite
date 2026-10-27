package main

// Tests for the crosspost fan-out half.
//
// The task itself needs NATS (it only enqueues), so the test pins the two pieces it
// stands on: the follower cursor (stable order, no repeats across pages) and the
// chunking, plus the source check that keeps an edit or a delete from being fanned
// out as a fresh post.

import (
	"context"
	"testing"
)

func TestChunkStrings(t *testing.T) {
	got := chunkStrings([]string{"a", "b", "c", "d", "e"}, 2)
	if len(got) != 3 || len(got[0]) != 2 || len(got[2]) != 1 {
		t.Fatalf("chunks = %v", got)
	}
	if chunkStrings(nil, 25) != nil {
		t.Fatal("an empty list produced a chunk")
	}
}

func TestFollowerWebhooksPagingAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const channelID = "990000000000011001"
	ids := []string{"990000000000011010", "990000000000011011", "990000000000011012"}
	for _, id := range ids {
		key := bigintSeg(channelID) + kvSep + bigintSeg(id)
		if err := putKV(ctx, "webhooks_by_source_channel_id", key, key, map[string]any{
			"source_channel_id": kvBigintJSON(channelID),
			"webhook_id":        kvBigintJSON(id),
		}, ""); err != nil {
			t.Fatalf("seed follower %s: %v", id, err)
		}
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'webhooks_by_source_channel_id'`)
	})

	first, err := followerWebhooks(ctx, channelID, "", 2)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("page 1 = %v", first)
	}
	second, err := followerWebhooks(ctx, channelID, first[len(first)-1], 2)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(second) != 1 || second[0] != ids[2] {
		t.Fatalf("page 2 = %v, want the remaining follower %s", second, ids[2])
	}
	for _, id := range first {
		if id == second[0] {
			t.Fatal("the cursor repeated a follower")
		}
	}
}

func TestCrosspostSourceGate(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		channelID = "990000000000012001"
		messageID = "990000000000012002"
	)
	partition, key, bucket := messageKeys(channelID, messageID)
	// A message without the CROSSPOSTED flag must not fan out.
	if err := putKV(ctx, messagesTable, partition, key, map[string]any{
		"channel_id": kvBigintJSON(channelID),
		"message_id": kvBigintJSON(messageID),
		"author_id":  kvBigintJSON("990000000000012003"),
		"bucket":     bucket,
		"flags":      0,
	}, messageID); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, messagesTable)
	})

	if err := taskCrosspostMessage(ctx, map[string]any{"channelId": channelID, "messageId": messageID}); err != nil {
		t.Fatalf("ordinary message must be a quiet skip, got %v", err)
	}
}
