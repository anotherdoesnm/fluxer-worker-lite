package main

// Tests for the admin "delete every message from these users" sweep.
//
// The sweep itself needs NATS (it only queues shred jobs), so the test pins the
// half that does the work: collecting a user's messages from the author index,
// with the channel count that ends up in the audit row.

import (
	"context"
	"testing"
)

func TestCollectUserMessageEntriesAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		userID   = "870000000000000001"
		channelA = "870000000000000002"
		channelB = "870000000000000003"
		message1 = "870000000000000010"
		message2 = "870000000000000011"
		message3 = "870000000000000012"
	)

	seed := func(channelID, messageID string) {
		key := bigintSeg(userID) + kvSep + bigintSeg(messageID)
		if err := putKV(ctx, messagesByAuthor, key, key, map[string]any{
			"author_id":  kvBigintJSON(userID),
			"channel_id": kvBigintJSON(channelID),
			"message_id": kvBigintJSON(messageID),
		}, ""); err != nil {
			t.Fatalf("seed index %s: %v", messageID, err)
		}
	}
	seed(channelA, message1)
	seed(channelA, message2)
	seed(channelB, message3)
	t.Cleanup(func() {
		for _, id := range []string{message1, message2, message3} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
				messagesByAuthor, bigintSeg(userID)+kvSep+bigintSeg(id))
		}
	})

	entries, channels, messages, err := collectUserMessageEntries(ctx, userID)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if messages != 3 {
		t.Fatalf("messages = %d, want 3", messages)
	}
	if channels != 2 {
		t.Fatalf("channels = %d, want 2", channels)
	}
	found := map[string]string{}
	for _, e := range entries {
		found[e[1]] = e[0]
	}
	if found[message1] != channelA || found[message2] != channelA || found[message3] != channelB {
		t.Fatalf("entries point at wrong channels: %v", found)
	}

	// A user with no messages must yield zero of everything, not an error: the
	// sweep skips such users and says so in the summary.
	entries, channels, messages, err = collectUserMessageEntries(ctx, "870000000000000099")
	if err != nil || len(entries) != 0 || channels != 0 || messages != 0 {
		t.Fatalf("empty user: entries=%v channels=%d messages=%d err=%v", entries, channels, messages, err)
	}
}
