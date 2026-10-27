package main

// Tests for the crosspost sync tasks.
//
// The three modes are the whole behaviour, and each leaves a different row behind:
// update follows the source and stores the new fingerprint, source_deleted keeps the
// copy with a placeholder and the flag, purge removes both the copy and the link.

import (
	"context"
	"testing"
)

func TestApplyCopySyncAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		sourceChannel = "990000000000015001"
		sourceMessage = "990000000000015002"
		webhookID     = "990000000000015003"
		targetChannel = "990000000000015004"
		targetMessage = "990000000000015005"
	)
	_, linkKey := crosspostLinkKeys(sourceMessage, webhookID)
	writeLink := func(fingerprint string) {
		if err := putKV(ctx, "crossposted_messages", linkKey, linkKey, map[string]any{
			"source_message_id":  kvBigintJSON(sourceMessage),
			"webhook_id":         kvBigintJSON(webhookID),
			"source_channel_id":  kvBigintJSON(sourceChannel),
			"target_channel_id":  kvBigintJSON(targetChannel),
			"target_message_id":  kvBigintJSON(targetMessage),
			"state":              "delivered",
			"source_fingerprint": fingerprint,
		}, ""); err != nil {
			t.Fatalf("seed link: %v", err)
		}
	}
	writeCopy := func(content string) {
		partition, key, bucket := messageKeys(targetChannel, targetMessage)
		if err := putKV(ctx, messagesTable, partition, key, map[string]any{
			"channel_id": kvBigintJSON(targetChannel),
			"message_id": kvBigintJSON(targetMessage),
			"author_id":  kvBigintJSON(webhookID),
			"bucket":     bucket,
			"content":    content,
			"flags":      0,
		}, targetMessage); err != nil {
			t.Fatalf("seed copy: %v", err)
		}
	}
	t.Cleanup(func() {
		for _, table := range []string{"crossposted_messages", messagesTable} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	// update: a changed fingerprint rewrites the copy and stores the new one.
	writeLink("old-fingerprint")
	writeCopy("old text")
	source := map[string]any{"content": "new text", "flags": 0}
	link, _ := readCrosspostLink(ctx, sourceMessage, webhookID)
	if err := applyCopySync(ctx, link, "update", source, messageFingerprint(source)); err != nil {
		t.Fatalf("update: %v", err)
	}
	copyRow, _ := readMessage(ctx, targetChannel, targetMessage)
	if got := copyRow["content"]; got != "new text" {
		t.Fatalf("content = %#v", got)
	}
	link, _ = readCrosspostLink(ctx, sourceMessage, webhookID)
	if got := typedString(link["source_fingerprint"]); got != messageFingerprint(source) {
		t.Fatalf("fingerprint = %q", got)
	}
	// A second update with the same fingerprint is a no-op.
	if err := applyCopySync(ctx, link, "update", source, messageFingerprint(source)); err != nil {
		t.Fatalf("second update: %v", err)
	}

	// source_deleted: the copy stays, flagged, with the placeholder.
	if err := applyCopySync(ctx, link, "source_deleted", nil, ""); err != nil {
		t.Fatalf("source_deleted: %v", err)
	}
	copyRow, _ = readMessage(ctx, targetChannel, targetMessage)
	if got := copyRow["content"]; got != crosspostDeletedContent {
		t.Fatalf("content = %#v", got)
	}
	if flags := intOrZero(copyRow["flags"]); flags&messageFlagSourceDeleted == 0 {
		t.Fatalf("flags = %d, want SOURCE_MESSAGE_DELETED", flags)
	}

	// purge: copy and link are gone.
	if err := applyCopySync(ctx, link, "purge", nil, ""); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if row, _ := readMessage(ctx, targetChannel, targetMessage); row != nil {
		t.Fatal("the copy survived a purge")
	}
	if row, _ := readKV(ctx, "crossposted_messages", linkKey); row != nil {
		t.Fatal("the link row survived a purge")
	}
}
