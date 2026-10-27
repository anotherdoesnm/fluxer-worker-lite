package main

// Tests for deleteMessageRow and the shred entry rules.
//
// The database half seeds a message with every satellite row upstream deletes
// with it — author index, pin, two reactions, an attachment lookup, a bucket
// stamp — plus a sibling message that must survive. Deleting and checking only
// the message row would miss exactly the rows that used to be forgotten.

import (
	"context"
	"testing"
)

func TestMessageAttachments(t *testing.T) {
	row := map[string]any{
		"attachments": []any{
			map[string]any{
				"attachment_id": kvBigintJSON("1563481711088500736"),
				"filename":      map[string]any{"__fluxer_type": "string", "value": "image.png"},
				"content_type":  "image/png",
			},
			map[string]any{"filename": "no-id.png"},
			"not an object",
		},
	}
	got := messageAttachments(row)
	if len(got) != 1 {
		t.Fatalf("got %d attachments, want 1: %+v", len(got), got)
	}
	if got[0].AttachmentID != "1563481711088500736" || got[0].Filename != "image.png" {
		t.Fatalf("unexpected attachment: %+v", got[0])
	}
	if len(messageAttachments(map[string]any{})) != 0 {
		t.Fatal("a row without attachments must yield none")
	}
}

func TestShredEntryKey(t *testing.T) {
	cases := []struct {
		name string
		in   shredEntry
		ok   bool
		key  string
	}{
		{"plain", shredEntry{ChannelID: "1", MessageID: "2"}, true, "1:2"},
		{"empty message", shredEntry{ChannelID: "1"}, false, ""},
		{"empty channel", shredEntry{MessageID: "2"}, false, ""},
		{"not a number", shredEntry{ChannelID: "1", MessageID: "abc"}, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := shredEntryKey(tc.in)
			if ok != tc.ok || key != tc.key {
				t.Fatalf("shredEntryKey(%+v) = %q, %v; want %q, %v", tc.in, key, ok, tc.key, tc.ok)
			}
		})
	}
}

func TestDeleteMessageRowAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		channelID   = "800000000000000001"
		messageID   = "800000000000000100"
		siblingID   = "800000000000000101"
		authorID    = "800000000000000200"
		otherAuthor = "800000000000000201"
		attachID    = "800000000000000300"
	)
	str := func(v string) map[string]any {
		return map[string]any{"__fluxer_type": "string", "value": v}
	}

	partition, messageKey, bucket := messageKeys(channelID, messageID)
	_, siblingKey, _ := messageKeys(channelID, siblingID)

	seed := []struct {
		table     string
		partition string
		rowKey    string
		row       map[string]any
		sortID    string
	}{
		{
			table: messagesTable, partition: partition, rowKey: messageKey,
			row: map[string]any{
				"channel_id":   kvBigintJSON(channelID),
				"message_id":   kvBigintJSON(messageID),
				"author_id":    kvBigintJSON(authorID),
				"bucket":       bucket,
				"content":      "shred me",
				"has_reaction": true,
				"attachments": []any{map[string]any{
					"attachment_id": kvBigintJSON(attachID),
					"filename":      str("image.png"),
				}},
			},
			sortID: messageID,
		},
		{
			table: messagesTable, partition: partition, rowKey: siblingKey,
			row: map[string]any{
				"channel_id": kvBigintJSON(channelID),
				"message_id": kvBigintJSON(siblingID),
				"author_id":  kvBigintJSON(otherAuthor),
				"bucket":     bucket,
				"content":    "keep me",
			},
			sortID: siblingID,
		},
		{ // author index of the shredded message
			table:     messagesByAuthor,
			partition: authorID,
			rowKey:    bigintSeg(authorID) + kvSep + bigintSeg(messageID),
			row:       map[string]any{"author_id": kvBigintJSON(authorID), "message_id": kvBigintJSON(messageID)},
		},
		{ // author index of the neighbour must survive
			table:     messagesByAuthor,
			partition: otherAuthor,
			rowKey:    bigintSeg(otherAuthor) + kvSep + bigintSeg(siblingID),
			row:       map[string]any{"author_id": kvBigintJSON(otherAuthor), "message_id": kvBigintJSON(siblingID)},
		},
		{ // a pin: the middle segment (timestamp) is upstream's encoding
			table:     channelPinsTable,
			partition: channelID,
			rowKey:    bigintSeg(channelID) + kvSep + "2026-01-01T00:00:00.000Z" + kvSep + bigintSeg(messageID),
			row:       map[string]any{"channel_id": kvBigintJSON(channelID), "message_id": kvBigintJSON(messageID)},
		},
		{ // two reactions on the shredded message
			table:     reactionsTable,
			partition: partition,
			rowKey:    partition + kvSep + bigintSeg(messageID) + kvSep + "👍" + kvSep + "user1",
			row:       map[string]any{"channel_id": kvBigintJSON(channelID), "message_id": kvBigintJSON(messageID)},
		},
		{
			table:     reactionsTable,
			partition: partition,
			rowKey:    partition + kvSep + bigintSeg(messageID) + kvSep + "🎉" + kvSep + "user2",
			row:       map[string]any{"channel_id": kvBigintJSON(channelID), "message_id": kvBigintJSON(messageID)},
		},
		{ // attachment lookup
			table:     attachmentLookupTbl,
			partition: channelID,
			rowKey:    bigintSeg(channelID) + kvSep + bigintSeg(attachID) + kvSep + "image.png",
			row:       map[string]any{"channel_id": kvBigintJSON(channelID), "filename": str("image.png")},
		},
		{ // bucket stamp of the bucket we are about to empty
			table:     channelBucketsTable,
			partition: channelID,
			rowKey:    partition,
			row:       map[string]any{"channel_id": kvBigintJSON(channelID), "bucket": bucket},
		},
	}
	for _, s := range seed {
		if err := putKV(ctx, s.table, s.partition, s.rowKey, s.row, s.sortID); err != nil {
			t.Fatalf("seed %s %s: %v", s.table, s.rowKey, err)
		}
	}
	t.Cleanup(func() {
		for _, s := range seed {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, s.table, s.rowKey)
		}
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, emptyBucketsTable, partition)
	})

	existed, err := deleteMessageRow(ctx, channelID, messageID)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !existed {
		t.Fatal("delete reported the message as absent")
	}

	count := func(t *testing.T, table, rowKey string) int {
		t.Helper()
		var n int
		if err := conn.QueryRow(
			`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, table, rowKey).Scan(&n); err != nil {
			t.Fatalf("count %s %s: %v", table, rowKey, err)
		}
		return n
	}
	if n := count(t, messagesTable, messageKey); n != 0 {
		t.Fatal("message row survived")
	}
	if n := count(t, messagesByAuthor, bigintSeg(authorID)+kvSep+bigintSeg(messageID)); n != 0 {
		t.Fatal("author index row survived")
	}
	if n := count(t, messagesTable, siblingKey); n != 1 {
		t.Fatal("the neighbouring message was deleted too")
	}
	if n := count(t, messagesByAuthor, bigintSeg(otherAuthor)+kvSep+bigintSeg(siblingID)); n != 1 {
		t.Fatal("the neighbour's author index row was deleted")
	}

	var pins, reactions, lookups int
	if err := conn.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND right(row_key, length($2)) = $2`,
		channelPinsTable, kvSep+bigintSeg(messageID)).Scan(&pins); err != nil {
		t.Fatalf("count pins: %v", err)
	}
	if pins != 0 {
		t.Fatalf("%d pin rows survived", pins)
	}
	if err := conn.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND left(row_key, length($2)) = $2`,
		reactionsTable, partition+kvSep+bigintSeg(messageID)+kvSep).Scan(&reactions); err != nil {
		t.Fatalf("count reactions: %v", err)
	}
	if reactions != 0 {
		t.Fatalf("%d reaction rows survived", reactions)
	}
	if err := conn.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND left(row_key, length($2)) = $2`,
		attachmentLookupTbl, bigintSeg(channelID)+kvSep+bigintSeg(attachID)+kvSep).Scan(&lookups); err != nil {
		t.Fatalf("count lookups: %v", err)
	}
	if lookups != 0 {
		t.Fatalf("%d attachment lookup rows survived", lookups)
	}

	// The bucket still holds the sibling, so its stamp must stay.
	if n := count(t, channelBucketsTable, partition); n != 1 {
		t.Fatal("bucket stamp dropped while the bucket still had a message")
	}

	if existed, err := deleteMessageRow(ctx, channelID, messageID); err != nil || existed {
		t.Fatalf("second delete = %v, %v; want false, nil", existed, err)
	}
}

func TestDeleteMessageEmptiesBucketAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		channelID = "810000000000000001"
		messageID = "810000000000000100"
	)
	partition, key, bucket := messageKeys(channelID, messageID)
	seed := []struct{ table, rowKey string }{
		{messagesTable, key},
		{channelBucketsTable, partition},
	}
	if err := putKV(ctx, messagesTable, partition, key, map[string]any{
		"channel_id": kvBigintJSON(channelID),
		"message_id": kvBigintJSON(messageID),
		"author_id":  kvBigintJSON("810000000000000200"),
		"bucket":     bucket,
	}, messageID); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	if err := putKV(ctx, channelBucketsTable, channelID, partition, map[string]any{
		"channel_id": kvBigintJSON(channelID), "bucket": bucket,
	}, ""); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range seed {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, s.table, s.rowKey)
		}
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, emptyBucketsTable, partition)
	})

	if _, err := deleteMessageRow(ctx, channelID, messageID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	var stamp int
	if err := conn.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		channelBucketsTable, partition).Scan(&stamp); err != nil {
		t.Fatalf("count stamp: %v", err)
	}
	if stamp != 0 {
		t.Fatal("bucket stamp survived the last message")
	}
	var empty int
	if err := conn.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		emptyBucketsTable, partition).Scan(&empty); err != nil {
		t.Fatalf("count empty bucket: %v", err)
	}
	if empty != 1 {
		t.Fatal("the bucket was not marked empty")
	}
}
