package main

// Tests for the guild archive.
//
// Two things matter beyond the file shape: the task refuses to run without the admin
// archive row (that row is what the admin screen reads), and the messages land in
// chronological order even though they are read newest-first.

import (
	"context"
	"encoding/json"
	"testing"
)

func TestGuildArchiveRowKey(t *testing.T) {
	key := guildArchiveRowKey("100", "200")
	if key != stringSeg("guild")+kvSep+bigintSeg("100")+kvSep+bigintSeg("200") {
		t.Fatalf("key = %q", key)
	}
}

func TestGuildTextBasedChannel(t *testing.T) {
	for _, channelType := range []int{0, 2, 5} {
		if !guildTextBasedChannel(channelType) {
			t.Fatalf("type %d should be harvested", channelType)
		}
	}
	for _, channelType := range []int{1, 3, 4, 10, 999} {
		if guildTextBasedChannel(channelType) {
			t.Fatalf("type %d should be skipped", channelType)
		}
	}
}

func TestTaskHarvestGuildDataRequiresRecord(t *testing.T) {
	openTestDB(t)
	ctx := context.Background()
	err := taskHarvestGuildData(ctx, map[string]any{"guildId": "990000000000017001", "archiveId": "990000000000017002"})
	if err == nil {
		t.Fatal("a missing admin archive row was accepted")
	}
}

func TestGuildArchiveEntriesAndMessages(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		guildID   = "990000000000018001"
		channelID = "990000000000018002"
	)
	guildKey := bigintSeg(guildID)
	if err := putKV(ctx, guildsTable, guildKey, guildKey, map[string]any{
		"guild_id": kvBigintJSON(guildID),
		"name":     map[string]any{"__fluxer_type": "string", "value": "archived"},
		"owner_id": kvBigintJSON("990000000000018003"),
		"features": map[string]any{"__fluxer_type": "set", "value": []any{}},
	}, ""); err != nil {
		t.Fatalf("seed guild: %v", err)
	}
	linkKey := bigintSeg(guildID) + kvSep + bigintSeg(channelID)
	if err := putKV(ctx, "channels_by_guild_id", linkKey, linkKey, map[string]any{
		"guild_id": kvBigintJSON(guildID), "channel_id": kvBigintJSON(channelID),
	}, ""); err != nil {
		t.Fatalf("seed channel link: %v", err)
	}
	channelKey := bigintSeg(channelID) + kvSep + boolSeg(false)
	if err := putKV(ctx, "channels", channelKey, channelKey, map[string]any{
		"channel_id": kvBigintJSON(channelID),
		"guild_id":   kvBigintJSON(guildID),
		"name":       map[string]any{"__fluxer_type": "string", "value": "general"},
		"type":       0,
	}, ""); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	var ids []string
	for i := 0; i < 3; i++ {
		id := "99000000000001801" + string(rune('0'+i))
		ids = append(ids, id)
		partition, key, bucket := messageKeys(channelID, id)
		if err := putKV(ctx, messagesTable, partition, key, map[string]any{
			"channel_id": kvBigintJSON(channelID),
			"message_id": kvBigintJSON(id),
			"author_id":  kvBigintJSON("990000000000018003"),
			"bucket":     bucket,
			"content":    "message " + string(rune('0'+i)),
		}, id); err != nil {
			t.Fatalf("seed message %s: %v", id, err)
		}
	}
	t.Cleanup(func() {
		for _, table := range []string{guildsTable, "channels_by_guild_id", "channels", messagesTable} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	data, err := collectGuildArchive(ctx, guildID)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	messages, _ := data["messages"].(map[string][]map[string]any)
	list := messages[channelID]
	if len(list) != 3 {
		t.Fatalf("messages = %d, want 3", len(list))
	}
	for i := 1; i < len(list); i++ {
		if typedStringFromAny(list[i-1]["id"]) > typedStringFromAny(list[i]["id"]) {
			t.Fatalf("messages are not chronological: %v", list)
		}
	}

	entries, err := guildArchiveEntries(data)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	got, err := readZip(mustZip(t, entries))
	if err != nil {
		t.Fatalf("zip: %v", err)
	}
	if _, ok := got["guild.json"]; !ok {
		t.Fatal("guild.json missing")
	}
	var guildFile map[string]any
	if err := json.Unmarshal(got["guild.json"], &guildFile); err != nil {
		t.Fatalf("guild.json: %v", err)
	}
	guildBlock, _ := guildFile["guild"].(map[string]any)
	if guildBlock["name"] != "archived" {
		t.Fatalf("guild block = %#v", guildBlock)
	}
	if _, ok := got["channels/"+channelID+"/messages.json"]; !ok {
		t.Fatal("the channel message file is missing")
	}
}
