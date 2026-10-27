package main

// Tests for the guild purge cascade.
//
// A cascade is only as good as its least-covered table: leaving one index behind
// means the guild keeps appearing somewhere after deletion, which is the kind of
// bug nobody notices until it is old. The test seeds one row in every table class
// the cascade touches — guild-keyed, document-keyed, channel, message, thread,
// member — and then checks all of them.

import (
	"context"
	"testing"
)

func TestGuildProcessDeletionAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		guildID   = "990000000000004001"
		ownerID   = "990000000000004002"
		memberID  = "990000000000004003"
		channelID = "990000000000004004"
		messageID = "990000000000004005"
		threadID  = "990000000000004006"
	)
	seed := func(table, key string, row map[string]any) {
		if err := putKV(ctx, table, key, key, row, ""); err != nil {
			t.Fatalf("seed %s %s: %v", table, key, err)
		}
	}
	guildKey := bigintSeg(guildID)
	seed(guildsTable, guildKey, map[string]any{
		"guild_id": kvBigintJSON(guildID),
		"name":     map[string]any{"__fluxer_type": "string", "value": "doomed"},
		"owner_id": kvBigintJSON(ownerID),
		"features": map[string]any{"__fluxer_type": "set", "value": []any{}},
	})
	// Guild-keyed rows.
	seed("guild_roles", bigintSeg(guildID)+kvSep+bigintSeg("990000000000004010"), map[string]any{
		"guild_id": kvBigintJSON(guildID), "role_id": kvBigintJSON("990000000000004010"),
	})
	seed("guild_members", bigintSeg(guildID)+kvSep+bigintSeg(memberID), map[string]any{
		"guild_id": kvBigintJSON(guildID), "user_id": kvBigintJSON(memberID),
	})
	seed("guild_bans", bigintSeg(guildID)+kvSep+bigintSeg(memberID), map[string]any{
		"guild_id": kvBigintJSON(guildID), "user_id": kvBigintJSON(memberID),
	})
	seed("invites_by_guild_id", bigintSeg(guildID)+kvSep+stringSeg("abc"), map[string]any{
		"guild_id": kvBigintJSON(guildID), "code": "abc",
	})
	seed("channels_by_guild_id", bigintSeg(guildID)+kvSep+bigintSeg(channelID), map[string]any{
		"guild_id": kvBigintJSON(guildID), "channel_id": kvBigintJSON(channelID),
	})
	// Document-keyed indexes.
	seed("guild_members_by_user_id", bigintSeg(memberID)+kvSep+bigintSeg(guildID), map[string]any{
		"guild_id": kvBigintJSON(guildID), "user_id": kvBigintJSON(memberID),
	})
	seed("invites", stringSeg("abc"), map[string]any{"guild_id": kvBigintJSON(guildID), "code": "abc"})
	seed("webhooks", bigintSeg("990000000000004020")+kvSep+stringSeg("tok"), map[string]any{
		"guild_id": kvBigintJSON(guildID), "webhook_id": kvBigintJSON("990000000000004020"),
	})
	// Channel, message, thread.
	channelKey := bigintSeg(channelID) + kvSep + boolSeg(false)
	seed("channels", channelKey, map[string]any{
		"channel_id": kvBigintJSON(channelID), "guild_id": kvBigintJSON(guildID), "type": 0,
	})
	partition, msgKey, bucket := messageKeys(channelID, messageID)
	seed(messagesTable, msgKey, map[string]any{
		"channel_id": kvBigintJSON(channelID),
		"message_id": kvBigintJSON(messageID),
		"author_id":  kvBigintJSON(memberID),
		"bucket":     bucket,
	})
	seed(messagesByAuthor, bigintSeg(memberID)+kvSep+bigintSeg(messageID), map[string]any{
		"author_id": kvBigintJSON(memberID), "channel_id": kvBigintJSON(channelID), "message_id": kvBigintJSON(messageID),
	})
	_ = partition
	seed(threadsByParentTable, bigintSeg(channelID)+kvSep+bigintSeg(threadID), map[string]any{
		"parent_id": kvBigintJSON(channelID), "thread_id": kvBigintJSON(threadID), "guild_id": kvBigintJSON(guildID),
	})
	seed(threadStateTable, bigintSeg(threadID), map[string]any{
		"thread_id": kvBigintJSON(threadID), "guild_id": kvBigintJSON(guildID), "parent_id": kvBigintJSON(channelID),
	})

	t.Cleanup(func() {
		for _, table := range []string{
			guildsTable, "guild_roles", "guild_members", "guild_bans", "invites_by_guild_id", "invites",
			"channels_by_guild_id", "guild_members_by_user_id", "webhooks", "channels", messagesTable,
			messagesByAuthor, threadsByParentTable, threadStateTable,
		} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	if err := taskGuildProcessDeletion(ctx, map[string]any{"guildId": guildID}); err != nil {
		t.Fatalf("task: %v", err)
	}

	remaining := []struct{ table, key, what string }{
		{guildsTable, guildKey, "guild row"},
		{"guild_roles", bigintSeg(guildID) + kvSep + bigintSeg("990000000000004010"), "role"},
		{"guild_members", bigintSeg(guildID) + kvSep + bigintSeg(memberID), "member"},
		{"guild_bans", bigintSeg(guildID) + kvSep + bigintSeg(memberID), "ban"},
		{"invites_by_guild_id", bigintSeg(guildID) + kvSep + stringSeg("abc"), "invite index"},
		{"invites", stringSeg("abc"), "invite"},
		{"channels_by_guild_id", bigintSeg(guildID) + kvSep + bigintSeg(channelID), "channel index"},
		{"guild_members_by_user_id", bigintSeg(memberID) + kvSep + bigintSeg(guildID), "member by user"},
		{"webhooks", bigintSeg("990000000000004020") + kvSep + stringSeg("tok"), "webhook"},
		{"channels", channelKey, "channel"},
		{messagesTable, msgKey, "message"},
		{messagesByAuthor, bigintSeg(memberID) + kvSep + bigintSeg(messageID), "author index"},
		{threadsByParentTable, bigintSeg(channelID) + kvSep + bigintSeg(threadID), "thread parent index"},
		{threadStateTable, bigintSeg(threadID), "thread state"},
	}
	for _, r := range remaining {
		if row, _ := readKV(ctx, r.table, r.key); row != nil {
			t.Fatalf("%s survived the purge (%s %s)", r.what, r.table, r.key)
		}
	}

	// A second run on a missing guild is a quiet no-op.
	if err := taskGuildProcessDeletion(ctx, map[string]any{"guildId": guildID}); err != nil {
		t.Fatalf("second run: %v", err)
	}
}
