package main

// Tests for purging a channel's threads.
//
// A thread that survives half-purged is worse than one left alone: the guild
// would keep a channel row with no messages, or a message with no channel. The test
// therefore seeds every table a thread touches and checks all of them, not just the
// state row.

import (
	"context"
	"testing"
)

func TestDeleteChannelThreadsAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		guildID   = "990000000000001001"
		parentID  = "990000000000001002"
		threadID  = "990000000000001003"
		userID    = "990000000000001004"
		messageID = "990000000000001005"
	)

	// Thread state and both index rows.
	stateKey := bigintSeg(threadID)
	if err := putKV(ctx, threadStateTable, stateKey, stateKey, map[string]any{
		"thread_id": kvBigintJSON(threadID),
		"guild_id":  kvBigintJSON(guildID),
		"parent_id": kvBigintJSON(parentID),
		"type":      11,
		"archived":  false,
	}, ""); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	parentKey := bigintSeg(parentID) + kvSep + bigintSeg(threadID)
	if err := putKV(ctx, threadsByParentTable, parentKey, parentKey, map[string]any{
		"parent_id": kvBigintJSON(parentID),
		"thread_id": kvBigintJSON(threadID),
		"guild_id":  kvBigintJSON(guildID),
		"type":      11,
	}, ""); err != nil {
		t.Fatalf("seed parent index: %v", err)
	}
	activeKey := bigintSeg(guildID) + kvSep + bigintSeg(threadID)
	if err := putKV(ctx, activeThreadsTable, activeKey, activeKey, map[string]any{
		"guild_id":  kvBigintJSON(guildID),
		"thread_id": kvBigintJSON(threadID),
	}, ""); err != nil {
		t.Fatalf("seed active index: %v", err)
	}
	// Channel row of the thread itself.
	channelKey := bigintSeg(threadID) + kvSep + boolSeg(false)
	if err := putKV(ctx, "channels", channelKey, channelKey, map[string]any{
		"channel_id": kvBigintJSON(threadID),
		"guild_id":   kvBigintJSON(guildID),
		"parent_id":  kvBigintJSON(parentID),
		"type":       11,
	}, ""); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	// A message and its author index.
	partition, msgKey, bucket := messageKeys(threadID, messageID)
	if err := putKV(ctx, messagesTable, partition, msgKey, map[string]any{
		"channel_id": kvBigintJSON(threadID),
		"message_id": kvBigintJSON(messageID),
		"author_id":  kvBigintJSON(userID),
		"bucket":     bucket,
	}, messageID); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	authorKey := bigintSeg(userID) + kvSep + bigintSeg(messageID)
	if err := putKV(ctx, messagesByAuthor, authorKey, authorKey, map[string]any{
		"author_id":  kvBigintJSON(userID),
		"channel_id": kvBigintJSON(threadID),
		"message_id": kvBigintJSON(messageID),
	}, ""); err != nil {
		t.Fatalf("seed author index: %v", err)
	}
	// Membership rows in both directions.
	memberKey := bigintSeg(threadID) + kvSep + bigintSeg(userID)
	if err := putKV(ctx, "thread_members", memberKey, memberKey, map[string]any{
		"thread_id": kvBigintJSON(threadID),
		"user_id":   kvBigintJSON(userID),
		"guild_id":  kvBigintJSON(guildID),
		"parent_id": kvBigintJSON(parentID),
	}, ""); err != nil {
		t.Fatalf("seed member: %v", err)
	}
	byUserKey := bigintSeg(userID) + kvSep + bigintSeg(guildID) + kvSep + bigintSeg(parentID) + kvSep + boolSeg(false) + kvSep + bigintSeg(threadID)
	if err := putKV(ctx, "thread_members_by_user", byUserKey, byUserKey, map[string]any{
		"user_id":   kvBigintJSON(userID),
		"thread_id": kvBigintJSON(threadID),
	}, ""); err != nil {
		t.Fatalf("seed member-by-user: %v", err)
	}

	t.Cleanup(func() {
		for _, table := range []string{threadStateTable, threadsByParentTable, activeThreadsTable, "channels", messagesTable, messagesByAuthor, "thread_members", "thread_members_by_user"} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	if err := taskDeleteChannelThreads(ctx, map[string]any{"guildId": guildID, "parentId": parentID}); err != nil {
		t.Fatalf("task: %v", err)
	}

	checks := []struct{ table, key, what string }{
		{threadStateTable, stateKey, "thread state"},
		{threadsByParentTable, parentKey, "parent index"},
		{activeThreadsTable, activeKey, "active index"},
		{"channels", channelKey, "channel row"},
		{messagesTable, msgKey, "message"},
		{messagesByAuthor, authorKey, "author index"},
		{"thread_members", memberKey, "thread member"},
		{"thread_members_by_user", byUserKey, "member-by-user"},
	}
	for _, c := range checks {
		if row, _ := readKV(ctx, c.table, c.key); row != nil {
			t.Fatalf("%s survived the purge (%s %s)", c.what, c.table, c.key)
		}
	}
}
