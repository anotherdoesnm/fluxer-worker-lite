package main

// Tests for dropping thread memberships when a person leaves a guild.
//
// Two rows per thread have to go, in both directions, and the task must refuse to
// act while the member row still exists — otherwise a re-join races the cleanup
// and the person loses threads they are in.

import (
	"context"
	"testing"
)

func TestRemoveThreadMembershipsAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		guildID   = "990000000000003001"
		userID    = "990000000000003002"
		parentID  = "990000000000003003"
		threadID  = "990000000000003004"
		threadTwo = "990000000000003005"
	)
	seedThread := func(threadID string) (memberKey, byUserKey string) {
		memberKey = bigintSeg(threadID) + kvSep + bigintSeg(userID)
		if err := putKV(ctx, "thread_members", memberKey, memberKey, map[string]any{
			"thread_id": kvBigintJSON(threadID),
			"user_id":   kvBigintJSON(userID),
			"guild_id":  kvBigintJSON(guildID),
			"parent_id": kvBigintJSON(parentID),
		}, ""); err != nil {
			t.Fatalf("seed thread_members: %v", err)
		}
		byUserKey = bigintSeg(userID) + kvSep + bigintSeg(guildID) + kvSep + bigintSeg(parentID) +
			kvSep + boolSeg(false) + kvSep + bigintSeg(threadID)
		if err := putKV(ctx, "thread_members_by_user", byUserKey, byUserKey, map[string]any{
			"user_id":   kvBigintJSON(userID),
			"thread_id": kvBigintJSON(threadID),
		}, ""); err != nil {
			t.Fatalf("seed thread_members_by_user: %v", err)
		}
		return memberKey, byUserKey
	}
	memberOne, byUserOne := seedThread(threadID)
	memberTwo, byUserTwo := seedThread(threadTwo)
	t.Cleanup(func() {
		for _, table := range []string{"thread_members", "thread_members_by_user", "guild_members"} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	// While the member row exists the task must do nothing.
	memberRow := bigintSeg(guildID) + kvSep + bigintSeg(userID)
	if err := putKV(ctx, "guild_members", memberRow, memberRow, map[string]any{
		"guild_id": kvBigintJSON(guildID), "user_id": kvBigintJSON(userID),
	}, ""); err != nil {
		t.Fatalf("seed member: %v", err)
	}
	if err := taskRemoveThreadMembershipsForGuildMember(ctx, map[string]any{"guildId": guildID, "userId": userID}); err != nil {
		t.Fatalf("task: %v", err)
	}
	if row, _ := readKV(ctx, "thread_members", memberOne); row == nil {
		t.Fatal("memberships were removed while the person was still a member")
	}

	// Member row gone: both directions of both threads go.
	if _, err := conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'guild_members' AND row_key = $1`, memberRow); err != nil {
		t.Fatalf("drop member: %v", err)
	}
	if err := taskRemoveThreadMembershipsForGuildMember(ctx, map[string]any{"guildId": guildID, "userId": userID}); err != nil {
		t.Fatalf("task: %v", err)
	}
	for _, key := range []string{memberOne, memberTwo} {
		if row, _ := readKV(ctx, "thread_members", key); row != nil {
			t.Fatalf("thread_members row survived: %s", key)
		}
	}
	for _, key := range []string{byUserOne, byUserTwo} {
		if row, _ := readKV(ctx, "thread_members_by_user", key); row != nil {
			t.Fatalf("thread_members_by_user row survived: %s", key)
		}
	}
}
