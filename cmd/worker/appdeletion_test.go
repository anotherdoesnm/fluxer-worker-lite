package main

// Tests for application deletion.
//
// The task is small but the order carries the meaning: the bot must be anonymised
// and its guilds told before the application rows disappear, so a crash halfway
// leaves something explainable rather than an ownerless bot.

import (
	"context"
	"testing"
)

func TestApplicationProcessDeletionAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		appID   = "950000000000000001" // also the bot user id (rebrand is identity)
		ownerID = "950000000000000002"
		guildID = "950000000000000003"
	)
	appKey := bigintSeg(appID)
	botKey := bigintSeg(appID)

	if err := putKV(ctx, applicationsTable, appKey, appKey, map[string]any{
		"application_id": kvBigintJSON(appID),
		"name":           map[string]any{"__fluxer_type": "string", "value": "e2e app"},
		"owner_id":       kvBigintJSON(ownerID),
	}, ""); err != nil {
		t.Fatalf("seed application: %v", err)
	}
	ownerKey := bigintSeg(ownerID) + kvSep + bigintSeg(appID)
	if err := putKV(ctx, applicationsByOwnTable, ownerKey, ownerKey, map[string]any{
		"owner_user_id":  kvBigintJSON(ownerID),
		"application_id": kvBigintJSON(appID),
	}, ""); err != nil {
		t.Fatalf("seed owner index: %v", err)
	}
	if err := putKV(ctx, "users", botKey, botKey, map[string]any{
		"user_id":       kvBigintJSON(appID),
		"username":      map[string]any{"__fluxer_type": "string", "value": "e2e_bot"},
		"global_name":   map[string]any{"__fluxer_type": "string", "value": "E2E Bot"},
		"discriminator": 1234,
		"bot":           true,
		"flags":         map[string]any{"__fluxer_type": "bigint", "value": "0"},
	}, ""); err != nil {
		t.Fatalf("seed bot: %v", err)
	}
	// The bot is a member of one guild, so the task should find it there.
	memberKey := bigintSeg(guildID) + kvSep + bigintSeg(appID)
	if err := putKV(ctx, "guild_members", memberKey, memberKey, map[string]any{
		"guild_id":  kvBigintJSON(guildID),
		"user_id":   kvBigintJSON(appID),
		"joined_at": kvDate(kvNow()),
		"mute":      false,
		"deaf":      false,
	}, ""); err != nil {
		t.Fatalf("seed member: %v", err)
	}
	byUserKey := bigintSeg(appID) + kvSep + bigintSeg(guildID)
	if err := putKV(ctx, "guild_members_by_user_id", byUserKey, byUserKey, map[string]any{
		"guild_id": kvBigintJSON(guildID),
		"user_id":  kvBigintJSON(appID),
	}, ""); err != nil {
		t.Fatalf("seed member index: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range []struct{ table, key string }{
			{applicationsTable, appKey},
			{applicationsByOwnTable, ownerKey},
			{"users", botKey},
			{"guild_members", memberKey},
			{"guild_members_by_user_id", byUserKey},
		} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, s.table, s.key)
		}
	})

	if err := taskApplicationProcessDeletion(ctx, map[string]any{"applicationId": appID}); err != nil {
		t.Fatalf("task: %v", err)
	}

	bot, err := readKV(ctx, "users", botKey)
	if err != nil || bot == nil {
		t.Fatalf("bot row missing: %v", err)
	}
	if got := typedString(bot["username"]); got != "DeletedUser" {
		t.Fatalf("username = %q, want DeletedUser", got)
	}
	if got := typedString(bot["global_name"]); got != "Deleted User" {
		t.Fatalf("global_name = %q", got)
	}
	if userFlags(bot)&userFlagDeleted == 0 {
		t.Fatal("the DELETED flag was not set")
	}
	if typedString(bot["avatar_hash"]) != "" {
		t.Fatal("the avatar hash survived anonymisation")
	}
	if app, _ := readKV(ctx, applicationsTable, appKey); app != nil {
		t.Fatal("the application row survived")
	}
	if idx, _ := readKV(ctx, applicationsByOwnTable, ownerKey); idx != nil {
		t.Fatal("the owner index row survived")
	}
}

func TestApplicationProcessDeletionMissingApplication(t *testing.T) {
	openTestDB(t)
	ctx := context.Background()
	// Nothing seeded: the task must be a quiet no-op, not an error.
	if err := taskApplicationProcessDeletion(ctx, map[string]any{"applicationId": "950000000000000099"}); err != nil {
		t.Fatalf("missing application: %v", err)
	}
}
