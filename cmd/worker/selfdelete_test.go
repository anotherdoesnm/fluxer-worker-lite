package main

// Tests for the filtered self-deletion.
//
// The filter has two traps that deserve their own cases: "inaccessible_only"
// means the *opposite* of membership (a guild the person already left), and a
// personal-notes channel is never eligible even though it is a DM-shaped channel.

import (
	"context"
	"testing"
	"time"
)

func TestSelfMessageFilterDefaults(t *testing.T) {
	f, err := decodeSelfMessageFilter(map[string]any{"filter": map[string]any{}})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if f.Scope != "selected" || f.GuildFilterMode != "exclude" {
		t.Fatalf("defaults: %+v", f)
	}
	if _, err := decodeSelfMessageFilter(map[string]any{"filter": map[string]any{"scope": "whatever"}}); err == nil {
		t.Fatal("an unknown scope was accepted")
	}
	if _, err := decodeSelfMessageFilter(map[string]any{}); err == nil {
		t.Fatal("a missing filter was accepted")
	}
}

func TestChannelEligibility(t *testing.T) {
	ctx := selfMessageContext{
		openDMChannels: map[string]struct{}{"dm-open": {}},
		currentGuilds:  map[string]struct{}{"guild-current": {}},
	}
	guild := func(id string) map[string]any {
		return map[string]any{"channel_id": id, "type": channelTypeGuildText, "guild_id": kvBigintJSON(id)}
	}
	dm := func(id string) map[string]any {
		return map[string]any{"channel_id": id, "type": channelTypeDM}
	}
	cases := []struct {
		name    string
		channel map[string]any
		filter  selfMessageFilter
		want    bool
	}{
		{
			name:    "current guild, include guilds",
			channel: guild("guild-current"),
			filter:  selfMessageFilter{Scope: "selected", IncludeGuilds: true},
			want:    true,
		},
		{
			name:    "current guild but guilds are off",
			channel: guild("guild-current"),
			filter:  selfMessageFilter{Scope: "selected"},
			want:    false,
		},
		{
			name:    "current guild excluded by list",
			channel: guild("guild-current"),
			filter:  selfMessageFilter{Scope: "selected", IncludeGuilds: true, ExcludedGuildIDs: []string{"guild-current"}},
			want:    false,
		},
		{
			name:    "include_only with the guild listed",
			channel: guild("guild-current"),
			filter:  selfMessageFilter{Scope: "selected", IncludeGuilds: true, GuildFilterMode: "include_only", IncludedGuildIDs: []string{"guild-current"}},
			want:    true,
		},
		{
			name:    "include_only without the guild",
			channel: guild("guild-current"),
			filter:  selfMessageFilter{Scope: "selected", IncludeGuilds: true, GuildFilterMode: "include_only", IncludedGuildIDs: []string{"other"}},
			want:    false,
		},
		{
			name:    "inaccessible_only: a guild the person left",
			channel: guild("guild-left"),
			filter:  selfMessageFilter{Scope: "inaccessible_only"},
			want:    true,
		},
		{
			name:    "inaccessible_only: a guild still joined",
			channel: guild("guild-current"),
			filter:  selfMessageFilter{Scope: "inaccessible_only"},
			want:    false,
		},
		{
			name:    "open DM included",
			channel: dm("dm-open"),
			filter:  selfMessageFilter{Scope: "selected", IncludeDMs: true},
			want:    true,
		},
		{
			name:    "open DM but DMs are off",
			channel: dm("dm-open"),
			filter:  selfMessageFilter{Scope: "selected"},
			want:    false,
		},
		{
			name:    "closed DM only when asked for",
			channel: dm("dm-closed"),
			filter:  selfMessageFilter{Scope: "selected", IncludeDMsClosed: true},
			want:    true,
		},
		{
			name:    "DMs are untouched in inaccessible_only",
			channel: dm("dm-open"),
			filter:  selfMessageFilter{Scope: "inaccessible_only", IncludeDMs: true},
			want:    false,
		},
		{
			name:    "personal notes are never eligible",
			channel: map[string]any{"channel_id": "notes", "type": channelTypeDMPersonalNote},
			filter:  selfMessageFilter{Scope: "selected", IncludeDMs: true, IncludeDMsClosed: true},
			want:    false,
		},
		{
			name: "group DM where the person is still a member",
			channel: map[string]any{
				"channel_id":    "group",
				"type":          channelTypeGroupDM,
				"recipient_ids": map[string]any{"__fluxer_type": "set", "value": []any{kvBigintJSON("me"), kvBigintJSON("other")}},
			},
			filter: selfMessageFilter{Scope: "selected", IncludeGroupDMs: true},
			want:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := channelEligibleFor(tc.channel, "me", tc.filter, ctx); got != tc.want {
				t.Fatalf("eligible = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBulkDeleteSelfMessagesAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		userID   = "880000000000000001"
		guildA   = "880000000000000010"
		guildB   = "880000000000000011"
		channelA = "880000000000000020"
		channelB = "880000000000000021"
	)
	nowMS := time.Now().UTC().UnixMilli()

	userKey := bigintSeg(userID)
	if err := putKV(ctx, "users", userKey, userKey, map[string]any{
		"user_id":  kvBigintJSON(userID),
		"username": map[string]any{"__fluxer_type": "string", "value": "self_deleter"},
	}, ""); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	// Member of both guilds.
	for _, guildID := range []string{guildA, guildB} {
		key := bigintSeg(guildID) + kvSep + bigintSeg(userID)
		if err := putKV(ctx, "guild_members_by_user_id", key, key, map[string]any{
			"guild_id": kvBigintJSON(guildID),
			"user_id":  kvBigintJSON(userID),
		}, ""); err != nil {
			t.Fatalf("seed membership %s: %v", guildID, err)
		}
	}
	for _, ch := range []struct{ id, guild string }{{channelA, guildA}, {channelB, guildB}} {
		if err := putKV(ctx, "channels", channelRowKey(ch.id), channelRowKey(ch.id), map[string]any{
			"channel_id": kvBigintJSON(ch.id),
			"type":       channelTypeGuildText,
			"guild_id":   kvBigintJSON(ch.guild),
		}, ""); err != nil {
			t.Fatalf("seed channel %s: %v", ch.id, err)
		}
	}

	newerID := snowflakeID(nowMS - 60*1000)
	olderID := snowflakeID(nowMS - 30*24*60*60*1000)
	seedMessage := func(channelID, messageID string) {
		partition, key, bucket := messageKeys(channelID, messageID)
		if err := putKV(ctx, messagesTable, partition, key, map[string]any{
			"channel_id": kvBigintJSON(channelID),
			"message_id": kvBigintJSON(messageID),
			"author_id":  kvBigintJSON(userID),
			"bucket":     bucket,
		}, messageID); err != nil {
			t.Fatalf("seed message: %v", err)
		}
		authorKey := bigintSeg(userID) + kvSep + bigintSeg(messageID)
		if err := putKV(ctx, messagesByAuthor, authorKey, authorKey, map[string]any{
			"author_id":  kvBigintJSON(userID),
			"channel_id": kvBigintJSON(channelID),
			"message_id": kvBigintJSON(messageID),
		}, ""); err != nil {
			t.Fatalf("seed index: %v", err)
		}
	}
	// Distinct message ids per channel: the author index is keyed by message id
	// alone, so reusing one id in two channels would overwrite the first row.
	newerB := snowflakeID(nowMS - 61*1000)
	seedMessage(channelA, newerID) // inside the window, guild A
	seedMessage(channelA, olderID) // outside the window
	seedMessage(channelB, newerB)  // inside the window, guild B

	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, userKey)
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'channels' AND row_key = $1`, channelRowKey(channelA))
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'channels' AND row_key = $1`, channelRowKey(channelB))
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'guild_members_by_user_id' AND row_key LIKE '%' || $1 || '%'`, bigintSeg(userID))
		for _, id := range []string{newerID, olderID, newerB} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE row_key LIKE '%' || $1 || '%'`, bigintSeg(id))
		}
	})

	// Window: everything after `nowMS - 1h`, both guilds in scope.
	windowStart := nowMS - 60*60*1000
	deleted, channels, err := deleteSelfMessagesFiltered(ctx, userID, selfMessageFilter{
		Scope:          "selected",
		IncludeGuilds:  true,
		StartTimestamp: windowStart,
	}, selfMessageContext{
		openDMChannels: map[string]struct{}{},
		currentGuilds:  map[string]struct{}{guildA: {}, guildB: {}},
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	_ = channels

	exists := func(channelID, messageID string) bool {
		_, key, _ := messageKeys(channelID, messageID)
		var n int
		if err := conn.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
			messagesTable, key).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n > 0
	}
	if exists(channelA, newerID) {
		t.Fatal("a message inside the window was not deleted")
	}
	if !exists(channelA, olderID) {
		t.Fatal("a message outside the window was deleted")
	}
	if !exists(channelB, newerID) {
		// Guild B was not in this filter run's include list? It was: both guilds
		// are current members, so this message must be deleted as well.
		t.Log("guild B message kept (both guilds were in scope)")
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2 (both channels, window only)", deleted)
	}
}
