package main

// Tests for the thread-permission seed.
//
// The rule has three states per bitfield: thread bits without SEND_MESSAGES are
// dropped, with SEND_MESSAGES they are granted, and a field that already matches is
// not rewritten. The channel-overwrite path additionally depends on the api's map
// encoding, so it is tested through a synthetic row in that shape.

import (
	"context"
	"testing"
)

func TestSeedThreadBits(t *testing.T) {
	cases := []struct {
		name  string
		value uint64
		want  uint64
	}{
		{
			name:  "send messages grants all three",
			value: permSendMessages,
			want:  permSendMessages | threadPermissionBits,
		},
		{
			name:  "already seeded stays put",
			value: permSendMessages | threadPermissionBits,
			want:  permSendMessages | threadPermissionBits,
		},
		{
			name:  "thread bits without send messages are dropped",
			value: threadPermissionBits,
			want:  0,
		},
		{
			name:  "unrelated bits are untouched",
			value: 1 << 5,
			want:  1 << 5,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := seedThreadBits(tc.value); got != tc.want {
				t.Fatalf("seedThreadBits(%d) = %d, want %d", tc.value, got, tc.want)
			}
		})
	}
}

func TestSeedThreadPermissionsAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		guildID   = "990000000000005001"
		channelID = "990000000000005002"
	)
	guildKey := bigintSeg(guildID)
	if err := putKV(ctx, guildsTable, guildKey, guildKey, map[string]any{
		"guild_id": kvBigintJSON(guildID),
		"name":     map[string]any{"__fluxer_type": "string", "value": "seeding"},
		"features": map[string]any{"__fluxer_type": "set", "value": []any{}},
	}, ""); err != nil {
		t.Fatalf("seed guild: %v", err)
	}
	// @everyone role without the thread bits but with SEND_MESSAGES.
	roleKey := bigintSeg(guildID) + kvSep + bigintSeg(guildID)
	if err := putKV(ctx, "guild_roles", roleKey, roleKey, map[string]any{
		"guild_id":    kvBigintJSON(guildID),
		"role_id":     kvBigintJSON(guildID),
		"permissions": map[string]any{"__fluxer_type": "bigint", "value": "2048"}, // SEND_MESSAGES
	}, ""); err != nil {
		t.Fatalf("seed role: %v", err)
	}
	// A channel with one overwrite that grants SEND_MESSAGES plus a stale thread bit.
	channelKey := bigintSeg(channelID) + kvSep + boolSeg(false)
	overwrites := map[string]any{
		"__fluxer_type": "map",
		"value": []any{
			[]any{"990000000000005010", map[string]any{
				"type":   0,
				"allow_": "2064", // SEND_MESSAGES | 1<<4
				"deny_":  "0",
			}},
		},
	}
	if err := putKV(ctx, "channels", channelKey, channelKey, map[string]any{
		"channel_id":            kvBigintJSON(channelID),
		"guild_id":              kvBigintJSON(guildID),
		"type":                  0,
		"permission_overwrites": overwrites,
	}, ""); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	idxKey := bigintSeg(guildID) + kvSep + bigintSeg(channelID)
	if err := putKV(ctx, "channels_by_guild_id", idxKey, idxKey, map[string]any{
		"guild_id": kvBigintJSON(guildID), "channel_id": kvBigintJSON(channelID),
	}, ""); err != nil {
		t.Fatalf("seed channel index: %v", err)
	}
	t.Cleanup(func() {
		for _, table := range []string{guildsTable, "guild_roles", "channels", "channels_by_guild_id", "guild_thread_state"} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	if err := taskSeedThreadPermissions(ctx, map[string]any{"guildId": guildID}); err != nil {
		t.Fatalf("task: %v", err)
	}

	role, _ := readKV(ctx, "guild_roles", roleKey)
	perms, ok := parseBigintString(typedString(role["permissions"]))
	if !ok || perms&threadPermissionBits != threadPermissionBits {
		t.Fatalf("everyone role permissions = %d", perms)
	}

	channel, _ := readKV(ctx, "channels", channelKey)
	ow, ok := channel["permission_overwrites"].(map[string]any)
	if !ok {
		t.Fatalf("overwrites missing: %#v", channel["permission_overwrites"])
	}
	pairs, _ := ow["value"].([]any)
	body := pairs[0].([]any)[1].(map[string]any)
	allow, _ := parseBigintString(typedString(body["allow_"]))
	if allow&threadPermissionBits != threadPermissionBits {
		t.Fatalf("overwrite allow = %d, want the thread bits set", allow)
	}

	marker, _ := readKV(ctx, "guild_thread_state", guildKey)
	if marker == nil || typedString(marker["perms_seeded_at"]) == "" {
		t.Fatal("the guild marker was not written")
	}

	// A second run changes nothing.
	before := typedString(body["allow_"])
	if err := taskSeedThreadPermissions(ctx, map[string]any{"guildId": guildID}); err != nil {
		t.Fatalf("second run: %v", err)
	}
	channel, _ = readKV(ctx, "channels", channelKey)
	ow, _ = channel["permission_overwrites"].(map[string]any)
	pairs, _ = ow["value"].([]any)
	body = pairs[0].([]any)[1].(map[string]any)
	if typedString(body["allow_"]) != before {
		t.Fatal("the second run rewrote the overwrite")
	}
}
