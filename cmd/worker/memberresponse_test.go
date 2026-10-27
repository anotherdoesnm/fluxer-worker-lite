package main

// Tests for user and member rendering.
//
// The privacy rule is the point: a hidden or enforced user must not render at
// all, so the caller can skip the dispatch instead of leaking the profile.

import (
	"context"
	"testing"
	"time"
)

func TestUserPartialResponse(t *testing.T) {
	now := time.Now().UTC()
	base := func(mutate func(map[string]any)) map[string]any {
		row := map[string]any{
			"user_id":       kvBigintJSON("900000000000000001"),
			"username":      map[string]any{"__fluxer_type": "string", "value": "alice"},
			"global_name":   map[string]any{"__fluxer_type": "string", "value": "Alice"},
			"discriminator": 7,
			"bot":           false,
			"system":        false,
			"flags":         map[string]any{"__fluxer_type": "bigint", "value": "0"},
		}
		if mutate != nil {
			mutate(row)
		}
		return row
	}

	out, ok := userPartialResponse(base(nil), now)
	if !ok {
		t.Fatal("a normal user was not rendered")
	}
	if out["discriminator"] != "0007" {
		t.Fatalf("discriminator = %v, want 0007", out["discriminator"])
	}
	if out["username"] != "alice" || out["global_name"] != "Alice" {
		t.Fatalf("identity: %#v", out)
	}

	cases := []struct {
		name string
		row  map[string]any
		ok   bool
	}{
		{"profile hidden flag", base(func(r map[string]any) {
			r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": "128"} // 1<<7
		}), false},
		{"spammer", base(func(r map[string]any) {
			r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": "64"} // 1<<6
		}), false},
		{"temporarily banned", base(func(r map[string]any) {
			r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": strconvPow38()}
			r["temp_banned_until"] = kvDate(now.Add(time.Hour))
		}), false},
		{"enforcement deletion pending", base(func(r map[string]any) {
			r["pending_deletion_at"] = kvDate(now)
			r["deletion_reason_code"] = 3
		}), false},
		{"user-requested deletion is not enforcement", base(func(r map[string]any) {
			r["pending_deletion_at"] = kvDate(now)
			r["deletion_reason_code"] = deletionReasonUserRequested
		}), true},
		{"system user is never hidden", base(func(r map[string]any) {
			r["system"] = true
			r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": "128"}
		}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := userPartialResponse(tc.row, now)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
		})
	}

	// Deleted users are anonymised in place, not skipped.
	deleted := base(func(r map[string]any) {
		r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": "17179869184"} // 1<<34
	})
	out, ok = userPartialResponse(deleted, now)
	if !ok || out["username"] != "DeletedUser" || out["discriminator"] != "0000" {
		t.Fatalf("deleted user: ok=%v out=%#v", ok, out)
	}

	// A hidden staff badge drops STAFF from the public bitfield.
	staffHidden := base(func(r map[string]any) {
		r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": strconvStaffWithHidden()}
	})
	out, _ = userPartialResponse(staffHidden, now)
	if out["flags"].(int) != 0 {
		t.Fatalf("staff-hidden flags = %d, want 0", out["flags"])
	}
}

func strconvPow38() string           { return "274877906944" }       // 1<<38
func strconvStaffWithHidden() string { return "144115188075855873" } // 1<<57 | 1<<0

func TestGuildMemberResponse(t *testing.T) {
	now := time.Now().UTC()
	user := map[string]any{
		"user_id":       kvBigintJSON("900000000000000002"),
		"username":      map[string]any{"__fluxer_type": "string", "value": "bob"},
		"discriminator": 1,
	}
	member := map[string]any{
		"guild_id":                     kvBigintJSON("900000000000000010"),
		"user_id":                      kvBigintJSON("900000000000000002"),
		"joined_at":                    kvDate(now),
		"nick":                         nil,
		"role_ids":                     nil,
		"mute":                         false,
		"deaf":                         false,
		"communication_disabled_until": nil,
	}
	out, ok := guildMemberResponse(member, user, now)
	if !ok {
		t.Fatal("a normal member was not rendered")
	}
	if roles, ok := out["roles"].([]string); !ok || roles == nil || len(roles) != 0 {
		t.Fatalf("roles = %#v, want an empty list", out["roles"])
	}
	if out["mute"] != false {
		t.Fatalf("mute = %v", out["mute"])
	}

	// A timed-out member is muted, with the deadline carried through.
	member["communication_disabled_until"] = kvDate(now.Add(time.Hour))
	out, _ = guildMemberResponse(member, user, now)
	if out["mute"] != true {
		t.Fatal("a timed-out member is not muted")
	}
	if out["communication_disabled_until"] == nil {
		t.Fatal("the timeout deadline was dropped")
	}

	// Premium-sanitized members keep no avatar or banner.
	member["is_premium_sanitized"] = true
	member["avatar_hash"] = map[string]any{"__fluxer_type": "string", "value": "a_abc"}
	out, _ = guildMemberResponse(member, user, now)
	if out["avatar"] != nil || out["banner"] != nil {
		t.Fatalf("sanitized member kept an asset: %#v", out)
	}
}

func TestApplyGuildMemberAddsAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		guildID = "900000000000000010"
		userOK  = "900000000000000020"
		userHid = "900000000000000021"
	)
	guildKey := bigintSeg(guildID)
	if err := putKV(ctx, guildsTable, guildKey, guildKey, map[string]any{
		"guild_id":     kvBigintJSON(guildID),
		"name":         map[string]any{"__fluxer_type": "string", "value": "join target"},
		"features":     map[string]any{"__fluxer_type": "set", "value": []any{}},
		"member_count": 3,
	}, ""); err != nil {
		t.Fatalf("seed guild: %v", err)
	}
	seedUser := func(id, name, flags string) {
		key := bigintSeg(id)
		if err := putKV(ctx, "users", key, key, map[string]any{
			"user_id":       kvBigintJSON(id),
			"username":      map[string]any{"__fluxer_type": "string", "value": name},
			"global_name":   nil,
			"discriminator": 0,
			"bot":           false,
			"system":        false,
			"flags":         map[string]any{"__fluxer_type": "bigint", "value": flags},
		}, ""); err != nil {
			t.Fatalf("seed user %s: %v", id, err)
		}
	}
	seedUser(userOK, "ok_user", "0")
	seedUser(userHid, "hidden_user", "128") // PROFILE_HIDDEN

	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'guilds' AND row_key = $1`, guildKey)
		for _, id := range []string{userOK, userHid} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, bigintSeg(id))
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'guild_members' AND row_key = $1`,
				bigintSeg(guildID)+kvSep+bigintSeg(id))
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'guild_members_by_user_id' AND row_key = $1`,
				bigintSeg(id)+kvSep+bigintSeg(guildID))
		}
	})

	added, already, hidden, failed, err := applyGuildMemberAdds(ctx, "900000000000000099", "", guildID, []string{userOK, userHid})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if added != 1 || hidden != 1 || already != 0 || failed != 0 {
		t.Fatalf("counters: added=%d hidden=%d already=%d failed=%d", added, hidden, already, failed)
	}

	member, err := readKV(ctx, "guild_members", bigintSeg(guildID)+kvSep+bigintSeg(userOK))
	if err != nil || member == nil {
		t.Fatalf("member row missing: %v", err)
	}
	if got := intOrZero(member["join_source_type"]); got != joinSourceAdminForceAdd {
		t.Fatalf("join_source_type = %d", got)
	}
	if member["joined_at"] == nil {
		t.Fatal("joined_at missing")
	}
	if idx, _ := readKV(ctx, "guild_members_by_user_id", bigintSeg(userOK)+kvSep+bigintSeg(guildID)); idx == nil {
		t.Fatal("the by-user index row is missing")
	}
	if h, _ := readKV(ctx, "guild_members", bigintSeg(guildID)+kvSep+bigintSeg(userHid)); h != nil {
		t.Fatal("a hidden user got a member row")
	}

	guild, _ := readKV(ctx, guildsTable, guildKey)
	if got := intOrZero(guild["member_count"]); got != 4 {
		t.Fatalf("member_count = %d, want 4", got)
	}
}
