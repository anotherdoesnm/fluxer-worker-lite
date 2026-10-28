package main

import (
	"reflect"
	"testing"
)

func TestBuildUsernameSearch(t *testing.T) {
	cases := []struct{ in, want string }{
		{"doesnm", "doesnm oesnm esnm snm nm"},
		{"AB", "ab"},
		{"a", "a"},
		{"", ""},
		{"  MixedCase ", "mixedcase ixedcase xedcase edcase dcase case ase se"},
	}
	for _, tc := range cases {
		if got := buildUsernameSearch(tc.in); got != tc.want {
			t.Fatalf("buildUsernameSearch(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// The suffix list is deduplicated: "aaa" would otherwise repeat "aa".
	if got := buildUsernameSearch("aaa"); got != "aaa aa" {
		t.Fatalf("buildUsernameSearch(aaa) = %q", got)
	}
}

func TestDiscriminatorString(t *testing.T) {
	for _, tc := range []struct {
		in   int
		want string
	}{{0, "0000"}, {7, "0007"}, {42, "0042"}, {12345, "12345"}} {
		if got := discriminatorString(tc.in); got != tc.want {
			t.Fatalf("discriminatorString(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestGuildMemberSearchDoc(t *testing.T) {
	member := map[string]any{
		"guild_id":           map[string]any{"value": "1563469298515574784", "__fluxer_type": "bigint"},
		"user_id":            map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"nick":               "kir",
		"role_ids":           []any{map[string]any{"value": "11", "__fluxer_type": "bigint"}},
		"joined_at":          map[string]any{"value": "2026-10-24T08:28:48.638Z", "__fluxer_type": "date"},
		"join_source_type":   2,
		"source_invite_code": "abc",
		"inviter_id":         map[string]any{"value": "42", "__fluxer_type": "bigint"},
	}
	user := map[string]any{
		"user_id":       map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"username":      "doesnm",
		"global_name":   "Kirill",
		"discriminator": 0,
		"bot":           false,
	}
	doc := guildMemberSearchDoc("1563469298515574784", member, user)

	if doc["id"] != "1563469298515574784_1563468490076061696" {
		t.Fatalf("id = %v", doc["id"])
	}
	if doc["guildId"] != "1563469298515574784" || doc["userId"] != "1563468490076061696" {
		t.Fatalf("ids = %v / %v", doc["guildId"], doc["userId"])
	}
	if doc["username"] != "doesnm" || doc["usernameSearch"] != "doesnm oesnm esnm snm nm" {
		t.Fatalf("username fields = %v / %v", doc["username"], doc["usernameSearch"])
	}
	if doc["discriminator"] != "0000" || doc["globalName"] != "Kirill" || doc["nickname"] != "kir" {
		t.Fatalf("display fields = %v / %v / %v", doc["discriminator"], doc["globalName"], doc["nickname"])
	}
	if !reflect.DeepEqual(doc["roleIds"], []string{"11"}) {
		t.Fatalf("roleIds = %v", doc["roleIds"])
	}
	if doc["joinedAt"] != int64(1792830528) {
		t.Fatalf("joinedAt = %v", doc["joinedAt"])
	}
	if doc["joinSourceType"] != 2 || doc["sourceInviteCode"] != "abc" || doc["inviterId"] != "42" {
		t.Fatalf("join fields = %v / %v / %v", doc["joinSourceType"], doc["sourceInviteCode"], doc["inviterId"])
	}
	// userCreatedAt is derived from the user's own snowflake, not from the row.
	if doc["userCreatedAt"] != snowflakeSeconds("1563468490076061696") {
		t.Fatalf("userCreatedAt = %v", doc["userCreatedAt"])
	}
	if doc["isBot"] != false {
		t.Fatalf("isBot = %v", doc["isBot"])
	}
}

func TestGuildMemberSearchDocMissingFields(t *testing.T) {
	doc := guildMemberSearchDoc("1", map[string]any{"user_id": map[string]any{"value": "2", "__fluxer_type": "bigint"}}, map[string]any{})
	if doc["nickname"] != nil || doc["globalName"] != nil || doc["sourceInviteCode"] != nil || doc["inviterId"] != nil {
		t.Fatalf("missing fields should be null, got %v", doc)
	}
	if doc["joinedAt"] != int64(0) {
		t.Fatalf("joinedAt = %v, want 0", doc["joinedAt"])
	}
	if !reflect.DeepEqual(doc["roleIds"], []string(nil)) {
		t.Fatalf("roleIds = %#v, want nil", doc["roleIds"])
	}
}

func TestUserSearchDoc(t *testing.T) {
	user := map[string]any{
		"user_id":        map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"username":       "doesnm",
		"discriminator":  0,
		"bot":            false,
		"system":         false,
		"flags":          map[string]any{"value": "549755813888", "__fluxer_type": "bigint"},
		"acls":           map[string]any{"value": []any{"*"}, "__fluxer_type": "set"},
		"email_verified": true,
		"email_bounced":  false,
		"last_active_at": map[string]any{"value": "2026-10-28T10:00:00.000Z", "__fluxer_type": "date"},
	}
	doc := userSearchDoc(user)
	if doc["id"] != "1563468490076061696" || doc["username"] != "doesnm" {
		t.Fatalf("identity: %v", doc)
	}
	if doc["flags"] != "549755813888" {
		t.Fatalf("flags = %v", doc["flags"])
	}
	if !reflect.DeepEqual(doc["acls"], []string{"*"}) {
		t.Fatalf("acls = %v", doc["acls"])
	}
	if doc["emailVerified"] != true || doc["isBot"] != false {
		t.Fatalf("booleans: %v", doc)
	}
	if doc["createdAt"] != snowflakeSeconds("1563468490076061696") {
		t.Fatalf("createdAt = %v", doc["createdAt"])
	}
	if doc["lastActiveAt"] != int64(1793181600) {
		t.Fatalf("lastActiveAt = %v", doc["lastActiveAt"])
	}
	// The fields this instance has no columns for must be null, not zero.
	for _, key := range []string{"email", "premiumType", "tempBannedUntil", "pendingDeletionAt"} {
		if doc[key] != nil {
			t.Fatalf("%s = %v, want nil", key, doc[key])
		}
	}
}

func TestUserSearchDocDefaults(t *testing.T) {
	doc := userSearchDoc(map[string]any{})
	if doc["flags"] != "0" {
		t.Fatalf("flags = %v, want the \"0\" string", doc["flags"])
	}
	if doc["lastActiveAt"] != nil {
		t.Fatalf("lastActiveAt = %v, want nil", doc["lastActiveAt"])
	}
	if !reflect.DeepEqual(doc["acls"], []string(nil)) {
		t.Fatalf("acls = %#v", doc["acls"])
	}
}
