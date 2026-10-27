package main

// Tests for the user export.
//
// The archive's shape is the contract: `user.json` must carry the account block, the
// security file must carry sessions and relationships, and the OAuth file must be a
// list. A person comparing the export with the docs should find the same files the api
// produces.

import (
	"context"
	"encoding/json"
	"testing"
)

func TestHarvestEntriesShape(t *testing.T) {
	data := map[string]any{
		"schema_version":     harvestSchemaVersion,
		"exported_at":        "2026-10-27T00:00:00Z",
		"user":               map[string]any{"id": "1", "username": "alice"},
		"auth_sessions":      []map[string]any{{"client_ip": "203.0.113.7"}},
		"relationships":      []map[string]any{{"target_user_id": "2", "type": 1}},
		"notes":              []map[string]any{},
		"private_channels":   []map[string]any{},
		"oauth_applications": []map[string]any{{"application_id": "9"}},
		"messages_by_channel": map[string][]map[string]any{
			"555": {{"id": "1", "content": "hello"}},
		},
		"security": map[string]any{"last_active_ip": "203.0.113.7"},
	}
	entries, err := userHarvestEntries(data)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	got, err := readZip(mustZip(t, entries))
	if err != nil {
		t.Fatalf("zip: %v", err)
	}
	if _, ok := got["user.json"]; !ok {
		t.Fatal("user.json missing")
	}
	var userFile map[string]any
	if err := json.Unmarshal(got["user.json"], &userFile); err != nil {
		t.Fatalf("user.json: %v", err)
	}
	userBlock, ok := userFile["user"].(map[string]any)
	if !ok || userBlock["username"] != "alice" {
		t.Fatalf("user block = %#v", userFile["user"])
	}
	if _, ok := userFile["schema_version"]; ok {
		t.Fatal("schema_version belongs outside the archive body")
	}
	var oauth map[string]any
	if err := json.Unmarshal(got["integrations/oauth.json"], &oauth); err != nil {
		t.Fatalf("oauth: %v", err)
	}
	if _, ok := got["channels/555/messages.json"]; !ok {
		t.Fatal("the per-channel message file is missing")
	}
	if _, ok := oauth["applications"].([]any); !ok {
		t.Fatalf("oauth applications = %#v", oauth["applications"])
	}
}

func TestCollectUserHarvestAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const userID = "990000000000016001"
	userKey := bigintSeg(userID)
	if err := putKV(ctx, "users", userKey, userKey, map[string]any{
		"user_id":             kvBigintJSON(userID),
		"username":            map[string]any{"__fluxer_type": "string", "value": "harvester"},
		"locale":              "en-US",
		"flags":               map[string]any{"__fluxer_type": "bigint", "value": "0"},
		"avatar_hash":         nil,
		"authenticator_types": map[string]any{"__fluxer_type": "set", "value": []any{"totp"}},
	}, ""); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	sessKey := bigintSeg(userID) + kvSep + stringSeg("hash")
	if err := putKV(ctx, "auth_sessions", sessKey, sessKey, map[string]any{
		"user_id":   kvBigintJSON(userID),
		"client_ip": "203.0.113.7",
	}, ""); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	t.Cleanup(func() {
		for _, table := range []string{"users", "auth_sessions"} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	data, err := collectUserHarvest(ctx, userID)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	user, ok := data["user"].(map[string]any)
	if !ok || user["username"] != "harvester" {
		t.Fatalf("user = %#v", data["user"])
	}
	if user["mfa_enabled"] != true {
		t.Fatal("mfa_enabled is false with an authenticator set")
	}
	sessions, _ := data["auth_sessions"].([]map[string]any)
	if len(sessions) != 1 {
		t.Fatalf("auth_sessions = %#v", data["auth_sessions"])
	}
}

func mustZip(t *testing.T, entries []zipEntry) []byte {
	t.Helper()
	data, err := buildZip(entries)
	if err != nil {
		t.Fatalf("zip: %v", err)
	}
	return data
}
