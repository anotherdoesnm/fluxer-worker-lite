package main

// Tests for the flag sweep and the private user object behind it.
//
// The private object is only used to tell a person's own clients about their
// account, so the test pins the fields that must not be silently missing (email,
// ACLs, tier, has_password) and the two guard refusals that exist to prevent
// irreversible mistakes.

import (
	"context"
	"testing"
	"time"
)

func TestUserPrivateResponse(t *testing.T) {
	row := map[string]any{
		"user_id":        kvBigintJSON("930000000000000001"),
		"username":       map[string]any{"__fluxer_type": "string", "value": "carol"},
		"global_name":    nil,
		"discriminator":  12,
		"bot":            false,
		"system":         false,
		"flags":          map[string]any{"__fluxer_type": "bigint", "value": "0"},
		"acls":           map[string]any{"__fluxer_type": "set", "value": []any{"*"}},
		"traits":         map[string]any{"__fluxer_type": "set", "value": []any{"premium"}},
		"email":          map[string]any{"__fluxer_type": "string", "value": "carol@example.org"},
		"email_verified": true,
		"password_hash":  "$argon2id$...",
		"premium_type":   1,
	}
	out, ok := userPrivateResponse(row, time.Now().UTC())
	if !ok {
		t.Fatal("private response refused a normal user")
	}
	if out["email"] != "carol@example.org" {
		t.Fatalf("email = %#v", out["email"])
	}
	if acls, ok := out["acls"].([]string); !ok || len(acls) != 1 || acls[0] != "*" {
		t.Fatalf("acls = %#v", out["acls"])
	}
	if out["account_tier"] != "verified" {
		t.Fatalf("account_tier = %#v", out["account_tier"])
	}
	if out["has_password"] != true {
		t.Fatal("has_password is false for a row with a password hash")
	}
	if out["premium_type"] != 1 {
		t.Fatalf("premium_type = %#v", out["premium_type"])
	}
	if out["discriminator"] != "0012" {
		t.Fatalf("discriminator = %#v", out["discriminator"])
	}

	// A hidden profile must still render for its owner.
	hidden := map[string]any{
		"user_id":             kvBigintJSON("930000000000000002"),
		"username":            map[string]any{"__fluxer_type": "string", "value": "hidden_user"},
		"flags":               map[string]any{"__fluxer_type": "bigint", "value": "128"},
		"authenticator_types": map[string]any{"__fluxer_type": "set", "value": []any{"totp", "email"}},
	}
	out, ok = userPrivateResponse(hidden, time.Now().UTC())
	if !ok {
		t.Fatal("the owner cannot see their own hidden profile")
	}
	auths, _ := out["authenticator_types"].([]string)
	if len(auths) != 1 || auths[0] != "totp" {
		t.Fatalf("authenticator_types = %#v, want only totp", out["authenticator_types"])
	}
	if out["mfa_enabled"] != true {
		t.Fatal("mfa_enabled is false with an active authenticator")
	}

	// The pending bulk deletion block appears while a request is queued.
	row["pending_bulk_message_deletion_at"] = kvDate(time.Now().UTC())
	row["pending_bulk_message_deletion_message_count"] = 7
	out, _ = userPrivateResponse(row, time.Now().UTC())
	pending, ok := out["pending_bulk_message_deletion"].(map[string]any)
	if !ok || pending["message_count"] != 7 {
		t.Fatalf("pending block = %#v", out["pending_bulk_message_deletion"])
	}
}

func TestApplyUserFlagChangeAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		userID  = "930000000000000101"
		adminID = "930000000000000102"
	)
	seed := func(flags string, extra map[string]any) map[string]any {
		row := map[string]any{
			"user_id":  kvBigintJSON(userID),
			"username": map[string]any{"__fluxer_type": "string", "value": "flag_target"},
			"flags":    map[string]any{"__fluxer_type": "bigint", "value": flags},
		}
		for k, v := range extra {
			row[k] = v
		}
		key := bigintSeg(userID)
		if err := putKV(ctx, "users", key, key, row, ""); err != nil {
			t.Fatalf("seed: %v", err)
		}
		return row
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, bigintSeg(userID))
	})

	// Add LIMIT_EXEMPT (1<<62), remove nothing.
	seed("0", nil)
	changed, err := applyUserFlagChange(ctx, adminID, userID, []string{"4611686018427387904"}, nil, "")
	if err != nil || !changed {
		t.Fatalf("add flag: changed=%v err=%v", changed, err)
	}
	row, _ := readKV(ctx, "users", bigintSeg(userID))
	if userFlags(row) != userFlagLimitExempt {
		t.Fatalf("flags = %d", userFlags(row))
	}

	// Re-applying is a no-op.
	if changed, err := applyUserFlagChange(ctx, adminID, userID, []string{"4611686018427387904"}, nil, ""); err != nil || changed {
		t.Fatalf("repeat: changed=%v err=%v", changed, err)
	}

	// Refuse to remove DELETED.
	seed("17179869184", nil) // 1<<34
	_, err = applyUserFlagChange(ctx, adminID, userID, nil, []string{"17179869184"}, "")
	if err == nil || !isRefusedFlagChange(err) {
		t.Fatalf("removing DELETED returned %v, want a refusal", err)
	}

	// Refuse to remove DISABLED while the ban is still running.
	seed("274877906944", map[string]any{"temp_banned_until": kvDate(time.Now().Add(time.Hour))}) // 1<<38
	_, err = applyUserFlagChange(ctx, adminID, userID, nil, []string{"274877906944"}, "")
	if err == nil || !isRefusedFlagChange(err) {
		t.Fatalf("removing DISABLED during a ban returned %v, want a refusal", err)
	}
}
