package main

// Tests for the passkey-trait backfill.
//
// The rule has three states and each one matters: a visible credential sets the
// trait, a superseded credential does not, and no credential clears it. Getting
// the middle one wrong would leave the trait on accounts whose only credential was
// replaced by the migration.

import (
	"context"
	"testing"
)

func TestSyncPasskeyTraitAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		withCredential = "920000000000000001"
		supersededOnly = "920000000000000002"
		noCredential   = "920000000000000003"
	)
	seedUser := func(id string, traits []any) {
		key := bigintSeg(id)
		if err := putKV(ctx, "users", key, key, map[string]any{
			"user_id":  kvBigintJSON(id),
			"username": map[string]any{"__fluxer_type": "string", "value": "u" + id},
			"traits":   map[string]any{"__fluxer_type": "set", "value": traits},
		}, ""); err != nil {
			t.Fatalf("seed user %s: %v", id, err)
		}
	}
	seedCredential := func(userID, credentialID string, supersededBy any) {
		key := bigintSeg(userID) + kvSep + bigintSeg(credentialID)
		row := map[string]any{
			"user_id":       kvBigintJSON(userID),
			"credential_id": kvBigintJSON(credentialID),
		}
		if supersededBy != nil {
			row["superseded_by"] = supersededBy
		}
		if err := putKV(ctx, "webauthn_credentials", key, key, row, ""); err != nil {
			t.Fatalf("seed credential: %v", err)
		}
	}

	seedUser(withCredential, []any{"premium"})
	seedCredential(withCredential, "920000000000000011", nil)

	seedUser(supersededOnly, []any{"passkey"})
	seedCredential(supersededOnly, "920000000000000021", kvBigintJSON("920000000000000022"))

	seedUser(noCredential, []any{"passkey"})

	t.Cleanup(func() {
		for _, id := range []string{withCredential, supersededOnly, noCredential} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, bigintSeg(id))
		}
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'webauthn_credentials' AND left(row_key, length($1)) = $1`,
			bigintSeg(withCredential)+kvSep)
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'webauthn_credentials' AND left(row_key, length($1)) = $1`,
			bigintSeg(supersededOnly)+kvSep)
	})

	// A visible credential adds the trait next to the existing one.
	changed, err := syncPasskeyTrait(ctx, withCredential)
	if err != nil || changed != 1 {
		t.Fatalf("visible credential: changed=%d err=%v", changed, err)
	}
	row, _ := readKV(ctx, "users", bigintSeg(withCredential))
	traits := stringSet(row["traits"])
	if !containsString(traits, "passkey") || !containsString(traits, "premium") {
		t.Fatalf("traits = %v, want passkey and premium", traits)
	}

	// A superseded credential must not keep the trait.
	changed, err = syncPasskeyTrait(ctx, supersededOnly)
	if err != nil || changed != -1 {
		t.Fatalf("superseded credential: changed=%d err=%v", changed, err)
	}
	row, _ = readKV(ctx, "users", bigintSeg(supersededOnly))
	if containsString(stringSet(row["traits"]), "passkey") {
		t.Fatal("the trait survived a superseded-only account")
	}

	// No credentials at all: same removal.
	if changed, err = syncPasskeyTrait(ctx, noCredential); err != nil || changed != -1 {
		t.Fatalf("no credentials: changed=%d err=%v", changed, err)
	}

	// Idempotent: a second pass changes nothing.
	if changed, err := syncPasskeyTrait(ctx, withCredential); err != nil || changed != 0 {
		t.Fatalf("second pass: changed=%d err=%v", changed, err)
	}
}
