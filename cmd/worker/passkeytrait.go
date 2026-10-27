package main

// taskBackfillPasskeyTrait ports BackfillPasskeyTrait.ts.
//
// Before passkeys were a trait, an account could hold credentials without the
// `passkey` trait on its row, so the profile card and a few checks disagreed with
// reality. This one-shot task walks the credential index, and for every account
// that owns at least one *visible* credential (not superseded by a newer one)
// makes the trait match: add it when credentials exist, drop it when they do not.
//
// Idempotent by construction: it compares the trait to the truth and writes only
// when they differ, and it records the completion marker the api checks, so a
// second run is a no-op.
//
// One documented gap: the api dispatches USER_UPDATE after each change so open
// clients refresh the profile card. That payload is the *private* user object,
// which this worker does not build yet, so the row is updated and clients pick the
// trait up on their next fetch. Sending the partial object in its place would show
// the owner a profile with fields missing.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

const (
	passkeyTraitDoneKey = "backfill:passkey_trait:done"
	passkeyTrait        = "passkey"
)

func taskBackfillPasskeyTrait(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("backfillPasskeyTrait: %w", err)
		}
	}
	owners, err := passkeyOwners(ctx)
	if err != nil {
		return fmt.Errorf("backfillPasskeyTrait: credential owners: %w", err)
	}
	set, cleared, failed := 0, 0, 0
	for _, userID := range owners {
		changed, err := syncPasskeyTrait(ctx, userID)
		if err != nil {
			failed++
			log.Printf("backfillPasskeyTrait: %s: %v", userID, err)
			continue
		}
		switch changed {
		case 1:
			set++
		case -1:
			cleared++
		}
	}
	if failed > 0 {
		// The api treats a partial failure as retryable: without the marker the
		// job is re-queued, and the next run only touches what is still wrong.
		return fmt.Errorf("backfillPasskeyTrait: %d users could not be updated", failed)
	}
	if err := valkey().Set(ctx, passkeyTraitDoneKey, time.Now().UTC().Format(time.RFC3339), 0).Err(); err != nil {
		return fmt.Errorf("backfillPasskeyTrait: completion marker: %w", err)
	}
	log.Printf("backfillPasskeyTrait: owners=%d set=%d cleared=%d", len(owners), set, cleared)
	return nil
}

// passkeyOwners reads the distinct user ids out of the credential index. The
// index is keyed by credential id, so the same account can appear many times.
func passkeyOwners(ctx context.Context) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT DISTINCT row_data->'user_id'->>'value' FROM fluxer_kv
WHERE table_name = 'webauthn_credential_lookup'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var userID *string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		if userID != nil && isDigits(*userID) {
			out = append(out, *userID)
		}
	}
	return out, rows.Err()
}

// syncPasskeyTrait makes one account's trait match its credentials. It returns 1
// when the trait was added, -1 when it was removed and 0 when nothing changed.
func syncPasskeyTrait(ctx context.Context, userID string) (int, error) {
	row, err := readKV(ctx, "users", bigintSeg(userID))
	if err != nil {
		return 0, fmt.Errorf("read user: %w", err)
	}
	if row == nil {
		return 0, nil
	}
	hasVisible, err := hasVisibleCredentials(ctx, userID)
	if err != nil {
		return 0, err
	}
	traits := stringSet(row["traits"])
	hasTrait := containsString(traits, passkeyTrait)
	if hasVisible == hasTrait {
		return 0, nil
	}
	next := make([]string, 0, len(traits)+1)
	for _, trait := range traits {
		if trait == passkeyTrait {
			continue
		}
		next = append(next, trait)
	}
	if hasVisible {
		next = append(next, passkeyTrait)
	}
	var value any
	if len(next) > 0 {
		value = map[string]any{"__fluxer_type": "set", "value": stringSliceToAny(next)}
	}
	if err := patchUserTraits(ctx, userID, value); err != nil {
		return 0, err
	}
	if hasVisible {
		return 1, nil
	}
	return -1, nil
}

// hasVisibleCredentials counts credentials that are not superseded. A superseded
// credential is one the passkey migration replaced; it stays in the table for the
// lookup, and it must not keep the trait alive.
func hasVisibleCredentials(ctx context.Context, userID string) (bool, error) {
	prefix := bigintSeg(userID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'webauthn_credentials' AND left(row_key, length($1)) = $1`, prefix)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return false, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		if typedString(row["superseded_by"]) == "" && row["superseded_by"] == nil {
			return true, nil
		}
	}
	return false, rows.Err()
}

func patchUserTraits(ctx context.Context, userID string, value any) error {
	return patchRowField(ctx, "users", bigintSeg(userID), "traits", value)
}
