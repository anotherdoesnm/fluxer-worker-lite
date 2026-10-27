package main

// Tests for the deleted-users scrubbing backfill.
//
// The predicate is narrow on purpose: only accounts whose deletion *finished* are
// touched. A row that is deleted but still has a pending deletion is mid-flow, and
// scrubbing it would race the flow that is about to finish it.

import (
	"context"
	"testing"
)

func TestIsDeletionCompleteRow(t *testing.T) {
	base := func(mutate func(map[string]any)) map[string]any {
		row := map[string]any{
			"user_id": kvBigintJSON("990000000000010001"),
			"flags":   map[string]any{"__fluxer_type": "bigint", "value": "17179869184"}, // 1<<34 DELETED
		}
		if mutate != nil {
			mutate(row)
		}
		return row
	}
	if !isDeletionCompleteRow(base(nil)) {
		t.Fatal("a finished deletion was not recognised")
	}
	if isDeletionCompleteRow(base(func(r map[string]any) { r["pending_deletion_at"] = kvDate(kvNow()) })) {
		t.Fatal("a pending deletion was treated as finished")
	}
	if isDeletionCompleteRow(base(func(r map[string]any) {
		r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": "0"}
	})) {
		t.Fatal("a live account was treated as deleted")
	}
}

func TestScrubDeletedUserRowAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const userID = "990000000000010101"
	rowKey := bigintSeg(userID)
	if err := putKV(ctx, "users", rowKey, rowKey, map[string]any{
		"user_id":       kvBigintJSON(userID),
		"username":      map[string]any{"__fluxer_type": "string", "value": "DeletedUser"},
		"flags":         map[string]any{"__fluxer_type": "bigint", "value": "17179869184"},
		"email":         map[string]any{"__fluxer_type": "string", "value": "gone@example.org"},
		"bio":           map[string]any{"__fluxer_type": "string", "value": "left a bio"},
		"avatar_hash":   map[string]any{"__fluxer_type": "string", "value": "a_hash"},
		"locale":        "en-US",
		"password_hash": "$argon2id$...",
	}, ""); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, rowKey)
	})

	row, _ := readKV(ctx, "users", rowKey)
	cleared, err := scrubDeletedUserRow(ctx, userID, row)
	if err != nil || !cleared {
		t.Fatalf("scrub: cleared=%v err=%v", cleared, err)
	}
	row, _ = readKV(ctx, "users", rowKey)
	for _, field := range []string{"email", "bio", "avatar_hash", "locale", "password_hash"} {
		if v, ok := row[field]; ok && !isNullOrEmptyValue(v) {
			t.Fatalf("%s survived: %#v", field, v)
		}
	}
	// The identity the deletion flow wrote stays.
	if got := typedString(row["username"]); got != "DeletedUser" {
		t.Fatalf("username = %q", got)
	}
	if userFlags(row)&userFlagDeleted == 0 {
		t.Fatal("the DELETED flag was cleared")
	}

	// Second pass: nothing left to clear.
	row, _ = readKV(ctx, "users", rowKey)
	if cleared, err := scrubDeletedUserRow(ctx, userID, row); err != nil || cleared {
		t.Fatalf("second pass: cleared=%v err=%v", cleared, err)
	}
}
