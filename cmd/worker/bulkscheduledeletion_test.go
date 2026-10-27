package main

// Tests for the closure half of scheduled deletion.
//
// The flag pair is the subtle part: a request must leave SELF_DELETED and *not*
// DELETED (the account is still readable until the grace period ends), and the
// schedule must carry who set it and why, because a deletion without an author is
// indistinguishable from a bug.

import (
	"context"
	"testing"
	"time"
)

func TestWriteDeletionClosureAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		userID  = "940000000000000001"
		adminID = "940000000000000002"
	)
	rowKey := bigintSeg(userID)
	seed := func(extra map[string]any) {
		row := map[string]any{
			"user_id":       kvBigintJSON(userID),
			"username":      map[string]any{"__fluxer_type": "string", "value": "closing_user"},
			"flags":         map[string]any{"__fluxer_type": "bigint", "value": "0"},
			"password_hash": "$argon2id$...",
		}
		for k, v := range extra {
			row[k] = v
		}
		if err := putKV(ctx, "users", rowKey, rowKey, row, ""); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, rowKey)
	})

	seed(nil)
	pendingAt, flags, err := writeDeletionClosure(ctx, adminID, userID, 14, deletionReasonInactive)
	if err != nil {
		t.Fatalf("closure: %v", err)
	}
	if want := userFlagSelfDeleted; flags != want {
		t.Fatalf("flags = %d, want SELF_DELETED only (%d)", flags, want)
	}
	if flags&userFlagDeleted != 0 {
		t.Fatal("the closure set DELETED, which would hide the account immediately")
	}
	if delta := pendingAt.Sub(time.Now().UTC()); delta < 13*24*time.Hour || delta > 15*24*time.Hour {
		t.Fatalf("pending_at is %v away, want ~14 days", delta)
	}

	row, err := readKV(ctx, "users", rowKey)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got := typedString(row["pending_deletion_at"]); got == "" {
		t.Fatal("pending_deletion_at was not written")
	}
	if got, _ := intFieldChecked(map[string]any{"v": row["deletion_reason_code"]}, "v"); got != deletionReasonInactive {
		t.Fatalf("deletion_reason_code = %d", got)
	}
	if got := typedString(row["deletion_scheduled_by"]); got != adminID {
		t.Fatalf("deletion_scheduled_by = %q", got)
	}

	// A second closure must be refused rather than silently rescheduling.
	if _, _, err := writeDeletionClosure(ctx, adminID, userID, 30, deletionReasonInactive); err == nil || !isRefusedFlagChange(err) {
		t.Fatalf("second closure returned %v, want a refusal", err)
	}

	// A holder request uses the user-requested reason code. The row is re-seeded
	// with an explicit null: putKV merges documents, so simply re-inserting would
	// keep the previous schedule.
	seed(map[string]any{"pending_deletion_at": nil})
	if _, _, err := writeDeletionClosure(ctx, adminID, userID, 14, deletionReasonUserRequested); err != nil {
		t.Fatalf("holder request closure: %v", err)
	}
	row, _ = readKV(ctx, "users", rowKey)
	if got, _ := intFieldChecked(map[string]any{"v": row["deletion_reason_code"]}, "v"); got != deletionReasonUserRequested {
		t.Fatalf("holder request reason code = %d", got)
	}
}
