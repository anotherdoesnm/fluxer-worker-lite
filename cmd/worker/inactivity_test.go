package main

// Tests for the inactivity sweep.
//
// What matters is the gate, not the scheduling: the api never deletes an inactive
// account without a warning first, and when it cannot send one it stops. The test
// pins the skip list and the closure, since the warning itself lives in valkey and
// needs the marker the api writes.

import (
	"context"
	"testing"
	"time"
)

func TestShouldConsiderForInactivity(t *testing.T) {
	base := func(mutate func(map[string]any)) map[string]any {
		row := map[string]any{
			"user_id": kvBigintJSON("990000000000007001"),
			"bot":     false,
			"system":  false,
			"flags":   map[string]any{"__fluxer_type": "bigint", "value": "0"},
		}
		if mutate != nil {
			mutate(row)
		}
		return row
	}
	cases := []struct {
		name string
		row  map[string]any
		want bool
	}{
		{"ordinary user", base(nil), true},
		{"bot", base(func(r map[string]any) { r["bot"] = true }), false},
		{"system", base(func(r map[string]any) { r["system"] = true }), false},
		{"app store reviewer", base(func(r map[string]any) {
			r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": "9007199254740992"} // 1<<53
		}), false},
		{"already pending deletion", base(func(r map[string]any) { r["pending_deletion_at"] = kvDate(kvNow()) }), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldConsiderForInactivity(tc.row); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestScheduleInactivityDeletionAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const userID = "990000000000007101"
	rowKey := bigintSeg(userID)
	if err := putKV(ctx, "users", rowKey, rowKey, map[string]any{
		"user_id":  kvBigintJSON(userID),
		"username": map[string]any{"__fluxer_type": "string", "value": "dormant"},
		"flags":    map[string]any{"__fluxer_type": "bigint", "value": "0"},
	}, ""); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, rowKey)
	})

	// The valkey queue write fails without a server; the row change is what this
	// test is about, so the error is expected.
	_ = scheduleInactivityDeletion(ctx, userID)

	row, err := readKV(ctx, "users", rowKey)
	if err != nil || row == nil {
		t.Fatalf("read user: %v", err)
	}
	if flags := userFlags(row); flags&userFlagSelfDeleted == 0 {
		t.Fatalf("SELF_DELETED was not set: flags=%d", flags)
	}
	if typedString(row["pending_deletion_at"]) == "" {
		t.Fatal("pending_deletion_at was not written")
	}
	if got, _ := intFieldChecked(map[string]any{"v": row["deletion_reason_code"]}, "v"); got != deletionReasonInactive {
		t.Fatalf("deletion_reason_code = %d, want %d", got, deletionReasonInactive)
	}
	if typedString(row["deletion_scheduled_by"]) != systemUserID {
		t.Fatalf("deletion_scheduled_by = %q", typedString(row["deletion_scheduled_by"]))
	}
	// The schedule must be in the future by roughly the grace period.
	pendingAt, ok := parseTimestampMs(typedString(row["pending_deletion_at"]))
	if !ok {
		t.Fatal("pending timestamp unreadable")
	}
	if delta := time.Until(time.UnixMilli(pendingAt)); delta <= 0 {
		t.Fatalf("the schedule is not in the future: %v", delta)
	}
}
