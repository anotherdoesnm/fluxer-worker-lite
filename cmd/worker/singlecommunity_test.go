package main

// Tests for the single-community join run.
//
// The eligibility rule is the api's list of "not this one" cases, and the paging
// cursor is the part that has to be right for a run to finish instead of looping
// on the same page forever.

import (
	"context"
	"testing"
	"time"
)

func kvNow() time.Time { return time.Now().UTC() }

func TestCanJoinSingleCommunity(t *testing.T) {
	base := func(mutate func(map[string]any)) map[string]any {
		row := map[string]any{
			"user_id": kvBigintJSON("910000000000000001"),
			"bot":     false,
			"system":  false,
			"flags":   map[string]any{"__fluxer_type": "bigint", "value": "0"},
			"traits":  map[string]any{"__fluxer_type": "set", "value": []any{}},
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
		{"deleted flag", base(func(r map[string]any) {
			r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": "17179869184"} // 1<<34
		}), false},
		{"disabled flag", base(func(r map[string]any) {
			r["flags"] = map[string]any{"__fluxer_type": "bigint", "value": "274877906944"} // 1<<38
		}), false},
		{"pending deletion", base(func(r map[string]any) { r["pending_deletion_at"] = kvDate(kvNow()) }), false},
		{"awaiting approval", base(func(r map[string]any) {
			r["traits"] = map[string]any{"__fluxer_type": "set", "value": []any{"registration_pending_approval"}}
		}), false},
		{"rejected registration", base(func(r map[string]any) {
			r["traits"] = map[string]any{"__fluxer_type": "set", "value": []any{"registration_rejected"}}
		}), false},
		{"the system user itself", map[string]any{
			"user_id": kvBigintJSON(systemUserID),
			"flags":   map[string]any{"__fluxer_type": "bigint", "value": "0"},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canJoinSingleCommunity(tc.row); got != tc.want {
				t.Fatalf("canJoin = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUsersPageAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	ids := []string{"910000000000000101", "910000000000000102", "910000000000000103"}
	for _, id := range ids {
		key := bigintSeg(id)
		if err := putKV(ctx, "users", key, key, map[string]any{
			"user_id":  kvBigintJSON(id),
			"username": map[string]any{"__fluxer_type": "string", "value": "u" + id},
			"flags":    map[string]any{"__fluxer_type": "bigint", "value": "0"},
		}, ""); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	t.Cleanup(func() {
		for _, id := range ids {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, bigintSeg(id))
		}
	})

	first, cursor, err := usersPage(ctx, "", 2)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(first) != 2 || cursor == "" {
		t.Fatalf("page 1 = %d rows, cursor %q", len(first), cursor)
	}
	second, cursor2, err := usersPage(ctx, cursor, 2)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("page 2 = %d rows, want 1", len(second))
	}
	if cursor2 != "" {
		t.Fatalf("a short page must end the run, cursor = %q", cursor2)
	}
	// Pages must not overlap.
	if typedString(first[1]["user_id"]) == typedString(second[0]["user_id"]) {
		t.Fatal("the cursor repeated a user")
	}
}
