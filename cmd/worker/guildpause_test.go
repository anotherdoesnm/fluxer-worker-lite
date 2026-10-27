package main

// Tests for lifting an expired guild invite pause.
//
// The cron needs valkey, which the test environment does not have, so the test
// drives the per-guild lift directly — that is where the decision lives: lift,
// wait, or forget.

import (
	"context"
	"testing"
	"time"
)

func TestStringSet(t *testing.T) {
	got := stringSet(map[string]any{
		"__fluxer_type": "set",
		"value":         []any{"INVITES_DISABLED", "BANNER"},
	})
	if len(got) != 2 || got[0] != "INVITES_DISABLED" || got[1] != "BANNER" {
		t.Fatalf("stringSet = %v", got)
	}
	if stringSet(nil) != nil {
		t.Fatal("nil column must yield nil")
	}
	if stringSet(map[string]any{"value": "not a list"}) != nil {
		t.Fatal("a non-list value must yield nil")
	}
	if !containsString(got, "BANNER") || containsString(got, "RAID_DETECTED") {
		t.Fatal("containsString is wrong")
	}
}

func TestLiftGuildInvitePauseAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const guildID = "840000000000000001"
	rowKey := bigintSeg(guildID)
	past := time.Now().Add(-5 * time.Minute).UTC()
	seed := map[string]any{
		"guild_id": kvBigintJSON(guildID),
		"name":     map[string]any{"__fluxer_type": "string", "value": "paused guild"},
		"features": map[string]any{
			"__fluxer_type": "set",
			"value":         []any{"INVITES_DISABLED", "RAID_DETECTED", "BANNER"},
		},
		"invites_paused_until": kvDate(past),
	}
	if err := putKV(ctx, guildsTable, rowKey, rowKey, seed, ""); err != nil {
		t.Fatalf("seed guild: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, guildsTable, rowKey)
	})

	// The dequeue at the end talks to valkey, which this environment does not
	// have: the row work is what is under test here, so the dequeue error is
	// expected and only the "lifted" verdict is checked.
	lifted, _ := liftGuildInvitePause(ctx, guildID, time.Now().UTC())
	if !lifted {
		t.Fatal("an expired pause was not lifted")
	}

	row, err := readKV(ctx, guildsTable, rowKey)
	if err != nil {
		t.Fatalf("read guild: %v", err)
	}
	features := stringSet(row["features"])
	if containsString(features, "INVITES_DISABLED") || containsString(features, "RAID_DETECTED") {
		t.Fatalf("pause features survived: %v", features)
	}
	if !containsString(features, "BANNER") {
		t.Fatalf("an unrelated feature was dropped: %v", features)
	}
	if got := typedString(row["invites_paused_until"]); got != "" {
		t.Fatalf("invites_paused_until was not cleared: %q", got)
	}
	if got := typedString(row["name"]); got != "paused guild" {
		t.Fatalf("the patch rewrote unrelated fields: %q", got)
	}
}

func TestLiftGuildInvitePauseKeepsAFutureDeadline(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const guildID = "840000000000000002"
	rowKey := bigintSeg(guildID)
	future := time.Now().Add(time.Hour).UTC()
	seed := map[string]any{
		"guild_id":             kvBigintJSON(guildID),
		"features":             map[string]any{"__fluxer_type": "set", "value": []any{"INVITES_DISABLED"}},
		"invites_paused_until": kvDate(future),
	}
	if err := putKV(ctx, guildsTable, rowKey, rowKey, seed, ""); err != nil {
		t.Fatalf("seed guild: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, guildsTable, rowKey)
	})

	// valkey is not reachable here, so a "reschedule" attempt fails; what the
	// test pins is that the row is not touched before the deadline.
	lifted, err := liftGuildInvitePause(ctx, guildID, time.Now().UTC())
	if lifted {
		t.Fatal("a pause that is still running was lifted")
	}
	row, err := readKV(ctx, guildsTable, rowKey)
	if err != nil {
		t.Fatalf("read guild: %v", err)
	}
	if !containsString(stringSet(row["features"]), "INVITES_DISABLED") {
		t.Fatal("the future pause lost its feature")
	}
	if typedString(row["invites_paused_until"]) == "" {
		t.Fatal("the future deadline was cleared")
	}
	_ = err
}
