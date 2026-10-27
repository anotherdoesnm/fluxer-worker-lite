package main

// Tests for the guild feature sweep.
//
// The diff is pure and easy to get subtly wrong (an id listed on both sides must
// end up removed), so it has its own table test; the database half checks that a
// real row is rewritten and that a no-op change writes nothing.

import (
	"context"
	"testing"
)

func TestApplyFeatureDiff(t *testing.T) {
	cases := []struct {
		name   string
		before []string
		add    []string
		remove []string
		want   []string
	}{
		{"add only", []string{"BANNER"}, []string{"ANIMATED_BANNER"}, nil, []string{"BANNER", "ANIMATED_BANNER"}},
		{"remove only", []string{"BANNER", "INVITE_SPLASH"}, nil, []string{"BANNER"}, []string{"INVITE_SPLASH"}},
		{"both sides: removal wins", []string{"NEWS"}, []string{"NEWS"}, []string{"NEWS"}, []string{}},
		{"already present", []string{"BANNER"}, []string{"BANNER"}, nil, []string{"BANNER"}},
		{"duplicate input collapses", []string{"BANNER", "BANNER"}, nil, nil, []string{"BANNER"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := applyFeatureDiff(tc.before, tc.add, tc.remove)
			if !equalStringSets(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestApplyGuildFeatureChangeAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const guildID = "850000000000000001"
	rowKey := bigintSeg(guildID)
	if err := putKV(ctx, guildsTable, rowKey, rowKey, map[string]any{
		"guild_id": kvBigintJSON(guildID),
		"name":     map[string]any{"__fluxer_type": "string", "value": "sweep target"},
		"features": map[string]any{"__fluxer_type": "set", "value": []any{"BANNER"}},
	}, ""); err != nil {
		t.Fatalf("seed guild: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, guildsTable, rowKey)
	})

	changed, err := applyGuildFeatureChange(ctx, guildID, []string{"ANIMATED_BANNER"}, []string{"BANNER"})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !changed {
		t.Fatal("a real feature change was reported as no-op")
	}
	row, err := readKV(ctx, guildsTable, rowKey)
	if err != nil {
		t.Fatalf("read guild: %v", err)
	}
	features := stringSet(row["features"])
	if !equalStringSets(features, []string{"ANIMATED_BANNER"}) {
		t.Fatalf("features = %v", features)
	}
	if got := typedString(row["name"]); got != "sweep target" {
		t.Fatalf("the patch rewrote unrelated fields: %q", got)
	}

	// Applying the same diff again must be a no-op: nothing to write, nothing to
	// dispatch, nothing to audit.
	changed, err = applyGuildFeatureChange(ctx, guildID, []string{"ANIMATED_BANNER"}, []string{"BANNER"})
	if err != nil {
		t.Fatalf("apply again: %v", err)
	}
	if changed {
		t.Fatal("a repeated change was reported as applied")
	}
}
