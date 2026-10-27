package main

// Tests for the hidden-badge backfill.
//
// The migration has one property worth pinning above all: it must not touch bits it
// did not come to move. The api clears exactly three masks (legacy premium flags,
// dead-user flags, the staff-hidden bit) and folds the legacy premium bits into the
// premium column; everything else has to survive.

import (
	"context"
	"testing"
)

func TestLegacyHiddenProfileBadges(t *testing.T) {
	// Staff hidden, plus the premium "hide profile badges" legacy bit.
	flags := legacyStaffHidden | (1 << 40)
	got := legacyHiddenProfileBadges(flags, 0)
	for _, want := range []string{"staff", "plutonium", "visionary", "premium"} {
		if !containsString(got, want) {
			t.Fatalf("hidden = %v, missing %q", got, want)
		}
	}
	// Already-migrated account: nothing to report.
	if got := legacyHiddenProfileBadges(0, 0); len(got) != 0 {
		t.Fatalf("plain account reports %v", got)
	}
	// A modern premium bit in the premium column is read directly.
	if got := legacyHiddenProfileBadges(0, legacyOperatorBadgeHidden); !containsString(got, "operator_pass") {
		t.Fatalf("operator pass not found: %v", got)
	}
}

func TestClearLegacyBadgeHideBits(t *testing.T) {
	flags := legacyStaffHidden | (1 << 40) | (1 << 37) | (1 << 53) // + unrelated bits
	premium := legacyPremiumBadgeHidden | premiumPerksDisabled
	newFlags, newPremium := clearLegacyBadgeHideBits(flags, premium)

	if newFlags&legacyStaffHidden != 0 {
		t.Fatal("the staff-hidden bit survived")
	}
	if newFlags&(1<<40) != 0 || newFlags&(1<<37) != 0 {
		t.Fatal("legacy premium bits survived in the flags column")
	}
	if newFlags&(1<<53) == 0 {
		t.Fatal("an unrelated flag was cleared")
	}
	if newPremium&legacyPremiumBadgeHidden != 0 {
		t.Fatal("the badge-hide premium bit survived")
	}
	if newPremium&premiumPerksDisabled == 0 {
		t.Fatal("an unrelated premium bit was cleared")
	}
	// The discriminator legacy bit folds in and stays (it is not a badge-hide bit).
	if newPremium&premiumDiscriminator == 0 {
		t.Fatal("the folded discriminator bit was lost")
	}
}

func TestMigrateBadgeHideBitsAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const userID = "990000000000009001"
	rowKey := bigintSeg(userID)
	seed := func(flags uint64, premium int, hidden []any) {
		row := map[string]any{
			"user_id":       kvBigintJSON(userID),
			"username":      map[string]any{"__fluxer_type": "string", "value": "badge_user"},
			"flags":         map[string]any{"__fluxer_type": "bigint", "value": itoaU(flags)},
			"premium_flags": premium,
		}
		if hidden != nil {
			row["hidden_profile_badges"] = map[string]any{"__fluxer_type": "set", "value": hidden}
		}
		if err := putKV(ctx, "users", rowKey, rowKey, row, ""); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = 'users' AND row_key = $1`, rowKey)
	})

	seed(legacyStaffHidden|(1<<40), 0, []any{"partner"})
	row, _ := readKV(ctx, "users", rowKey)
	migrated, err := migrateBadgeHideBits(ctx, userID, row)
	if err != nil || !migrated {
		t.Fatalf("migrate: %v %v", migrated, err)
	}
	row, _ = readKV(ctx, "users", rowKey)
	hidden := stringSet(row["hidden_profile_badges"])
	for _, want := range []string{"partner", "staff", "plutonium", "visionary", "premium"} {
		if !containsString(hidden, want) {
			t.Fatalf("hidden = %v, missing %q", hidden, want)
		}
	}
	if userFlags(row)&legacyStaffHidden != 0 {
		t.Fatal("the staff-hidden bit survived in the row")
	}
	if pf, _ := intFieldChecked(map[string]any{"v": row["premium_flags"]}, "v"); pf != 0 {
		t.Fatalf("premium_flags = %d, want 0", pf)
	}

	// A second run has nothing to do.
	row, _ = readKV(ctx, "users", rowKey)
	if migrated, err := migrateBadgeHideBits(ctx, userID, row); err != nil || migrated {
		t.Fatalf("second run: %v %v", migrated, err)
	}
}

func itoaU(v uint64) string {
	if v == 0 {
		return "0"
	}
	out := ""
	for v > 0 {
		out = string(rune('0'+v%10)) + out
		v /= 10
	}
	return out
}
