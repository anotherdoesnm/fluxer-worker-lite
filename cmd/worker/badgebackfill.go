package main

// taskBackfillHiddenProfileBadges ports BackfillHiddenProfileBadges.ts.
//
// Hiding a profile badge used to be recorded by bits inside the user's flags and
// premium flags. Those bits moved into their own list, `hidden_profile_badges`, and
// this one-shot task migrates accounts that still carry the old ones: the hidden
// kinds are merged into the list, the legacy bits are cleared from both bitfields,
// and the rest of the bits are left alone.
//
// The bit tables come from @fluxer/constants (the image ships the TypeScript and
// the bundle with the same constants): a legacy premium flag bit folds into the
// modern premium flag first, then the badge-hide bits are read from the folded
// value, which is why an account with both old and new bits ends up with the same
// list either way.
//
// Idempotent: an account with no legacy bits is skipped, and the completion marker
// stops the task from being queued again.

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

const (
	legacyStaffHidden    uint64 = 1 << 57
	badgeBackfillDoneKey        = "backfill:hidden_profile_badges:done"
)

// Premium flag bits (PremiumFlags), which live in their own column.
const (
	premiumDiscriminator            = 1 << 0
	premiumBadgeMasked              = 1 << 2
	premiumBadgeTimestampHidden     = 1 << 3
	premiumPerksSanitized           = 1 << 5
	premiumPurchaseDisabled         = 1 << 6
	premiumEnabledOverride          = 1 << 7
	premiumPerksDisabled            = 1 << 8
	premiumOperatorBadgeTsHidden    = 1 << 11
	premiumOperatorBadgeCountHidden = 1 << 12
)

// Badge-hide bits inside the premium flags.
const (
	legacyPremiumBadgeHidden  = 1 << 1
	legacyPremiumSeqHidden    = 1 << 4
	legacyOperatorBadgeHidden = 1 << 9
)

// premiumProfileBadgeKinds are the badges the premium bit hides.
var premiumProfileBadgeKinds = []string{"plutonium", "visionary", "premium"}

// legacyPremiumFlagBitsToNew folds a legacy user-flag bit into its modern premium
// flag bit.
var legacyPremiumFlagBitsToNew = [][2]uint64{
	{1 << 37, premiumDiscriminator},
	{1 << 40, legacyPremiumBadgeHidden},
	{1 << 41, premiumBadgeMasked},
	{1 << 42, premiumBadgeTimestampHidden},
	{1 << 43, legacyPremiumSeqHidden},
	{1 << 44, premiumPerksSanitized},
	{1 << 45, premiumPurchaseDisabled},
	{1 << 46, premiumEnabledOverride},
}

const legacyDeadUserFlagsMask uint64 = 1<<35 | 1<<52 | 1<<54 | 1<<55 | 1<<56 | 1<<58 | 1<<61

func taskBackfillHiddenProfileBadges(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("backfillHiddenProfileBadges: %w", err)
		}
	}
	users, err := allUserRows(ctx)
	if err != nil {
		return fmt.Errorf("backfillHiddenProfileBadges: scan: %w", err)
	}
	migrated, failed := 0, 0
	for _, user := range users {
		userID := typedString(user["user_id"])
		if userID == "" {
			continue
		}
		ok, err := migrateBadgeHideBits(ctx, userID, user)
		if err != nil {
			failed++
			log.Printf("backfillHiddenProfileBadges: %s: %v", userID, err)
			continue
		}
		if ok {
			migrated++
		}
	}
	if failed > 0 {
		// No marker on a partial run: the next one only touches what is left.
		return fmt.Errorf("backfillHiddenProfileBadges: %d users could not be migrated", failed)
	}
	if err := valkey().Set(ctx, badgeBackfillDoneKey, time.Now().UTC().Format(time.RFC3339), 0).Err(); err != nil {
		return fmt.Errorf("backfillHiddenProfileBadges: completion marker: %w", err)
	}
	log.Printf("backfillHiddenProfileBadges: users=%d migrated=%d", len(users), migrated)
	return nil
}

// migrateBadgeHideBits moves one account's legacy bits. A false result means there
// was nothing to move.
func migrateBadgeHideBits(ctx context.Context, userID string, user map[string]any) (bool, error) {
	flags := userFlags(user)
	premiumFlags, _ := intFieldChecked(map[string]any{"v": user["premium_flags"]}, "v")
	hidden := legacyHiddenProfileBadges(flags, premiumFlags)
	if len(hidden) == 0 {
		return false, nil
	}
	clearedFlags, clearedPremium := clearLegacyBadgeHideBits(flags, premiumFlags)

	merged := stringSet(user["hidden_profile_badges"])
	for _, kind := range hidden {
		if !containsString(merged, kind) {
			merged = append(merged, kind)
		}
	}
	var hiddenValue any
	if len(merged) > 0 {
		hiddenValue = map[string]any{"__fluxer_type": "set", "value": stringSliceToAny(merged)}
	}
	rowKey := bigintSeg(userID)
	if err := patchRowField(ctx, "users", rowKey, "flags", map[string]any{
		"__fluxer_type": "bigint", "value": strconv.FormatUint(clearedFlags, 10),
	}); err != nil {
		return false, err
	}
	if err := patchRowField(ctx, "users", rowKey, "premium_flags", clearedPremium); err != nil {
		return false, err
	}
	if err := patchRowField(ctx, "users", rowKey, "hidden_profile_badges", hiddenValue); err != nil {
		return false, err
	}
	return true, nil
}

// legacyHiddenProfileBadges is the api's function, bit for bit.
func legacyHiddenProfileBadges(flags uint64, premiumFlags int) []string {
	hidden := make([]string, 0, 4)
	if flags&legacyStaffHidden != 0 {
		hidden = append(hidden, "staff")
	}
	folded := uint64(premiumFlags) | extractPremiumFlagsFromLegacyUserFlags(flags)
	if folded&legacyPremiumBadgeHidden != 0 {
		hidden = append(hidden, premiumProfileBadgeKinds...)
	}
	if folded&legacyPremiumSeqHidden != 0 {
		hidden = append(hidden, "visionary_number")
	}
	if folded&legacyOperatorBadgeHidden != 0 {
		hidden = append(hidden, "operator_pass")
	}
	return uniqueStrings(hidden)
}

// extractPremiumFlagsFromLegacyUserFlags folds legacy user-flag bits into the
// premium bitfield.
func extractPremiumFlagsFromLegacyUserFlags(flags uint64) uint64 {
	var out uint64
	for _, pair := range legacyPremiumFlagBitsToNew {
		if flags&pair[0] != 0 {
			out |= pair[1]
		}
	}
	return out
}

// clearLegacyBadgeHideBits removes the legacy bits from both bitfields, keeping
// everything else — including the folded premium bits that are not about badges.
func clearLegacyBadgeHideBits(flags uint64, premiumFlags int) (uint64, int) {
	var legacyPremiumMask uint64
	for _, pair := range legacyPremiumFlagBitsToNew {
		legacyPremiumMask |= pair[0]
	}
	newFlags := flags &^ legacyPremiumMask &^ legacyDeadUserFlagsMask &^ legacyStaffHidden
	folded := uint64(premiumFlags) | extractPremiumFlagsFromLegacyUserFlags(flags)
	folded &^= legacyPremiumBadgeHidden | legacyPremiumSeqHidden | legacyOperatorBadgeHidden
	return newFlags, int(folded)
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
