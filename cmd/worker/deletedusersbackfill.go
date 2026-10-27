package main

// taskBackfillDeletedUsers ports BackfillDeletedUsers.ts.
//
// An account deletion has two halves: marking the row deleted and then scrubbing the
// personal fields off it. If a release ever marked accounts without scrubbing (or a
// crash landed between the halves), the rows keep emails, avatars, bios and premium
// state that the person asked to have removed. This one-shot task finds those rows —
// DELETED set, no pending deletion — and clears exactly the fields the api clears,
// leaving the anonymised identity alone because the deletion flow already wrote it.
//
// The api also deletes secondary indices and releases the email; both are documented
// gaps here (the index set lives in the api's user repository, and email release
// needs the email-ownership table this worker does not touch).

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

const deletedUsersBackfillDoneKey = "backfill:deleted_users:done"

// personalFieldsToClear mirrors FIELDS_CLEARED_ON_DELETION: the fields a deleted
// account must not keep.
var personalFieldsToClear = []string{
	"email", "email_bounced", "password_hash", "password_last_changed_at", "totp_secret",
	"authenticator_types", "avatar_hash", "avatar_color", "banner_hash", "banner_color",
	"bio", "pronouns", "accent_color", "timezone", "date_of_birth", "locale",
	"premium_flags", "hidden_profile_badges", "premium_type", "premium_since", "premium_until",
	"premium_gift_extension_ends_at", "premium_will_cancel", "premium_billing_cycle",
	"premium_lifetime_sequence", "premium_grace_ends_at", "stripe_subscription_id",
	"stripe_customer_id", "has_ever_purchased", "terms_agreed_at", "privacy_agreed_at",
	"last_active_at", "last_active_ip", "acls", "traits", "first_refund_at",
	"gift_inventory_server_seq", "gift_inventory_client_seq", "premium_onboarding_dismissed_at",
	"mention_flags", "last_voice_activity_sharing_change_at",
}

func taskBackfillDeletedUsers(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("backfillDeletedUsers: %w", err)
		}
	}
	users, err := allUserRows(ctx)
	if err != nil {
		return fmt.Errorf("backfillDeletedUsers: scan: %w", err)
	}
	deleted, scrubbed, failed := 0, 0, 0
	for _, user := range users {
		if !isDeletionCompleteRow(user) {
			continue
		}
		deleted++
		userID := typedString(user["user_id"])
		cleared, err := scrubDeletedUserRow(ctx, userID, user)
		if err != nil {
			failed++
			log.Printf("backfillDeletedUsers: %s: %v", userID, err)
			continue
		}
		if cleared {
			scrubbed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("backfillDeletedUsers: %d users could not be scrubbed", failed)
	}
	if err := valkey().Set(ctx, deletedUsersBackfillDoneKey, time.Now().UTC().Format(time.RFC3339), 0).Err(); err != nil {
		return fmt.Errorf("backfillDeletedUsers: completion marker: %w", err)
	}
	log.Printf("backfillDeletedUsers: deleted=%d scrubbed=%d", deleted, scrubbed)
	return nil
}

// isDeletionCompleteRow is isDeletionComplete: the DELETED flag without a pending
// deletion, which is what the api leaves behind after the flow finished (or failed).
func isDeletionCompleteRow(user map[string]any) bool {
	return userFlags(user)&userFlagDeleted != 0 && typedString(user["pending_deletion_at"]) == ""
}

// scrubDeletedUserRow nulls the personal fields that still carry a value. Returns
// whether anything was cleared, which is what the summary counts.
func scrubDeletedUserRow(ctx context.Context, userID string, user map[string]any) (bool, error) {
	present := make([]string, 0, len(personalFieldsToClear))
	for _, field := range personalFieldsToClear {
		if _, ok := user[field]; !ok {
			continue
		}
		if isNullOrEmptyValue(user[field]) {
			continue
		}
		present = append(present, field)
	}
	if len(present) == 0 {
		return false, nil
	}
	q := fmt.Sprintf(`
UPDATE fluxer_kv SET row_data = row_data || jsonb_build_object(%s), updated_at = now()
WHERE table_name = 'users' AND row_key = $1`, strings.Join(nullPatchPairs(present), ", "))
	if _, err := db.ExecContext(ctx, q, bigintSeg(userID)); err != nil {
		return false, err
	}
	return true, nil
}

// nullPatchPairs builds the jsonb_build_object arguments for a list of fields.
func nullPatchPairs(fields []string) []string {
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		out = append(out, "'"+strings.ReplaceAll(field, "'", "")+"', NULL")
	}
	return out
}

// isNullOrEmptyValue treats an absent, null or empty collection as "nothing to clear".
func isNullOrEmptyValue(v any) bool {
	switch value := v.(type) {
	case nil:
		return true
	case map[string]any:
		if list, ok := value["value"].([]any); ok {
			return len(list) == 0
		}
		return false
	case []any:
		return len(value) == 0
	case string:
		return value == ""
	}
	return false
}
