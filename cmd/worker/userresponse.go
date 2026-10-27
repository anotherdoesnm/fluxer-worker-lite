package main

// The user half of the gateway payloads.
//
// Several worker tasks end by telling clients about a user — a member added, a
// profile patched, a role granted. Every one of those payloads embeds
// mapUserToPartialResponse, so this file ports it once, together with the rule
// that decides whether a user may be rendered at all.
//
// The rule is the part that matters for privacy. A user who set PROFILE_HIDDEN,
// or who is under enforcement (marked as spammer, temporarily banned, or pending
// an enforcement deletion), must not appear under their real name. Upstream
// replaces the name with a pseudonym derived by HMAC from a secret and a word
// list that lives in the api image. Neither is available to this worker, so a
// hidden user is *not rendered at all*: the caller skips the dispatch and says so
// in the log. Sending the real profile would be a leak, and inventing a
// pseudonym would be a lie that also breaks the "hidden" signal in the client.
//
// Known approximation: animated-avatar entitlement comes from the instance's
// limit configuration upstream and cannot be resolved here, so a bot keeps its
// animation and a human keeps it only with premium. Worst case a premium-enabled
// animated avatar is downgraded to its static hash — cosmetic, never a leak.

import (
	"strconv"
	"time"
)

// Bit positions from @fluxer/constants UserFlags. userFlagDeleted lives in
// deletionqueue.go, where the deletion state machine already needs it.
const (
	userFlagStaff             uint64 = 1 << 0
	userFlagPartner           uint64 = 1 << 2
	userFlagBugHunter         uint64 = 1 << 3
	userFlagFriendlyBot       uint64 = 1 << 4
	userFlagFriendlyBotManual uint64 = 1 << 5
	userFlagSpammer           uint64 = 1 << 6
	userFlagProfileHidden     uint64 = 1 << 7
	userFlagDisabled          uint64 = 1 << 38
	userFlagAccountLimited    uint64 = 1 << 50
	userFlagStaffHidden       uint64 = 1 << 57
)

// userFlags reads the typed bigint flags column.
func userFlags(row map[string]any) uint64 {
	if n, ok := intFieldChecked(map[string]any{"v": row["flags"]}, "v"); ok {
		if n < 0 {
			return 0
		}
		return uint64(n)
	}
	return 0
}

// isTempBanned mirrors isTemporarilyBanned: disabled with a ban expiry in the
// future.
func isTempBanned(row map[string]any, now time.Time) bool {
	if userFlags(row)&userFlagDisabled == 0 {
		return false
	}
	until := typedString(row["temp_banned_until"])
	if until == "" {
		return false
	}
	ms, ok := parseTimestampMs(until)
	return ok && ms > now.UnixMilli()
}

// isEnforcementDeletion mirrors isPendingEnforcementDeletion.
func isEnforcementDeletion(row map[string]any) bool {
	if typedString(row["pending_deletion_at"]) == "" {
		return false
	}
	code, ok := intFieldChecked(map[string]any{"v": row["deletion_reason_code"]}, "v")
	if !ok {
		return false
	}
	return code != deletionReasonUserRequested && code != deletionReasonOther
}

// isProfileHidden mirrors isProfileHidden. A system user is never hidden.
func isProfileHidden(row map[string]any, now time.Time) bool {
	if b, _ := row["system"].(bool); b {
		return false
	}
	flags := userFlags(row)
	if flags&userFlagProfileHidden != 0 {
		return true
	}
	return flags&userFlagSpammer != 0 || isTempBanned(row, now) || isEnforcementDeletion(row)
}

// isDeletedForDisplay matches the api's rule: deleted, not pending a deletion
// (anonymized in place), and not a system user.
func isDeletedForDisplay(row map[string]any) bool {
	flags := userFlags(row)
	if flags&userFlagDeleted == 0 {
		return false
	}
	if typedString(row["pending_deletion_at"]) != "" {
		return false
	}
	b, _ := row["system"].(bool)
	return !b
}

// userPartialResponse renders the object other people receive for a user, or
// reports that the user must not be rendered.
func userPartialResponse(row map[string]any, now time.Time) (map[string]any, bool) {
	if row == nil {
		return nil, false
	}
	if isProfileHidden(row, now) {
		return nil, false
	}
	return userOwnPartialResponse(row), true
}

// userOwnPartialResponse is the same object without the privacy mask: a person
// always sees their own profile, even when it is hidden from everybody else.
func userOwnPartialResponse(row map[string]any) map[string]any {
	isBot, _ := row["bot"].(bool)
	isSystem, _ := row["system"].(bool)
	flags := userFlags(row)

	if isDeletedForDisplay(row) {
		out := map[string]any{
			"id":            typedString(row["user_id"]),
			"username":      "DeletedUser",
			"discriminator": "0000",
			"global_name":   "Deleted User",
			"avatar":        nil,
			"avatar_color":  nil,
			"flags":         0,
		}
		if isBot {
			out["bot"] = true
		}
		if isSystem {
			out["system"] = true
		}
		return out
	}

	out := map[string]any{
		"id":            typedString(row["user_id"]),
		"username":      typedString(row["username"]),
		"discriminator": padDiscriminator(row["discriminator"]),
		"global_name":   nullableString(typedString(row["global_name"])),
		"avatar":        nullableString(stripAnimationPrefixUnless(typedString(row["avatar_hash"]), avatarAnimationEntitled(row))),
		"avatar_color":  nullableInt(row["avatar_color"]),
		"flags":         int(flags & publicUserFlags(flags)),
	}
	if isBot {
		out["bot"] = true
	}
	if isSystem {
		out["system"] = true
	}
	if mf, ok := intFieldChecked(map[string]any{"v": row["mention_flags"]}, "v"); ok && mf != 0 {
		out["mention_flags"] = mf
	}
	return out
}

// publicUserFlags is PUBLIC_USER_FLAGS, minus STAFF when the staff badge is
// hidden.
func publicUserFlags(flags uint64) uint64 {
	mask := userFlagStaff | userFlagPartner | userFlagBugHunter | userFlagFriendlyBot | userFlagFriendlyBotManual | userFlagSpammer
	if flags&userFlagStaffHidden != 0 {
		mask &^= userFlagStaff
	}
	return mask
}

// avatarAnimationEntitled is the approximation described in the file comment.
func avatarAnimationEntitled(row map[string]any) bool {
	if b, _ := row["bot"].(bool); b {
		return true
	}
	premium, _ := intFieldChecked(map[string]any{"v": row["premium_type"]}, "v")
	return premium > 0
}

// padDiscriminator renders the four-digit discriminator the api sends.
func padDiscriminator(v any) string {
	n, _ := intFieldChecked(map[string]any{"v": v}, "v")
	s := strconv.Itoa(n)
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}
