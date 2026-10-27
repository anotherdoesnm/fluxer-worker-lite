package main

// The private user object: what a person's own clients receive on USER_UPDATE.
//
// It is the own-partial response plus everything the owner may see — email,
// ACLs, traits, premium state, MFA, account tier, pending deletion. Upstream
// derives several of those from instance policy (identity mode, verification
// basis, NSFW policy), which lives in instance_configuration and is not read by
// this worker. Those fields are approximated here in the narrowest way that keeps
// the object well-formed, and each approximation is named in OMISSIONS.md:
//
//	account_tier      verified when the account has a confirmed email or a system
//	                  account, unverified with an email or a password, else
//	                  unclaimed
//	verification_need null for verified accounts, otherwise the most likely next
//	                  step for the tier
//	nsfw_allowed      the row's stored value when present, true otherwise (this
//	                  instance's policy is permissive)
//	profile_restricted / date_of_birth_required   false: neither is enforced here
//
// Everything else is read straight from the row.

import (
	"sort"
	"time"
)

// userPrivateResponse renders the owner's own view of their account.
func userPrivateResponse(row map[string]any, now time.Time) (map[string]any, bool) {
	if row == nil {
		return nil, false
	}
	out := userOwnPartialResponse(row)
	flags := userFlags(row)
	isBot, _ := row["bot"].(bool)
	isSystem, _ := row["system"].(bool)

	traits := stringSet(row["traits"])
	sort.Strings(traits)
	if len(traits) > 0 {
		out["traits"] = traits
	} else {
		out["traits"] = []string{}
	}
	out["is_staff"] = flags&userFlagStaff != 0
	out["acls"] = stringSet(row["acls"])
	out["email"] = nullableString(typedString(row["email"]))
	out["email_bounced"] = boolField(row, "email_bounced")
	out["has_verified_phone"] = false
	out["bio"] = nullableString(typedString(row["bio"]))
	out["pronouns"] = nullableString(typedString(row["pronouns"]))
	out["accent_color"] = nullableInt(row["accent_color"])
	out["timezone"] = nullableString(typedString(row["timezone"]))
	out["timezone_privacy_flags"] = intOrZero(row["timezone_privacy_flags"])
	out["banner"] = nullableString(stripAnimationPrefixUnless(typedString(row["banner_hash"]), avatarAnimationEntitled(row)))
	out["banner_color"] = nullableString(typedString(row["banner_color"]))

	authenticators := activeAuthenticatorTypes(row)
	out["authenticator_types"] = authenticators
	out["mfa_enabled"] = len(authenticators) > 0
	out["verified"] = boolField(row, "email_verified")

	premium := premiumActive(row, now)
	out["premium_type"] = premiumType(row, premium)
	out["premium_since"] = premiumSince(row, premium)
	out["premium_until"] = premiumUntil(row, premium)
	out["premium_will_cancel"] = boolField(row, "premium_will_cancel")
	out["premium_billing_cycle"] = nullableString(typedString(row["premium_billing_cycle"]))
	out["premium_lifetime_sequence"] = nullableInt(row["premium_lifetime_sequence"])
	out["premium_grace_ends_at"] = nullableString(typedString(row["premium_grace_ends_at"]))
	for _, key := range []string{
		"premium_discriminator", "premium_badge_hidden", "premium_badge_masked",
		"premium_badge_timestamp_hidden", "premium_badge_sequence_hidden",
		"premium_purchase_disabled", "premium_enabled_override", "premium_perks_disabled",
	} {
		out[key] = premiumFlag(row, key)
	}
	out["password_last_changed_at"] = nullableString(typedString(row["password_last_changed_at"]))
	out["last_voice_activity_sharing_change_at"] = nullableString(typedString(row["last_voice_activity_sharing_change_at"]))
	out["required_actions"] = []string{}
	out["account_limited"] = flags&userFlagAccountLimited != 0
	out["nsfw_allowed"] = nsfwAllowed(row)
	out["has_dismissed_premium_onboarding"] = premium && typedString(row["premium_onboarding_dismissed_at"]) != ""
	out["has_ever_purchased"] = boolField(row, "has_ever_purchased")
	serverSeq, hasServer := intFieldChecked(map[string]any{"v": row["gift_inventory_server_seq"]}, "v")
	clientSeq, _ := intFieldChecked(map[string]any{"v": row["gift_inventory_client_seq"]}, "v")
	out["has_unread_gift_inventory"] = hasServer && (clientSeq == 0 || clientSeq < serverSeq)
	out["unread_gift_inventory_count"] = unreadGiftCount(hasServer, serverSeq, clientSeq)
	if flags&(1<<60) != 0 { // AGE_VERIFIED_ADULT
		out["age_verified_adult"] = true
	}
	out["terms_agreed_at"] = nullableString(typedString(row["terms_agreed_at"]))
	out["privacy_agreed_at"] = nullableString(typedString(row["privacy_agreed_at"]))
	tier := accountTier(row, flags, isBot, isSystem)
	out["claimed"] = tier != "unclaimed"
	out["account_tier"] = tier
	out["verification_need"] = verificationNeed(tier, row)
	out["has_password"] = typedString(row["password_hash"]) != ""
	out["date_of_birth_required"] = false
	out["profile_restricted"] = false
	if pending, ok := pendingBulkDeletion(row); ok {
		out["pending_bulk_message_deletion"] = pending
	} else {
		out["pending_bulk_message_deletion"] = nil
	}
	return out, true
}

// premiumActive is the api's isPremium(): a paid tier that has not expired.
func premiumActive(row map[string]any, now time.Time) bool {
	if premiumType(row, true) == 0 {
		return false
	}
	until := typedString(row["premium_until"])
	if until == "" {
		return true
	}
	ms, ok := parseTimestampMs(until)
	return !ok || ms > now.UnixMilli()
}

func premiumType(row map[string]any, active bool) int {
	if !active {
		return 0
	}
	n, _ := intFieldChecked(map[string]any{"v": row["premium_type"]}, "v")
	return n
}

func premiumSince(row map[string]any, active bool) any {
	if !active {
		return nil
	}
	return nullableString(typedString(row["premium_since"]))
}

func premiumUntil(row map[string]any, active bool) any {
	if !active {
		return nil
	}
	return nullableString(typedString(row["premium_until"]))
}

// premiumFlag reads the bitfield of premium flags; absent means false.
func premiumFlag(row map[string]any, name string) bool {
	n, _ := intFieldChecked(map[string]any{"v": row["premium_flags"]}, "v")
	bit, ok := premiumFlagBits[name]
	if !ok || n == 0 {
		return false
	}
	return n&bit != 0
}

// Bit positions from PremiumFlags; only the ones the response exposes.
var premiumFlagBits = map[string]int{
	"premium_discriminator":          1 << 0,
	"premium_badge_hidden":           1 << 1,
	"premium_badge_masked":           1 << 2,
	"premium_badge_timestamp_hidden": 1 << 3,
	"premium_badge_sequence_hidden":  1 << 4,
	"premium_purchase_disabled":      1 << 5,
	"premium_enabled_override":       1 << 6,
	"premium_perks_disabled":         1 << 7,
}

// activeAuthenticatorTypes keeps only TOTP and WebAuthn, as the api does: other
// authenticator types are stored but not shown as MFA.
func activeAuthenticatorTypes(row map[string]any) []string {
	all := stringSet(row["authenticator_types"])
	out := make([]string, 0, len(all))
	for _, t := range all {
		if t == "totp" || t == "webauthn" {
			out = append(out, t)
		}
	}
	if out == nil {
		return []string{}
	}
	return out
}

func nsfwAllowed(row map[string]any) bool {
	if v, ok := row["nsfw_allowed"].(bool); ok {
		return v
	}
	return true
}

func unreadGiftCount(hasServer bool, server, client int) int {
	if !hasServer {
		return 0
	}
	if client >= server {
		return 0
	}
	return server - client
}

func accountTier(row map[string]any, flags uint64, isBot, isSystem bool) string {
	if isBot || isSystem {
		return "verified"
	}
	hasEmail := typedString(row["email"]) != ""
	if hasEmail && boolField(row, "email_verified") {
		return "verified"
	}
	if hasEmail || typedString(row["password_hash"]) != "" {
		return "unverified"
	}
	return "unclaimed"
}

func verificationNeed(tier string, row map[string]any) any {
	switch tier {
	case "verified":
		return nil
	case "unverified":
		if typedString(row["email"]) != "" && !boolField(row, "email_verified") {
			return "confirm_email"
		}
		return "add_email"
	default:
		return "add_email"
	}
}

// pendingBulkDeletion renders the pending bulk message deletion block the owner
// sees while their request is queued.
func pendingBulkDeletion(row map[string]any) (map[string]any, bool) {
	at := typedString(row["pending_bulk_message_deletion_at"])
	if at == "" {
		return nil, false
	}
	channels, _ := intFieldChecked(map[string]any{"v": row["pending_bulk_message_deletion_channel_count"]}, "v")
	messages, _ := intFieldChecked(map[string]any{"v": row["pending_bulk_message_deletion_message_count"]}, "v")
	return map[string]any{
		"scheduled_at":  at,
		"channel_count": channels,
		"message_count": messages,
	}, true
}
