package main

// The guild-member half of the gateway payloads: mapMemberFields from GuildModel.ts.
//
// A member object is the member's own fields plus the user partial, so a hidden
// user makes the whole object unrenderable and the caller skips the dispatch.

import "time"

// guildMemberResponse renders one member, or reports that the embedded user must
// not be shown.
func guildMemberResponse(member, user map[string]any, now time.Time) (map[string]any, bool) {
	if member == nil {
		return nil, false
	}
	userPartial, ok := userPartialResponse(user, now)
	if !ok {
		return nil, false
	}

	// A timed-out member is rendered as muted, which is what the clients show.
	isTimedOut := false
	if raw := typedString(member["communication_disabled_until"]); raw != "" {
		if ms, ok := parseTimestampMs(raw); ok && ms > now.UnixMilli() {
			isTimedOut = true
		}
	}
	premiumSanitized, _ := member["is_premium_sanitized"].(bool)
	mute, _ := member["mute"].(bool)
	deaf, _ := member["deaf"].(bool)

	out := map[string]any{
		"user":                         userPartial,
		"nick":                         nullableString(typedString(member["nick"])),
		"avatar":                       nil,
		"banner":                       nil,
		"accent_color":                 nullableInt(member["accent_color"]),
		"roles":                        stringSet(member["role_ids"]),
		"joined_at":                    nullableString(typedString(member["joined_at"])),
		"mute":                         mute || isTimedOut,
		"deaf":                         deaf,
		"communication_disabled_until": nullableString(typedString(member["communication_disabled_until"])),
	}
	if !premiumSanitized {
		out["avatar"] = nullableString(typedString(member["avatar_hash"]))
		out["banner"] = nullableString(typedString(member["banner_hash"]))
	}
	if roles, ok := out["roles"].([]string); ok && roles == nil {
		out["roles"] = []string{}
	}
	if pf, ok := intFieldChecked(map[string]any{"v": member["profile_flags"]}, "v"); ok && pf != 0 {
		out["profile_flags"] = pf
	}
	if mf, ok := intFieldChecked(map[string]any{"v": member["mention_flags"]}, "v"); ok && mf != 0 {
		out["mention_flags"] = mf
	}
	return out, true
}
