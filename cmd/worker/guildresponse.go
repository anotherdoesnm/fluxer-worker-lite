package main

// Guild rows become GUILD_UPDATE payloads here.
//
// Ported from GuildModel.ts: the response is the guild's public shape, and three
// of its assets are gated by features — an animated icon needs ANIMATED_ICON, a
// banner needs BANNER (and its animation needs ANIMATED_BANNER), a splash needs
// INVITE_SPLASH. The api also *strips* the animation prefix instead of dropping
// the asset, so a guild that loses its entitlement keeps a static-looking hash
// rather than a broken image.
//
// This mapper existed in the api and not in the worker, which is why several
// tasks (lifting an invite pause, changing features in bulk) could patch rows but
// had nothing to send to clients. Sending a partial object would be worse than
// sending nothing: clients cache guilds.

import "strings"

const animatedHashPrefix = "a_"

// guildResponse maps a guild row to the object clients receive.
func guildResponse(row map[string]any) map[string]any {
	features := stringSet(row["features"])
	has := func(name string) bool { return containsString(features, name) }

	icon := stripAnimationPrefixUnless(typedString(row["icon_hash"]), has("ANIMATED_ICON"))
	banner := typedString(row["banner_hash"])
	if !has("BANNER") {
		banner = ""
	} else {
		banner = stripAnimationPrefixUnless(banner, has("ANIMATED_BANNER"))
	}
	splash := typedString(row["splash_hash"])
	if !has("INVITE_SPLASH") {
		splash = ""
	}
	embedSplash := typedString(row["embed_splash_hash"])
	if !has("INVITE_SPLASH") {
		embedSplash = ""
	}

	out := map[string]any{
		"id":                    typedString(row["guild_id"]),
		"name":                  typedString(row["name"]),
		"icon":                  nullableString(icon),
		"banner":                nullableString(banner),
		"banner_width":          widthIf(banner, row["banner_width"]),
		"banner_height":         widthIf(banner, row["banner_height"]),
		"splash":                nullableString(splash),
		"splash_width":          widthIf(splash, row["splash_width"]),
		"splash_height":         widthIf(splash, row["splash_height"]),
		"embed_splash":          nullableString(embedSplash),
		"embed_splash_width":    widthIf(embedSplash, row["embed_splash_width"]),
		"embed_splash_height":   widthIf(embedSplash, row["embed_splash_height"]),
		"splash_card_alignment": intOrZero(row["splash_card_alignment"]),
		"features":              featureList(features),

		"vanity_url_code":               nullableString(typedString(row["vanity_url_code"])),
		"owner_id":                      typedString(row["owner_id"]),
		"system_channel_id":             nullableString(typedString(row["system_channel_id"])),
		"system_channel_flags":          intOrZero(row["system_channel_flags"]),
		"rules_channel_id":              nullableString(typedString(row["rules_channel_id"])),
		"afk_channel_id":                nullableString(typedString(row["afk_channel_id"])),
		"afk_timeout":                   intOrZero(row["afk_timeout"]),
		"verification_level":            intOrZero(row["verification_level"]),
		"mfa_level":                     intOrZero(row["mfa_level"]),
		"nsfw_level":                    intOrZero(row["nsfw_level"]),
		"nsfw":                          boolValue(row["nsfw"]),
		"content_warning_level":         nullableInt(row["content_warning_level"]),
		"content_warning_text":          nullableString(typedString(row["content_warning_text"])),
		"explicit_content_filter":       intOrZero(row["explicit_content_filter"]),
		"default_message_notifications": intOrZero(row["default_message_notifications"]),
		"disabled_operations":           intOrZero(row["disabled_operations"]),
		"message_history_cutoff":        nullableString(typedString(row["message_history_cutoff"])),
	}
	return out
}

// stripAnimationPrefixUnless keeps an animated hash only when the guild is
// entitled to it; otherwise the "a_" prefix is dropped and the static variant is
// used.
func stripAnimationPrefixUnless(hash string, entitled bool) string {
	if hash == "" || entitled {
		return hash
	}
	return strings.TrimPrefix(hash, animatedHashPrefix)
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// widthIf mirrors the api's "dimension only makes sense with the asset" rule: a
// banner that is stripped for features loses its width and height too.
func widthIf(assetHash string, v any) any {
	if assetHash == "" {
		return nil
	}
	return nullableInt(v)
}

func nullableInt(v any) any {
	if v == nil {
		return nil
	}
	if n, ok := intFieldChecked(map[string]any{"v": v}, "v"); ok {
		return n
	}
	return nil
}

func intOrZero(v any) int {
	if n, ok := intFieldChecked(map[string]any{"v": v}, "v"); ok {
		return n
	}
	return 0
}

func boolValue(v any) bool {
	b, _ := v.(bool)
	return b
}

// featureList always serialises as an array, never null: clients iterate it.
func featureList(features []string) []string {
	if features == nil {
		return []string{}
	}
	return features
}
