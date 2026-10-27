package main

// Tests for the guild response mapper.
//
// The interesting part is not that id and name come through — it is the feature
// gating: the same row must produce a different icon/banner/splash depending on
// the features set, and must never produce a null features array.

import "testing"

func guildRow(features ...string) map[string]any {
	values := make([]any, 0, len(features))
	for _, f := range features {
		values = append(values, f)
	}
	return map[string]any{
		"guild_id":               kvBigintJSON("1563469298515574784"),
		"name":                   map[string]any{"__fluxer_type": "string", "value": "1GB RAM Fluxer Testing"},
		"owner_id":               kvBigintJSON("1563468490076061696"),
		"system_channel_id":      kvBigintJSON("1563469298515574787"),
		"features":               map[string]any{"__fluxer_type": "set", "value": values},
		"icon_hash":              map[string]any{"__fluxer_type": "string", "value": "a_iconhash"},
		"banner_hash":            map[string]any{"__fluxer_type": "string", "value": "a_bannerhash"},
		"banner_width":           960,
		"banner_height":          540,
		"splash_hash":            map[string]any{"__fluxer_type": "string", "value": "splashhash"},
		"splash_width":           1920,
		"embed_splash_hash":      map[string]any{"__fluxer_type": "string", "value": "embedsplash"},
		"nsfw":                   false,
		"afk_timeout":            300,
		"verification_level":     0,
		"mfa_level":              0,
		"content_warning_level":  nil,
		"message_history_cutoff": nil,
	}
}

func TestGuildResponseStripsUngatedAssets(t *testing.T) {
	// No entitlements at all: the banner and splash disappear, and the animated
	// icon is downgraded to its static hash instead of vanishing.
	resp := guildResponse(guildRow("BANNER_MISSING"))
	if got := resp["banner"]; got != nil {
		t.Fatalf("banner without the BANNER feature: %#v", got)
	}
	if got := resp["banner_width"]; got != nil {
		t.Fatalf("banner width without a banner: %#v", got)
	}
	if got := resp["splash"]; got != nil {
		t.Fatalf("splash without INVITE_SPLASH: %#v", got)
	}
	if got := resp["icon"]; got != "iconhash" {
		t.Fatalf("animated icon without ANIMATED_ICON: %#v", got)
	}
	if got, ok := resp["features"].([]string); !ok || len(got) != 1 || got[0] != "BANNER_MISSING" {
		t.Fatalf("features: %#v", resp["features"])
	}
}

func TestGuildResponseKeepsGatedAssets(t *testing.T) {
	resp := guildResponse(guildRow("ANIMATED_ICON", "ANIMATED_BANNER", "BANNER", "INVITE_SPLASH"))
	if got := resp["icon"]; got != "a_iconhash" {
		t.Fatalf("entitled animated icon was stripped: %#v", got)
	}
	if got := resp["banner"]; got != "a_bannerhash" {
		t.Fatalf("banner: %#v", got)
	}
	if got := resp["banner_width"]; got != 960 {
		t.Fatalf("banner width: %#v", got)
	}
	if got := resp["splash"]; got != "splashhash" {
		t.Fatalf("splash: %#v", got)
	}
	if got := resp["embed_splash"]; got != "embedsplash" {
		t.Fatalf("embed splash: %#v", got)
	}
	// Without ANIMATED_BANNER the banner is still shown, just not animated.
	plain := guildResponse(guildRow("BANNER"))
	if got := plain["banner"]; got != "bannerhash" {
		t.Fatalf("banner animation prefix: %#v", got)
	}
}

func TestGuildResponseAlwaysHasFeaturesArray(t *testing.T) {
	resp := guildResponse(map[string]any{
		"guild_id": kvBigintJSON("1"),
		"name":     map[string]any{"__fluxer_type": "string", "value": "no features"},
	})
	features, ok := resp["features"].([]string)
	if !ok {
		t.Fatalf("features is %T, want []string", resp["features"])
	}
	if len(features) != 0 {
		t.Fatalf("features: %v", features)
	}
	if resp["id"] != "1" || resp["name"] != "no features" {
		t.Fatalf("identity lost: %#v", resp)
	}
}
