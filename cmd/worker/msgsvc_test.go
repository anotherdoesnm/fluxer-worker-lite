package main

// Tests for the service client.
//
// The payload is the part that can be checked here: it has to match the api's
// GetResponseById contract, and a missing field shows up as a rejected request (or,
// worse, a response with unsigned attachment URLs) rather than as a compile error.

import (
	"context"
	"testing"
)

func TestMessageResponsePayload(t *testing.T) {
	t.Setenv("FLUXER_MEDIA_ENDPOINT", "https://media.example")
	t.Setenv("FLUXER_INSTANCE_NAME", "lab")
	t.Setenv("FLUXER_MEDIA_PROXY_SECRET_KEY", "mk")
	t.Setenv("FLUXER_ATTACHMENT_URL_SECRET", "sk")

	cutoff := int64(1700000000000)
	payload := messageResponsePayload("viewer", "chan", "msg", messageResponseAccess{
		SourceGuildID:         "guild",
		MessageHistoryCutoff:  &cutoff,
		CanReadMessageHistory: true,
		IncludeHidden:         true,
	})

	want := map[string]any{
		"op":                           "GetResponseById",
		"channel_id":                   "chan",
		"message_id":                   "msg",
		"viewer_user_id":               "viewer",
		"source_guild_id":              "guild",
		"message_history_cutoff_ms":    cutoff,
		"can_read_message_history":     true,
		"include_hidden":               true,
		"media_endpoint":               "https://media.example",
		"media_proxy_secret_key":       "mk",
		"attachment_url_secret_base64": "sk",
		"instance_name":                "lab",
		"include_reactions":            true,
	}
	for key, expected := range want {
		if got := payload[key]; got != expected {
			t.Fatalf("%s = %#v, want %#v", key, got, expected)
		}
	}
	if len(payload) != len(want) {
		t.Fatalf("payload has %d keys, want %d: %#v", len(payload), len(want), payload)
	}
}

func TestMessageResponsePayloadWithoutGuild(t *testing.T) {
	payload := messageResponsePayload("v", "c", "m", messageResponseAccess{})
	if payload["source_guild_id"] != nil {
		t.Fatalf("source_guild_id = %#v, want nil for a DM", payload["source_guild_id"])
	}
	if payload["message_history_cutoff_ms"] != nil {
		t.Fatalf("cutoff = %#v, want nil", payload["message_history_cutoff_ms"])
	}
}

func TestSvcRequestWithoutNATS(t *testing.T) {
	// No connection in tests: the call must fail rather than return an empty object.
	prev := workerNC
	workerNC = nil
	defer func() { workerNC = prev }()
	if _, err := getMessageResponse(context.Background(), "v", "c", "m", messageResponseAccess{}); err == nil {
		t.Fatal("a missing connection did not fail the call")
	}
}
