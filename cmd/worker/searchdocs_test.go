package main

import (
	"reflect"
	"testing"
)

func TestMessageSearchDoc(t *testing.T) {
	msg := map[string]any{
		"message_id":       map[string]any{"value": "1564995185476108288", "__fluxer_type": "bigint"},
		"channel_id":       map[string]any{"value": "1564928035637952512", "__fluxer_type": "bigint"},
		"author_id":        map[string]any{"value": "1564721532968632320", "__fluxer_type": "bigint"},
		"content":          "look at https://example.com/page and https://docs.example.org/x.",
		"type":             0,
		"mention_everyone": true,
		"mentioned_user_ids": []any{
			map[string]any{"value": "42", "__fluxer_type": "bigint"},
			map[string]any{"value": "42", "__fluxer_type": "bigint"},
		},
		"edited_timestamp": map[string]any{"value": "2026-10-28T10:00:00.000Z", "__fluxer_type": "date"},
		"pinned_timestamp": map[string]any{"value": "2026-10-28T11:00:00.000Z", "__fluxer_type": "date"},
		"attachments": []any{map[string]any{
			"filename": "photo.PNG", "content_type": "image/png",
		}},
		"embeds": []any{map[string]any{
			"type":        "rich",
			"title":       "Hello",
			"description": "world",
			"url":         "https://embed.example.net/thing",
			"provider":    map[string]any{"name": "Example"},
			"fields": []any{
				map[string]any{"name": "How to", "value": "play"},
			},
		}},
	}

	doc := messageSearchDoc(msg, "999")

	if doc["id"] != "1564995185476108288" || doc["channelId"] != "1564928035637952512" {
		t.Fatalf("identity fields: %v", doc)
	}
	// The deviation from upstream: guildId comes from the channel row.
	if doc["guildId"] != "999" {
		t.Fatalf("guildId = %v, want 999", doc["guildId"])
	}
	if doc["authorType"] != "user" {
		t.Fatalf("authorType = %v", doc["authorType"])
	}
	if doc["isPinned"] != true {
		t.Fatalf("isPinned = %v", doc["isPinned"])
	}
	if doc["mentionEveryone"] != true {
		t.Fatalf("mentionEveryone = %v", doc["mentionEveryone"])
	}
	if doc["editedAt"] != int64(1793181600) {
		t.Fatalf("editedAt = %v", doc["editedAt"])
	}
	if doc["createdAt"] != snowflakeSeconds("1564995185476108288") {
		t.Fatalf("createdAt = %v", doc["createdAt"])
	}
	if doc["hasLink"] != true || doc["hasEmbed"] != true || doc["hasFile"] != true || doc["hasImage"] != true {
		t.Fatalf("flags: %v", doc)
	}
	if doc["hasVideo"] != false || doc["hasSound"] != false || doc["hasSticker"] != false || doc["hasForward"] != false {
		t.Fatalf("unexpected media flags: %v", doc)
	}
	if !reflect.DeepEqual(doc["embedTypes"], []string{"rich"}) {
		t.Fatalf("embedTypes = %v", doc["embedTypes"])
	}
	if !reflect.DeepEqual(doc["embedProviders"], []string{"Example"}) {
		t.Fatalf("embedProviders = %v", doc["embedProviders"])
	}
	if !reflect.DeepEqual(doc["attachmentExtensions"], []string{"png"}) {
		t.Fatalf("attachmentExtensions = %v", doc["attachmentExtensions"])
	}
	if !reflect.DeepEqual(doc["attachmentFilenames"], []string{"photo.PNG"}) {
		t.Fatalf("attachmentFilenames = %v", doc["attachmentFilenames"])
	}
	// Mentions are deduplicated, and both hosts are collected in order.
	if !reflect.DeepEqual(doc["mentionedUserIds"], []string{"42"}) {
		t.Fatalf("mentionedUserIds = %v", doc["mentionedUserIds"])
	}
	wantHosts := []string{"example.com", "docs.example.org", "embed.example.net"}
	if !reflect.DeepEqual(doc["linkHostnames"], wantHosts) {
		t.Fatalf("linkHostnames = %v, want %v", doc["linkHostnames"], wantHosts)
	}
	// The embed text is what message search matches on besides content.
	embedContent, _ := doc["embedContent"].([]string)
	for _, want := range []string{"Hello", "world", "How to", "play", "Example"} {
		if !containsString(embedContent, want) {
			t.Fatalf("embedContent %v is missing %q", embedContent, want)
		}
	}
}

func TestMessageSearchDocWebhookAndForward(t *testing.T) {
	msg := map[string]any{
		"message_id": map[string]any{"value": "5", "__fluxer_type": "bigint"},
		"channel_id": map[string]any{"value": "7", "__fluxer_type": "bigint"},
		"webhook_id": map[string]any{"value": "11", "__fluxer_type": "bigint"},
		"reference":  map[string]any{"type": 1},
		"message_snapshots": []any{
			map[string]any{"content": "forwarded body"},
		},
	}
	doc := messageSearchDoc(msg, "")
	if doc["authorType"] != "webhook" {
		t.Fatalf("authorType = %v", doc["authorType"])
	}
	if doc["hasForward"] != true {
		t.Fatalf("hasForward = %v", doc["hasForward"])
	}
	if doc["guildId"] != nil {
		t.Fatalf("guildId = %v, want nil for a DM", doc["guildId"])
	}
	if doc["authorId"] != nil {
		t.Fatalf("authorId = %v, want nil", doc["authorId"])
	}
}

func TestMessageTypeIsSystem(t *testing.T) {
	for _, tc := range []struct {
		typ  int
		want bool
	}{
		{messageTypeThreadCreated, true},
		{messageTypeThreadStarter, true},
		{0, false},
		{7, false},
	} {
		if got := messageTypeIsSystem(map[string]any{"type": tc.typ}); got != tc.want {
			t.Fatalf("type %d: got %v, want %v", tc.typ, got, tc.want)
		}
	}
}

func TestThreadSearchDoc(t *testing.T) {
	channel := map[string]any{
		"channel_id":      map[string]any{"value": "1564611107836919808", "__fluxer_type": "bigint"},
		"type":            11,
		"name":            "Release notes",
		"owner_id":        map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"last_message_id": map[string]any{"value": "1564995185476108288", "__fluxer_type": "bigint"},
	}
	state := map[string]any{
		"thread_id":         map[string]any{"value": "1564611107836919808", "__fluxer_type": "bigint"},
		"guild_id":          map[string]any{"value": "1563469298515574784", "__fluxer_type": "bigint"},
		"parent_id":         map[string]any{"value": "1563469298515574785", "__fluxer_type": "bigint"},
		"archived":          false,
		"locked":            true,
		"applied_tags":      []any{map[string]any{"value": "1", "__fluxer_type": "bigint"}},
		"archive_timestamp": map[string]any{"value": "2026-10-28T09:00:00.000Z", "__fluxer_type": "date"},
	}
	doc := threadSearchDoc(channel, state)

	if doc["id"] != "1564611107836919808" || doc["guildId"] != "1563469298515574784" {
		t.Fatalf("identity: %v", doc)
	}
	if doc["parentId"] != "1563469298515574785" {
		t.Fatalf("parentId = %v", doc["parentId"])
	}
	if doc["name"] != "Release notes" || doc["ownerId"] != "1563468490076061696" {
		t.Fatalf("name/owner: %v", doc)
	}
	if doc["type"] != 11 || doc["locked"] != true || doc["archived"] != false {
		t.Fatalf("state: %v", doc)
	}
	if !reflect.DeepEqual(doc["appliedTagIds"], []string{"1"}) {
		t.Fatalf("appliedTagIds = %v", doc["appliedTagIds"])
	}
	// A cursor page is ordered by (createdAt, idSequence), so both must be there.
	if doc["createdAt"] != snowflakeMillis("1564611107836919808") {
		t.Fatalf("createdAt = %v", doc["createdAt"])
	}
	if doc["idSequence"] != snowflakeSequence("1564611107836919808") {
		t.Fatalf("idSequence = %v", doc["idSequence"])
	}
	if doc["lastMessageAt"] != snowflakeMillis("1564995185476108288") {
		t.Fatalf("lastMessageAt = %v", doc["lastMessageAt"])
	}
	if doc["archivedAt"] != int64(1793178000)*1000 {
		t.Fatalf("archivedAt = %v", doc["archivedAt"])
	}
}

func TestSnowflakeHelpers(t *testing.T) {
	// The snowflake epoch is 2015-01-01; id 0 is the epoch and id 2^22 is one ms.
	if got := snowflakeMillis("0"); got != snowflakeEpoch {
		t.Fatalf("snowflakeMillis(0) = %d, want %d", got, snowflakeEpoch)
	}
	if got := snowflakeMillis("4194304"); got != snowflakeEpoch+1 {
		t.Fatalf("snowflakeMillis(2^22) = %d, want %d", got, snowflakeEpoch+1)
	}
	if got := snowflakeSequence("4194307"); got != 3 {
		t.Fatalf("snowflakeSequence = %d, want 3", got)
	}
	if _, ok := parseSnowflake("not-a-number"); ok {
		t.Fatalf("parseSnowflake accepted a non-numeric id")
	}
}

func TestURLHostname(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"https://example.com/path?q=1", "example.com"},
		{"http://user:pass@Example.COM:8080/x", "example.com"},
		{"https://xn--80ak6aa92e.com/", "xn--80ak6aa92e.com"},
		{"", ""},
		{"not a url", ""},
	} {
		if got := urlHostname(tc.in); got != tc.want {
			t.Fatalf("urlHostname(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := fileExtension("archive.tar.gz"); got != "gz" {
		t.Fatalf("fileExtension = %q", got)
	}
	if got := fileExtension("README"); got != "" {
		t.Fatalf("fileExtension(README) = %q, want empty", got)
	}
	if got := fileExtension("x.verylongextension"); got != "" {
		t.Fatalf("fileExtension accepted an over-long extension: %q", got)
	}
}
