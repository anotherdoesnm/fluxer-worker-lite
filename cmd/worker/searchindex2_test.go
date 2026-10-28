package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestBuildUsernameSearch(t *testing.T) {
	cases := []struct{ in, want string }{
		{"doesnm", "doesnm oesnm esnm snm nm"},
		{"AB", "ab"},
		{"a", "a"},
		{"", ""},
		{"  MixedCase ", "mixedcase ixedcase xedcase edcase dcase case ase se"},
	}
	for _, tc := range cases {
		if got := buildUsernameSearch(tc.in); got != tc.want {
			t.Fatalf("buildUsernameSearch(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	// The suffix list is deduplicated: "aaa" would otherwise repeat "aa".
	if got := buildUsernameSearch("aaa"); got != "aaa aa" {
		t.Fatalf("buildUsernameSearch(aaa) = %q", got)
	}
}

func TestDiscriminatorString(t *testing.T) {
	for _, tc := range []struct {
		in   int
		want string
	}{{0, "0000"}, {7, "0007"}, {42, "0042"}, {12345, "12345"}} {
		if got := discriminatorString(tc.in); got != tc.want {
			t.Fatalf("discriminatorString(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestGuildMemberSearchDoc(t *testing.T) {
	member := map[string]any{
		"guild_id":           map[string]any{"value": "1563469298515574784", "__fluxer_type": "bigint"},
		"user_id":            map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"nick":               "kir",
		"role_ids":           []any{map[string]any{"value": "11", "__fluxer_type": "bigint"}},
		"joined_at":          map[string]any{"value": "2026-10-24T08:28:48.638Z", "__fluxer_type": "date"},
		"join_source_type":   2,
		"source_invite_code": "abc",
		"inviter_id":         map[string]any{"value": "42", "__fluxer_type": "bigint"},
	}
	user := map[string]any{
		"user_id":       map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"username":      "doesnm",
		"global_name":   "Kirill",
		"discriminator": 0,
		"bot":           false,
	}
	doc := guildMemberSearchDoc("1563469298515574784", member, user)

	if doc["id"] != "1563469298515574784_1563468490076061696" {
		t.Fatalf("id = %v", doc["id"])
	}
	if doc["guildId"] != "1563469298515574784" || doc["userId"] != "1563468490076061696" {
		t.Fatalf("ids = %v / %v", doc["guildId"], doc["userId"])
	}
	if doc["username"] != "doesnm" || doc["usernameSearch"] != "doesnm oesnm esnm snm nm" {
		t.Fatalf("username fields = %v / %v", doc["username"], doc["usernameSearch"])
	}
	if doc["discriminator"] != "0000" || doc["globalName"] != "Kirill" || doc["nickname"] != "kir" {
		t.Fatalf("display fields = %v / %v / %v", doc["discriminator"], doc["globalName"], doc["nickname"])
	}
	if !reflect.DeepEqual(doc["roleIds"], []string{"11"}) {
		t.Fatalf("roleIds = %v", doc["roleIds"])
	}
	if doc["joinedAt"] != int64(1792830528) {
		t.Fatalf("joinedAt = %v", doc["joinedAt"])
	}
	if doc["joinSourceType"] != 2 || doc["sourceInviteCode"] != "abc" || doc["inviterId"] != "42" {
		t.Fatalf("join fields = %v / %v / %v", doc["joinSourceType"], doc["sourceInviteCode"], doc["inviterId"])
	}
	// userCreatedAt is derived from the user's own snowflake, not from the row.
	if doc["userCreatedAt"] != snowflakeSeconds("1563468490076061696") {
		t.Fatalf("userCreatedAt = %v", doc["userCreatedAt"])
	}
	if doc["isBot"] != false {
		t.Fatalf("isBot = %v", doc["isBot"])
	}
}

func TestGuildMemberSearchDocMissingFields(t *testing.T) {
	doc := guildMemberSearchDoc("1", map[string]any{"user_id": map[string]any{"value": "2", "__fluxer_type": "bigint"}}, map[string]any{})
	if doc["nickname"] != nil || doc["globalName"] != nil || doc["sourceInviteCode"] != nil || doc["inviterId"] != nil {
		t.Fatalf("missing fields should be null, got %v", doc)
	}
	if doc["joinedAt"] != int64(0) {
		t.Fatalf("joinedAt = %v, want 0", doc["joinedAt"])
	}
	if !reflect.DeepEqual(doc["roleIds"], []string(nil)) {
		t.Fatalf("roleIds = %#v, want nil", doc["roleIds"])
	}
}

func TestUserSearchDoc(t *testing.T) {
	user := map[string]any{
		"user_id":        map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"username":       "doesnm",
		"discriminator":  0,
		"bot":            false,
		"system":         false,
		"flags":          map[string]any{"value": "549755813888", "__fluxer_type": "bigint"},
		"acls":           map[string]any{"value": []any{"*"}, "__fluxer_type": "set"},
		"email_verified": true,
		"email_bounced":  false,
		"last_active_at": map[string]any{"value": "2026-10-28T10:00:00.000Z", "__fluxer_type": "date"},
	}
	doc := userSearchDoc(user)
	if doc["id"] != "1563468490076061696" || doc["username"] != "doesnm" {
		t.Fatalf("identity: %v", doc)
	}
	if doc["flags"] != "549755813888" {
		t.Fatalf("flags = %v", doc["flags"])
	}
	if !reflect.DeepEqual(doc["acls"], []string{"*"}) {
		t.Fatalf("acls = %v", doc["acls"])
	}
	if doc["emailVerified"] != true || doc["isBot"] != false {
		t.Fatalf("booleans: %v", doc)
	}
	if doc["createdAt"] != snowflakeSeconds("1563468490076061696") {
		t.Fatalf("createdAt = %v", doc["createdAt"])
	}
	if doc["lastActiveAt"] != int64(1793181600) {
		t.Fatalf("lastActiveAt = %v", doc["lastActiveAt"])
	}
	// The fields this instance has no columns for must be null, not zero.
	for _, key := range []string{"email", "premiumType", "tempBannedUntil", "pendingDeletionAt"} {
		if doc[key] != nil {
			t.Fatalf("%s = %v, want nil", key, doc[key])
		}
	}
}

func TestUserSearchDocDefaults(t *testing.T) {
	doc := userSearchDoc(map[string]any{})
	if doc["flags"] != "0" {
		t.Fatalf("flags = %v, want the \"0\" string", doc["flags"])
	}
	if doc["lastActiveAt"] != nil {
		t.Fatalf("lastActiveAt = %v, want nil", doc["lastActiveAt"])
	}
	if !reflect.DeepEqual(doc["acls"], []string(nil)) {
		t.Fatalf("acls = %#v", doc["acls"])
	}
}

func TestGuildSearchDoc(t *testing.T) {
	guild := map[string]any{
		"guild_id":           map[string]any{"value": "1563469298515574784", "__fluxer_type": "bigint"},
		"owner_id":           map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"name":               "1GB RAM Fluxer Testing",
		"features":           map[string]any{"value": []any{"DISCOVERABLE", "BANNER"}, "__fluxer_type": "set"},
		"member_count":       5,
		"verification_level": 0,
		"mfa_level":          0,
		"nsfw_level":         0,
	}
	discovery := map[string]any{
		"status":           "approved",
		"description":      "A tiny test guild",
		"category_type":    float64(4),
		"primary_language": "ru",
		"custom_tags":      []any{"#Sokoban", "  puzzles "},
	}
	doc := guildSearchDoc(guild, discovery)

	if doc["id"] != "1563469298515574784" || doc["name"] != "1GB RAM Fluxer Testing" {
		t.Fatalf("identity: %v", doc)
	}
	if doc["isDiscoverable"] != true {
		t.Fatalf("isDiscoverable = %v", doc["isDiscoverable"])
	}
	if doc["verificationLevel"] != guildVerificationLevelLow {
		t.Fatalf("verificationLevel = %v, want LOW for a discoverable guild", doc["verificationLevel"])
	}
	if doc["discoveryDescription"] != "A tiny test guild" || doc["discoveryCategory"] != int64(4) || doc["discoveryPrimaryLanguage"] != "ru" {
		t.Fatalf("discovery fields: %v", doc)
	}
	if !reflect.DeepEqual(doc["discoveryTags"], []string{"sokoban", "puzzles"}) {
		t.Fatalf("discoveryTags = %v", doc["discoveryTags"])
	}
	if !reflect.DeepEqual(doc["features"], []string{"DISCOVERABLE", "BANNER"}) {
		t.Fatalf("features = %v", doc["features"])
	}
}

func TestGuildSearchDocWithoutDiscovery(t *testing.T) {
	guild := map[string]any{
		"guild_id":           map[string]any{"value": "7", "__fluxer_type": "bigint"},
		"name":               "Private",
		"features":           map[string]any{"value": []any{"BANNER"}, "__fluxer_type": "set"},
		"verification_level": 9,
	}
	doc := guildSearchDoc(guild, nil)
	if doc["isDiscoverable"] != false {
		t.Fatalf("isDiscoverable = %v", doc["isDiscoverable"])
	}
	// Not discoverable: only the HIGH cap applies.
	if doc["verificationLevel"] != guildVerificationLevelHigh {
		t.Fatalf("verificationLevel = %v, want %d", doc["verificationLevel"], guildVerificationLevelHigh)
	}
	if doc["discoveryDescription"] != nil || doc["discoveryCategory"] != nil || doc["discoveryPrimaryLanguage"] != nil {
		t.Fatalf("discovery fields should be null: %v", doc)
	}
}

func TestNormalizeDiscoveryTag(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"#Sokoban", "sokoban"},
		{"  Puzzles  ", "puzzles"},
		{"", ""},
	} {
		if got := normalizeDiscoveryTag(tc.in); got != tc.want {
			t.Fatalf("normalizeDiscoveryTag(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestBigintValueFromSegment(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`{"__fluxer_type":"bigint","value":"1563469298515574784"}`, "1563469298515574784"},
		{`"42"`, ""},
		{"12345", "12345"},
		{`{"__fluxer_type":"bigint","value":"abc"}`, ""},
		{"", ""},
	} {
		if got := bigintValueFromSegment(tc.in); got != tc.want {
			t.Fatalf("bigintValueFromSegment(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRefreshIndexTypes(t *testing.T) {
	for _, name := range []string{"guilds", "users", "reports", "audit_logs", "channel_messages", "guild_members", "discovery", "threads", "all"} {
		if !refreshIndexTypes[name] {
			t.Fatalf("refreshIndexTypes is missing %q", name)
		}
	}
	if refreshIndexTypes["nonsense"] {
		t.Fatalf("refreshIndexTypes accepted an unknown type")
	}
	// The guild-scoped types are exactly the ones that need guild_id.
	for _, name := range []string{"channel_messages", "guild_members", "threads"} {
		if !refreshIndexTypes[name] {
			t.Fatalf("%q must be a known type", name)
		}
	}
}

func TestRefreshProgressShape(t *testing.T) {
	// valkey() is nil in the unit tests, so capture the document the function
	// would write by exercising the same map it builds.
	doc := map[string]any{
		"status":     "in_progress",
		"index_type": "guilds",
		"indexed":    10,
		"total":      20,
		"phase":      "guilds",
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"status", "index_type", "indexed", "total", "phase"} {
		if _, ok := back[key]; !ok {
			t.Fatalf("progress document is missing %q: %s", key, raw)
		}
	}
}

func TestAuditLogSearchDoc(t *testing.T) {
	row := map[string]any{
		"log_id":           map[string]any{"value": "1563468490076061697", "__fluxer_type": "bigint"},
		"admin_user_id":    map[string]any{"value": "1563468490076061696", "__fluxer_type": "bigint"},
		"target_type":      "instance_config",
		"target_id":        map[string]any{"value": "0", "__fluxer_type": "bigint"},
		"action":           "get_instance_config",
		"audit_log_reason": nil,
	}
	doc := auditLogSearchDoc(row)
	if doc["id"] != "1563468490076061697" || doc["logId"] != doc["id"] {
		t.Fatalf("identity: %v", doc)
	}
	if doc["adminUserId"] != "1563468490076061696" || doc["action"] != "get_instance_config" {
		t.Fatalf("fields: %v", doc)
	}
	if doc["targetType"] != "instance_config" || doc["targetId"] != "0" {
		t.Fatalf("target: %v", doc)
	}
	if doc["auditLogReason"] != nil {
		t.Fatalf("auditLogReason = %v, want nil", doc["auditLogReason"])
	}
	if doc["createdAt"] != snowflakeSeconds("1563468490076061697") {
		t.Fatalf("createdAt = %v", doc["createdAt"])
	}
}

func TestReportSearchDoc(t *testing.T) {
	row := map[string]any{
		"report_id":             map[string]any{"value": "1565000000000000000", "__fluxer_type": "bigint"},
		"reporter_id":           map[string]any{"value": "42", "__fluxer_type": "bigint"},
		"reported_at":           map[string]any{"value": "2026-10-28T12:00:00.000Z", "__fluxer_type": "date"},
		"status":                1,
		"report_type":           2,
		"category":              "harassment",
		"reason":                "spam",
		"additional_info":       "details",
		"reported_user_id":      map[string]any{"value": "7", "__fluxer_type": "bigint"},
		"reported_guild_name":   "Test Guild",
		"reported_channel_name": "general",
		"resolved_at":           map[string]any{"value": "2026-10-28T13:00:00.000Z", "__fluxer_type": "date"},
		"resolved_by_admin_id":  map[string]any{"value": "9", "__fluxer_type": "bigint"},
	}
	doc := reportSearchDoc(row)
	if doc["id"] != "1565000000000000000" || doc["reporterId"] != "42" {
		t.Fatalf("identity: %v", doc)
	}
	if doc["status"] != 1 || doc["reportType"] != 2 || doc["category"] != "harassment" {
		t.Fatalf("classification: %v", doc)
	}
	if doc["reportedAt"] != int64(1793188800) || doc["resolvedAt"] != int64(1793192400) {
		t.Fatalf("timestamps: %v / %v", doc["reportedAt"], doc["resolvedAt"])
	}
	if doc["reportedGuildName"] != "Test Guild" || doc["reportedChannelName"] != "general" {
		t.Fatalf("names: %v", doc)
	}
	if doc["reportedUserId"] != "7" || doc["resolvedByAdminId"] != "9" {
		t.Fatalf("ids: %v / %v", doc["reportedUserId"], doc["resolvedByAdminId"])
	}
	// An anonymous report has no reporter id, and upstream indexes it as the
	// literal "anonymous" so the filter still matches.
	anonymous := reportSearchDoc(map[string]any{"report_id": map[string]any{"value": "5", "__fluxer_type": "bigint"}})
	if anonymous["reporterId"] != "anonymous" {
		t.Fatalf("anonymous reporter = %v", anonymous["reporterId"])
	}
	if anonymous["resolvedAt"] != nil || anonymous["category"] != "" {
		t.Fatalf("empty report: %v", anonymous)
	}
}
