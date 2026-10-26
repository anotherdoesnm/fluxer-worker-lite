package main

// Tests for the audit-log batching task: the grouping rule (which decides what
// gets collapsed), the options encoding (which is what the client renders), and
// the four row keys, which have to match the strings the API itself writes.

import (
	"strings"
	"testing"
)

func del(logID, user, channel string) auditLogEntry {
	return auditLogEntry{LogID: logID, UserID: user, ActionType: auditActionMessageDelete, ChannelID: channel}
}

func other(logID, user string) auditLogEntry {
	return auditLogEntry{LogID: logID, UserID: user, ActionType: 30}
}

func TestGroupConsecutiveMessageDeletes(t *testing.T) {
	cases := []struct {
		name  string
		logs  []auditLogEntry
		want  [][]string // log ids per expected group, in order
		total int
	}{
		{
			name: "three consecutive deletions become one group",
			logs: []auditLogEntry{del("9", "u1", "c1"), del("8", "u1", "c1"), del("7", "u1", "c1")},
			want: [][]string{{"9", "8", "7"}},
		},
		{
			name: "a single deletion is left alone",
			logs: []auditLogEntry{del("9", "u1", "c1"), other("8", "u1")},
			want: nil,
		},
		{
			name: "another action type closes the run",
			logs: []auditLogEntry{del("9", "u1", "c1"), other("8", "u2"), del("7", "u1", "c1")},
			want: nil,
		},
		{
			name: "a different author starts a new group",
			logs: []auditLogEntry{del("9", "u1", "c1"), del("8", "u1", "c1"), del("7", "u2", "c1"), del("6", "u2", "c1")},
			want: [][]string{{"9", "8"}, {"7", "6"}},
		},
		{
			name: "a different channel starts a new group",
			logs: []auditLogEntry{del("9", "u1", "c1"), del("8", "u1", "c1"), del("7", "u1", "c2"), del("6", "u1", "c2")},
			want: [][]string{{"9", "8"}, {"7", "6"}},
		},
		{
			name: "an entry without a channel id closes the run",
			logs: []auditLogEntry{del("9", "u1", "c1"), {LogID: "8", UserID: "u1", ActionType: auditActionMessageDelete}, del("7", "u1", "c1")},
			want: nil,
		},
	}
	for _, tc := range cases {
		groups := groupConsecutiveMessageDeletes(tc.logs)
		if len(groups) != len(tc.want) {
			t.Fatalf("%s: got %d groups, want %d", tc.name, len(groups), len(tc.want))
		}
		for i, want := range tc.want {
			got := groups[i].Logs
			if len(got) != len(want) {
				t.Fatalf("%s: group %d has %d logs, want %d", tc.name, i, len(got), len(want))
			}
			for j, id := range want {
				if got[j].LogID != id {
					t.Fatalf("%s: group %d position %d is %s, want %s", tc.name, i, j, got[j].LogID, id)
				}
			}
		}
		for _, group := range groups {
			if group.UserID == "" || group.ChannelID == "" {
				t.Fatalf("%s: group lost its key columns: %+v", tc.name, group)
			}
		}
	}
}

func TestAuditOptionsRoundTrip(t *testing.T) {
	encoded := auditOptionsValue([][2]string{{"channel_id", "42"}, {"count", "3"}, {"thread_scoped", "1"}})

	channel, ok := auditOption(encoded, "channel_id")
	if !ok || channel != "42" {
		t.Fatalf("channel_id: got %q/%v", channel, ok)
	}
	count, ok := auditOption(encoded, "count")
	if !ok || count != "3" {
		t.Fatalf("count: got %q/%v", count, ok)
	}
	if _, ok := auditOption(encoded, "absent"); ok {
		t.Fatal("absent key reported as present")
	}
	if encoded := auditOptionsValue(nil); encoded != nil {
		t.Fatalf("empty options should encode as null, got %v", encoded)
	}

	// The API stores numbers in a hand-written map with string values, but a
	// numeric value must still read back, because that is what a JSON round trip
	// through Postgres produces for other option keys.
	numeric := map[string]any{
		"__fluxer_type": "map",
		"value":         []any{[]any{"count", float64(7)}},
	}
	if count, ok := auditOption(numeric, "count"); !ok || count != "7" {
		t.Fatalf("numeric option: got %q/%v", count, ok)
	}
}

func TestAuditLogKeys(t *testing.T) {
	const guild, user, log = "1", "2", "3"
	keys := auditLogKeys(guild, auditLogEntry{LogID: log, UserID: user, ActionType: auditActionMessageDelete})
	if len(keys) != 4 {
		t.Fatalf("got %d keys, want 4", len(keys))
	}
	guildSeg := `{"__fluxer_type":"bigint","value":"` + guild + `"}` + kvSep
	want := map[string]string{
		auditLogsTable:          guildSeg + `{"__fluxer_type":"bigint","value":"3"}`,
		auditLogsByUserTable:    guildSeg + `{"__fluxer_type":"bigint","value":"2"}` + kvSep + `{"__fluxer_type":"bigint","value":"3"}`,
		auditLogsByActionTable:  guildSeg + "72" + kvSep + `{"__fluxer_type":"bigint","value":"3"}`,
		auditLogsByUserActTable: guildSeg + `{"__fluxer_type":"bigint","value":"2"}` + kvSep + "72" + kvSep + `{"__fluxer_type":"bigint","value":"3"}`,
	}
	for _, key := range keys {
		if key.partition != key.rowKey {
			t.Fatalf("%s: partition and row key differ (%q vs %q)", key.table, key.partition, key.rowKey)
		}
		if !strings.HasPrefix(key.rowKey, guildSeg) {
			t.Fatalf("%s: row key does not start with the guild prefix: %q", key.table, key.rowKey)
		}
		if want[key.table] != key.rowKey {
			t.Fatalf("%s: got %q, want %q", key.table, key.rowKey, want[key.table])
		}
	}
}
