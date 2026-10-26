package main

// Tests for the account deletion scheduler: the queue member format (which the api
// writes and we must parse before destroying an account), the reason resolution and
// the eligibility rules.

import (
	"testing"
	"time"
)

func TestParseQueueMember(t *testing.T) {
	good := []struct {
		in     string
		user   string
		reason int
	}{
		{"1564229149046865920|1", "1564229149046865920", 1},
		{"1564229149046865920|0", "1564229149046865920", 0},
		{"1|2", "1", 2},
	}
	for _, tc := range good {
		user, reason, err := parseQueueMember(tc.in)
		if err != nil {
			t.Fatalf("%q: unexpected error %v", tc.in, err)
		}
		if user != tc.user || reason != tc.reason {
			t.Fatalf("%q: got %q/%d, want %q/%d", tc.in, user, reason, tc.user, tc.reason)
		}
	}

	bad := []string{
		"",
		"|1",
		"123|",
		"123",
		"123|4|5",
		"12a|1",
		"123|-1",
		"123|+1",
		"123|4294967296",
		"12345678901234567890|1", // 20 digits, longer than a snowflake
	}
	for _, in := range bad {
		if _, _, err := parseQueueMember(in); err == nil {
			t.Fatalf("%q: expected an error", in)
		}
	}
}

func TestResolveDeletionReason(t *testing.T) {
	cases := []struct {
		name   string
		state  userDeletionState
		queued int
		want   int
	}{
		{"stored reason wins", userDeletionState{Reason: 3, Flags: userFlagSelfDeleted}, 1, 3},
		{"queued reason wins over flags", userDeletionState{Reason: -1, Flags: userFlagSelfDeleted}, 4, 4},
		{"self deleted", userDeletionState{Reason: -1, Flags: userFlagSelfDeleted}, 0, deletionReasonUserRequested},
		{"already deleted", userDeletionState{Reason: -1, Flags: userFlagDeleted}, 0, deletionReasonOther},
		{"self-deleted flag wins over deleted flag", userDeletionState{Reason: -1, Flags: userFlagDeleted | userFlagSelfDeleted}, 0, deletionReasonUserRequested},
		{"nothing known", userDeletionState{Reason: -1}, 0, 0},
	}
	for _, tc := range cases {
		if got := resolveDeletionReason(&tc.state, tc.queued); got != tc.want {
			t.Fatalf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestDeletionBlocked(t *testing.T) {
	if !(&userDeletionState{Bot: true}).deletionBlocked() {
		t.Fatal("bots are not deleted automatically")
	}
	if !(&userDeletionState{Flags: userFlagAppStoreReviewer}).deletionBlocked() {
		t.Fatal("app store reviewers are not deleted automatically")
	}
	if (&userDeletionState{Flags: userFlagSelfDeleted}).deletionBlocked() {
		t.Fatal("an ordinary pending user must not be blocked")
	}
}

func TestPendingDeletionDue(t *testing.T) {
	now := time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC)
	if (&userDeletionState{}).pendingDeletionDue(now) {
		t.Fatal("a user with no schedule is never due")
	}
	exact := &userDeletionState{PendingAt: now}
	if !exact.pendingDeletionDue(now) {
		t.Fatal("the exact schedule is due")
	}
	later := &userDeletionState{PendingAt: now.Add(time.Minute)}
	if later.pendingDeletionDue(now) {
		t.Fatal("a future schedule is not due")
	}
}

// The api writes pending_deletion_at as a typed date and bot as a plain bool; both
// have to survive the row decoding or the scheduler silently skips users.
func TestDeletionStateFromRow(t *testing.T) {
	row := map[string]any{
		"user_id":              map[string]any{"__fluxer_type": "bigint", "value": "1564229149046865920"},
		"pending_deletion_at":  map[string]any{"__fluxer_type": "date", "value": "2026-10-26T12:00:00.000Z"},
		"deletion_started_at":  map[string]any{"__fluxer_type": "date", "value": "2026-10-26T12:30:00.000Z"},
		"deletion_reason_code": map[string]any{"__fluxer_type": "bigint", "value": "1"},
		"flags":                map[string]any{"__fluxer_type": "bigint", "value": "68719476736"},
		"bot":                  false,
	}
	state := deletionStateFromRow(row)
	if state.UserID != "1564229149046865920" {
		t.Fatalf("user id: %q", state.UserID)
	}
	if state.PendingAt.IsZero() || state.StartedAt.IsZero() {
		t.Fatalf("timestamps were not parsed: %+v", state)
	}
	if state.Reason != 1 || state.Flags != userFlagSelfDeleted || state.Bot {
		t.Fatalf("unexpected state: %+v", state)
	}

	absent := deletionStateFromRow(map[string]any{"user_id": "1"})
	if absent.Reason != -1 {
		t.Fatalf("an absent reason must stay -1, got %d", absent.Reason)
	}
}

func TestQueueMemberRoundTrip(t *testing.T) {
	member := queueMember("1564229149046865920", deletionReasonUserRequested)
	user, reason, err := parseQueueMember(member)
	if err != nil || user != "1564229149046865920" || reason != deletionReasonUserRequested {
		t.Fatalf("round trip of %q gave %q/%d/%v", member, user, reason, err)
	}
}

// guilds.member_count is a plain number while most values are wrapped; reading it
// the wrong way silently zeroed the counter on a live run.
func TestIntFieldChecked(t *testing.T) {
	cases := []struct {
		row  map[string]any
		want int
		ok   bool
	}{
		{map[string]any{"member_count": float64(7)}, 7, true},
		{map[string]any{"member_count": 3}, 3, true},
		{map[string]any{"member_count": map[string]any{"__fluxer_type": "bigint", "value": "42"}}, 42, true},
		{map[string]any{"member_count": "5"}, 0, false},
		{map[string]any{}, 0, false},
	}
	for _, tc := range cases {
		got, ok := intFieldChecked(tc.row, "member_count")
		if got != tc.want || ok != tc.ok {
			t.Fatalf("%+v: got %d/%v, want %d/%v", tc.row, got, ok, tc.want, tc.ok)
		}
	}
}

func TestParsePendingActivity(t *testing.T) {
	good := []struct {
		raw string
		ts  int64
		ip  string
	}{
		{`{"ts":1793024861212}`, 1793024861212, ""},
		{`{"ts":1793024861212,"ip":"192.168.1.10"}`, 1793024861212, "192.168.1.10"},
		{`{"ts":1,"ip":null}`, 1, ""},
	}
	for _, tc := range good {
		e, ok := parsePendingActivity(tc.raw)
		if !ok || e.TS != tc.ts {
			t.Fatalf("%s: got %+v ok=%v", tc.raw, e, ok)
		}
		got := ""
		if e.IP != nil {
			got = *e.IP
		}
		if got != tc.ip {
			t.Fatalf("%s: ip %q, want %q", tc.raw, got, tc.ip)
		}
	}
	for _, bad := range []string{``, `null`, `{}`, `{"ts":0}`, `{"ts":-5}`, `not json`} {
		if _, ok := parsePendingActivity(bad); ok {
			t.Fatalf("%q: expected the entry to be rejected", bad)
		}
	}
}

// The buffer names sessions with base64url while auth_sessions stores standard
// base64; getting this wrong makes every flush a silent no-op.
func TestSessionHashFromBase64URL(t *testing.T) {
	cases := map[string]string{
		"7pEHi4PRSKHlpiw4989GQbWYhXw3UYLduNNJxqFNipI": "7pEHi4PRSKHlpiw4989GQbWYhXw3UYLduNNJxqFNipI=",
		"nGK4hjDIdxOQCnOGYXFmjc-x3dNa5JJ7uTI7xbKlHY8": "nGK4hjDIdxOQCnOGYXFmjc+x3dNa5JJ7uTI7xbKlHY8=",
		"qAMIm6Vjp7tMNAoRC_VldMyWk1OJjGIbzYhMekOsiD0": "qAMIm6Vjp7tMNAoRC/VldMyWk1OJjGIbzYhMekOsiD0=",
	}
	for in, want := range cases {
		got, ok := sessionHashFromBase64URL(in)
		if !ok || got != want {
			t.Fatalf("%q: got %q (ok=%v), want %q", in, got, ok, want)
		}
	}
	for _, bad := range []string{"", "!!!", "a"} {
		if _, ok := sessionHashFromBase64URL(bad); ok {
			t.Fatalf("%q: expected a rejection", bad)
		}
	}
}
