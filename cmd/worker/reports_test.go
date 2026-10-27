package main

// Tests for ClearAuthenticatedReporterEmails.
//
// The first one is pure: it pins the rule that decides whether a report row
// still holds an email worth erasing. The second one needs a scratch database
// (FLUXER_TEST_PG_DSN) and checks the patch itself, including that rows the
// task must not touch survive unchanged.

import (
	"context"
	"testing"
)

func TestClearableReporter(t *testing.T) {
	str := func(v string) map[string]any {
		return map[string]any{"__fluxer_type": "string", "value": v}
	}
	cases := []struct {
		name   string
		row    map[string]any
		wantOK bool
	}{
		{
			name:   "authenticated reporter with a stored email",
			row:    map[string]any{"reporter_email": str("a@example.org"), "reporter_id": kvBigintJSON("42")},
			wantOK: true,
		},
		{
			name:   "email but no account: the copy is the only way to reach them",
			row:    map[string]any{"reporter_email": str("a@example.org"), "reporter_id": nil},
			wantOK: false,
		},
		{
			name:   "already cleared",
			row:    map[string]any{"reporter_email": nil, "reporter_id": kvBigintJSON("42")},
			wantOK: false,
		},
		{
			name:   "empty email",
			row:    map[string]any{"reporter_email": str(""), "reporter_id": kvBigintJSON("42")},
			wantOK: false,
		},
		{
			name:   "neither field",
			row:    map[string]any{},
			wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			email, id, ok := clearableReporter(tc.row)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if email == "" || id == "" {
				t.Fatalf("clearable row reported empty fields: email=%q id=%q", email, id)
			}
		})
	}
}

func TestClearAuthenticatedReporterEmailsAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const table = "iar_submissions"
	str := func(v string) map[string]any {
		return map[string]any{"__fluxer_type": "string", "value": v}
	}
	keys := []string{
		bigintSeg("900000000000000001"), // authenticated: email must go
		bigintSeg("900000000000000002"), // anonymous: email must stay
		bigintSeg("900000000000000003"), // no email at all
		bigintSeg("900000000000000004"), // authenticated, email already null
	}
	seed := map[string]map[string]any{
		keys[0]: {"report_id": kvBigintJSON("900000000000000001"), "reporter_email": str("keep-me?@example.org"), "reporter_id": kvBigintJSON("100000000000000001")},
		keys[1]: {"report_id": kvBigintJSON("900000000000000002"), "reporter_email": str("anonymous@example.org"), "reporter_id": nil},
		keys[2]: {"report_id": kvBigintJSON("900000000000000003"), "reporter_email": nil, "reporter_id": kvBigintJSON("100000000000000003")},
		keys[3]: {"report_id": kvBigintJSON("900000000000000004"), "reporter_email": nil, "reporter_id": kvBigintJSON("100000000000000004")},
	}
	for key, row := range seed {
		if err := putKV(ctx, table, key, key, row, ""); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	t.Cleanup(func() {
		for _, key := range keys {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, table, key)
		}
	})

	if err := taskClearAuthenticatedReporterEmails(ctx, nil); err != nil {
		t.Fatalf("task: %v", err)
	}

	read := func(t *testing.T, key string) map[string]any {
		t.Helper()
		row, err := readKVRow(ctx, table, key)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		if row == nil {
			t.Fatalf("row %s disappeared", key)
		}
		return row
	}

	if got := typedString(read(t, keys[0])["reporter_email"]); got != "" {
		t.Fatalf("authenticated reporter kept the email: %q", got)
	}
	if got := typedString(read(t, keys[1])["reporter_email"]); got != "anonymous@example.org" {
		t.Fatalf("anonymous reporter lost the email: %q", got)
	}
	// The row must survive the patch with its other columns intact: reports
	// carry legal context that this task has no business rewriting.
	if got := typedString(read(t, keys[0])["report_id"]); got != "900000000000000001" {
		t.Fatalf("report_id damaged by the patch: %q", got)
	}
}
