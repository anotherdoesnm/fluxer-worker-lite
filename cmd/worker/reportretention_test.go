package main

// Tests for the report-retention sweep.
//
// The state machine is the interesting part and it is pure, so most cases are
// table-driven: a resolved report older than retention expires, a report whose
// copied context has aged out keeps its row and loses the context, a legal hold
// wins over everything, and an open report is left alone.

import (
	"context"
	"testing"
	"time"
)

func reportFixture(mutate func(map[string]any)) reportRow {
	now := time.Now().UTC()
	row := map[string]any{
		"report_id":        kvBigintJSON("960000000000000001"),
		"status":           reportStatusResolved,
		"reported_at":      kvDate(now.AddDate(-3, 0, 0)),
		"resolved_at":      kvDate(now.AddDate(-2, 0, 0)),
		"category":         map[string]any{"__fluxer_type": "string", "value": "spam"},
		"reporter_outcome": map[string]any{"__fluxer_type": "string", "value": "violation"},
		"message_context":  []any{},
	}
	if mutate != nil {
		mutate(row)
	}
	return reportRow{RowKey: bigintSeg("960000000000000001"), Raw: row, ReportID: "960000000000000001"}
}

func TestReportRetentionState(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name    string
		report  reportRow
		days    int
		decided time.Time
		want    string
	}{
		{
			name:   "resolved long ago expires",
			report: reportFixture(nil),
			days:   365,
			want:   "expired",
		},
		{
			name:   "resolved yesterday is retained",
			report: reportFixture(func(r map[string]any) { r["resolved_at"] = kvDate(now.AddDate(0, 0, -1)) }),
			days:   365,
			want:   "retained",
		},
		{
			name: "open report with old copied context clears only the context",
			report: reportFixture(func(r map[string]any) {
				r["status"] = 0
				r["message_context"] = []any{map[string]any{"channel_id": kvBigintJSON("1")}}
			}),
			days: 365,
			want: "context_expired",
		},
		{
			name: "open report without copied context is retained",
			report: reportFixture(func(r map[string]any) {
				r["status"] = 0
				r["message_context"] = []any{}
			}),
			days: 365,
			want: "retained",
		},
		{
			name: "legal hold wins over expiry",
			report: reportFixture(func(r map[string]any) {
				r["legal_hold_until"] = kvDate(now.AddDate(0, 1, 0))
			}),
			days: 365,
			want: "held",
		},
		{
			name:    "a recent decision keeps the evidence",
			report:  reportFixture(nil),
			days:    365,
			decided: now.AddDate(0, 0, -30),
			want:    "retained",
		},
		{
			name: "a dropped complaint shortens the context window",
			report: reportFixture(func(r map[string]any) {
				r["status"] = 0
				r["reporter_outcome"] = map[string]any{"__fluxer_type": "string", "value": "no_violation"}
				r["reported_at"] = kvDate(now.AddDate(0, 0, -400))
				r["resolved_at"] = kvDate(now.AddDate(0, 0, -400))
				r["message_context"] = []any{map[string]any{"channel_id": kvBigintJSON("1")}}
			}),
			days: 365,
			want: "context_expired",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := reportRetentionState(tc.report, now, tc.days, tc.decided); got != tc.want {
				t.Fatalf("state = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReferencedObjectKeys(t *testing.T) {
	report := reportFixture(func(r map[string]any) {
		r["reported_channel_id"] = kvBigintJSON("970000000000000001")
		r["message_context"] = []any{
			map[string]any{
				"channel_id": kvBigintJSON("970000000000000001"),
				"attachments": []any{
					map[string]any{
						"attachment_id": kvBigintJSON("970000000000000010"),
						"filename":      map[string]any{"__fluxer_type": "string", "value": "a.png"},
					},
				},
			},
			map[string]any{
				"attachments": []any{
					map[string]any{
						"attachment_id": kvBigintJSON("970000000000000011"),
						"filename":      map[string]any{"__fluxer_type": "string", "value": "b.png"},
					},
				},
			},
		}
		r["reported_profile_snapshot"] = map[string]any{
			"user": map[string]any{"avatar": map[string]any{"key": "avatars/1/a_hash"}},
		}
	})
	keys := referencedObjectKeys(report)
	want := map[string]bool{
		"attachments/970000000000000001/970000000000000010/a.png": true,
		// The second message has no channel, so it falls back to the reported one.
		"attachments/970000000000000001/970000000000000011/b.png": true,
		"avatars/1/a_hash": true,
	}
	if len(keys) != len(want) {
		t.Fatalf("got %d keys, want %d: %v", len(keys), len(want), keys)
	}
	for _, key := range keys {
		if !want[key] {
			t.Fatalf("unexpected key %q", key)
		}
	}
}

func TestExpireReportSnapshotsAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	now := time.Now().UTC()
	old := now.AddDate(-3, 0, 0)
	seedReport := func(reportID string, resolvedAt time.Time, mutate func(map[string]any)) {
		key := bigintSeg(reportID)
		row := map[string]any{
			"report_id":   kvBigintJSON(reportID),
			"status":      reportStatusResolved,
			"reported_at": kvDate(old),
			"resolved_at": kvDate(resolvedAt),
			"category":    map[string]any{"__fluxer_type": "string", "value": "spam"},
			"message_context": []any{map[string]any{
				"channel_id": kvBigintJSON("980000000000000001"),
				"attachments": []any{map[string]any{
					"attachment_id": kvBigintJSON("980000000000000010"),
					"filename":      map[string]any{"__fluxer_type": "string", "value": "shared.png"},
				}},
			}},
		}
		if mutate != nil {
			mutate(row)
		}
		if err := putKV(ctx, iarReportsTable, key, key, row, ""); err != nil {
			t.Fatalf("seed report %s: %v", reportID, err)
		}
	}
	oldID := "980000000000000101"
	freshID := "980000000000000102"
	heldID := "980000000000000103"
	seedReport(oldID, now.AddDate(-2, 0, 0), nil)
	seedReport(freshID, now.AddDate(0, 0, -1), nil)
	seedReport(heldID, now.AddDate(-2, 0, 0), func(r map[string]any) {
		r["legal_hold_until"] = kvDate(now.AddDate(0, 1, 0))
	})
	t.Cleanup(func() {
		for _, id := range []string{oldID, freshID, heldID} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, iarReportsTable, bigintSeg(id))
		}
	})

	t.Setenv("FLUXER_REPORT_RETENTION_DAYS", "365")
	if err := taskExpireReportSnapshots(ctx, nil); err != nil {
		t.Fatalf("task: %v", err)
	}

	if row, _ := readKV(ctx, iarReportsTable, bigintSeg(oldID)); row != nil {
		t.Fatal("an expired report survived")
	}
	if row, _ := readKV(ctx, iarReportsTable, bigintSeg(freshID)); row == nil {
		t.Fatal("a fresh report was deleted")
	}
	if row, _ := readKV(ctx, iarReportsTable, bigintSeg(heldID)); row == nil {
		t.Fatal("a report under legal hold was deleted")
	}
}
