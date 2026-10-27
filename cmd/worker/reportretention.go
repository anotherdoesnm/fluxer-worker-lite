package main

// taskExpireReportSnapshots ports ExpireReportSnapshots.ts.
//
// Account reports keep a copy of what was reported — message context, a profile
// snapshot, the reporter's notes — and that copy must not live forever. The api
// keeps it for a year, shortens that when the reporter's complaint was dropped
// without a violation, and never while a legal hold is in force. This task walks
// the reports and applies exactly that policy:
//
//	retained        nothing to do
//	held            a legal hold covers it; nothing to do, but its objects count
//	                as referenced so another report cannot delete them
//	context_expired the copied context goes, the report itself stays
//	expired         the report, its notes and its evidence go
//
// Every object deletion is filtered through the set of keys other reports still
// reference, because the same attachment can appear in two reports.
//
// Not ported, both because the plumbing is absent: the S3 *listing* the api uses
// to find stray profile objects under reports/<id>/profile/ (this worker can only
// delete keys it can compute from the row), and the search-index update (no search
// backend on this instance).

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	reportContentRetentionDays = 365
	reportComplaintWindowDays  = 184
	reportChildSafetyDays      = 365
	decisionEvidenceTTLDays    = 365
	childSafetyCategory        = "child_safety"
	reportStatusResolved       = 1
	iarReportsTable            = "iar_submissions"
	iarNotesTable              = "iar_report_notes"
	enforcementDecisionsTable  = "enforcement_decisions"
)

type reportRow struct {
	RowKey   string
	Raw      map[string]any
	ReportID string
}

func taskExpireReportSnapshots(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("expireReportSnapshots: %w", err)
		}
	}
	retentionDays := 365
	if v := os.Getenv("FLUXER_REPORT_RETENTION_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			retentionDays = n
		}
	}
	dryRun := os.Getenv("FLUXER_REPORT_RETENTION_DRY_RUN") == "true"

	reports, err := scanReports(ctx)
	if err != nil {
		return fmt.Errorf("expireReportSnapshots: %w", err)
	}
	decidedAt, err := decisionDecidedAt(ctx)
	if err != nil {
		return fmt.Errorf("expireReportSnapshots: decisions: %w", err)
	}
	now := time.Now().UTC()

	// First pass: classify, and collect every key any surviving report still
	// references. Deleting in one pass would let an earlier report free an object
	// a later one still needs.
	retainedKeys := map[string]struct{}{}
	type dueReport struct {
		row   reportRow
		state string
	}
	var due []dueReport
	for _, report := range reports {
		state := reportRetentionState(report, now, retentionDays, decidedAt[report.ReportID])
		if state == "expired" || state == "context_expired" {
			due = append(due, dueReport{row: report, state: state})
			continue
		}
		for _, key := range referencedObjectKeys(report) {
			retainedKeys[key] = struct{}{}
		}
	}

	var expired, contextExpired, held, objectsDeleted, sharedKept, failed int
	for _, item := range due {
		if reportRetentionState(item.row, now, retentionDays, decidedAt[item.row.ReportID]) == "held" {
			held++
			continue
		}
		deleted, kept, err := deleteReportEvidence(ctx, item.row, retainedKeys, dryRun)
		objectsDeleted += deleted
		sharedKept += kept
		if err != nil {
			failed++
			log.Printf("expireReportSnapshots: report %s: evidence: %v", item.row.ReportID, err)
			continue
		}
		if dryRun {
			log.Printf("expireReportSnapshots: dry run: report %s would be %s", item.row.ReportID, item.state)
			continue
		}
		if item.state == "expired" {
			if err := deleteReportRows(ctx, item.row); err != nil {
				failed++
				log.Printf("expireReportSnapshots: report %s: delete: %v", item.row.ReportID, err)
				continue
			}
			expired++
			continue
		}
		if err := clearReportContext(ctx, item.row); err != nil {
			failed++
			log.Printf("expireReportSnapshots: report %s: clear context: %v", item.row.ReportID, err)
			continue
		}
		contextExpired++
	}
	log.Printf("expireReportSnapshots: scanned=%d due=%d expired=%d context_cleared=%d held=%d objects_deleted=%d shared_kept=%d failed=%d dry_run=%v",
		len(reports), len(due), expired, contextExpired, held, objectsDeleted, sharedKept, failed, dryRun)
	if failed > 0 {
		return fmt.Errorf("expireReportSnapshots: %d reports could not be processed", failed)
	}
	return nil
}

func scanReports(ctx context.Context) ([]reportRow, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_key, row_data::text FROM fluxer_kv WHERE table_name = $1 ORDER BY row_key`, iarReportsTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reportRow
	for rows.Next() {
		var rowKey, raw string
		if err := rows.Scan(&rowKey, &raw); err != nil {
			return nil, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		reportID := typedString(row["report_id"])
		if reportID == "" {
			continue
		}
		out = append(out, reportRow{RowKey: rowKey, Raw: row, ReportID: reportID})
	}
	return out, rows.Err()
}

// decisionDecidedAt maps a report id to the newest decision that referenced it:
// evidence stays as long as the decision it supports.
func decisionDecidedAt(ctx context.Context) (map[string]time.Time, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv WHERE table_name = $1`, enforcementDecisionsTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		createdAt, ok := parseTimestampMs(typedString(row["created_at"]))
		if !ok {
			continue
		}
		for _, reportID := range stringSet(row["report_ids"]) {
			if known, exists := out[reportID]; !exists || createdAt > known.UnixMilli() {
				out[reportID] = time.UnixMilli(createdAt).UTC()
			}
		}
	}
	return out, rows.Err()
}

// reportRetentionState is reportRetentionState from the api: expired, context
// expired, held, or nothing to do.
func reportRetentionState(report reportRow, now time.Time, retentionDays int, decidedAt time.Time) string {
	raw := report.Raw
	contentEnd := time.Time{}
	if reportedAt, ok := parseTimestampMs(typedString(raw["reported_at"])); ok {
		contentEnd = time.UnixMilli(reportedAt).UTC().AddDate(0, 0, reportContentRetentionDays)
	}
	resolvedAt := time.Time{}
	if v, ok := parseTimestampMs(typedString(raw["resolved_at"])); ok {
		resolvedAt = time.UnixMilli(v).UTC()
	} else if !contentEnd.IsZero() {
		resolvedAt = contentEnd.AddDate(0, 0, -reportContentRetentionDays)
	}
	deleteAt := time.Time{}
	contextEnd := contentEnd
	if status, _ := intFieldChecked(map[string]any{"v": raw["status"]}, "v"); status == reportStatusResolved {
		if resolvedAt.IsZero() {
			return "retained"
		}
		deleteAt = resolvedAt.AddDate(0, 0, retentionDays)
		childSafety := typedString(raw["category"]) == childSafetyCategory
		if childSafety {
			if candidate := resolvedAt.AddDate(0, 0, reportChildSafetyDays); candidate.After(deleteAt) {
				deleteAt = candidate
			}
		}
		if !decidedAt.IsZero() {
			if candidate := decidedAt.AddDate(0, 0, decisionEvidenceTTLDays); candidate.After(deleteAt) {
				deleteAt = candidate
			}
		}
		dropsContextEarly := typedString(raw["reporter_outcome"]) == "no_violation" && !childSafety && decidedAt.IsZero()
		if dropsContextEarly {
			if candidate := resolvedAt.AddDate(0, 0, reportComplaintWindowDays); candidate.Before(contextEnd) {
				contextEnd = candidate
			}
		}
	}

	var due string
	switch {
	case !deleteAt.IsZero() && !deleteAt.After(now):
		due = "expired"
	case !contextEnd.IsZero() && !contextEnd.After(now) && hasCopiedContext(report):
		due = "context_expired"
	default:
		return "retained"
	}
	if isReportUnderLegalHold(report, now) {
		return "held"
	}
	return due
}

func hasCopiedContext(report reportRow) bool {
	if ctxList, ok := report.Raw["message_context"].([]any); ok && len(ctxList) > 0 {
		return true
	}
	return report.Raw["reported_profile_snapshot"] != nil || report.Raw["additional_info"] != nil
}

func isReportUnderLegalHold(report reportRow, now time.Time) bool {
	until, ok := parseTimestampMs(typedString(report.Raw["legal_hold_until"]))
	return ok && until > now.UnixMilli()
}

// referencedObjectKeys lists the storage keys a report points at.
func referencedObjectKeys(report reportRow) []string {
	var keys []string
	channelFallback := typedString(report.Raw["reported_channel_id"])
	if list, ok := report.Raw["message_context"].([]any); ok {
		for _, item := range list {
			message, ok := item.(map[string]any)
			if !ok {
				continue
			}
			channelID := typedString(message["channel_id"])
			if channelID == "" {
				channelID = channelFallback
			}
			if channelID == "" {
				continue
			}
			attachments, ok := message["attachments"].([]any)
			if !ok {
				continue
			}
			for _, a := range attachments {
				attachment, ok := a.(map[string]any)
				if !ok {
					continue
				}
				attachmentID := typedString(attachment["attachment_id"])
				filename := typedString(attachment["filename"])
				if attachmentID == "" || filename == "" {
					continue
				}
				keys = append(keys, fmt.Sprintf("attachments/%s/%s/%s", channelID, attachmentID, filename))
			}
		}
	}
	keys = append(keys, profileSnapshotKeys(report.Raw["reported_profile_snapshot"])...)
	return keys
}

// profileSnapshotKeys pulls the stored keys out of the profile snapshot: user,
// member and guild assets, each carrying its own storage key.
func profileSnapshotKeys(v any) []string {
	snapshot, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	var keys []string
	for _, section := range []string{"user", "member", "guild"} {
		block, ok := snapshot[section].(map[string]any)
		if !ok {
			continue
		}
		for _, assetName := range []string{"avatar", "banner", "icon", "splash"} {
			asset, ok := block[assetName].(map[string]any)
			if !ok {
				continue
			}
			if key := typedString(asset["key"]); key != "" {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// deleteReportEvidence deletes the keys no other report references.
func deleteReportEvidence(ctx context.Context, report reportRow, retained map[string]struct{}, dryRun bool) (deleted, kept int, err error) {
	for _, key := range referencedObjectKeys(report) {
		if _, shared := retained[key]; shared {
			kept++
			continue
		}
		if dryRun {
			deleted++
			continue
		}
		// deleteObject logs and swallows failures by design: one missing object
		// must not stop the retention sweep.
		deleteObject(ctx, key, "report evidence")
		deleted++
	}
	return deleted, kept, nil
}

func deleteReportRows(ctx context.Context, report reportRow) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		iarReportsTable, report.RowKey); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = $1 AND left(row_key, length($2)) = $2`,
		iarNotesTable, bigintSeg(report.ReportID)+kvSep); err != nil {
		return err
	}
	return nil
}

// clearReportContext drops the copied context but keeps the report row.
func clearReportContext(ctx context.Context, report reportRow) error {
	for _, field := range []string{"message_context", "reported_profile_snapshot", "additional_info"} {
		if err := patchRowField(ctx, iarReportsTable, report.RowKey, field, nil); err != nil {
			return err
		}
	}
	return nil
}
