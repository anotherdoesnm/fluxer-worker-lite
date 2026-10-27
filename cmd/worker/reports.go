package main

// taskClearAuthenticatedReporterEmails ports ClearAuthenticatedReporterEmails.ts.
//
// An account report stores a copy of the reporter's email so a moderator can
// write back. When the report came from an authenticated account that copy
// stops being the only way to reach the person, so the task erases it and leaves
// the rest of the row alone. Upstream scans every report in pages of 500 and
// patches reporter_email to null where reporter_id is set; the same walk is done
// here, over iar_submissions, one page at a time.
//
// The task is deliberately quiet: a report whose patch fails is counted and
// logged, and the run continues, because one unreadable row must not stop the
// erasure for everybody else.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
)

const (
	// Same page size as upstream; the scan is paginated by report_id, which is
	// the table's only primary key column.
	reporterEmailPageSize = 500
	reporterEmailTable    = "iar_submissions"
)

type reporterEmailSummary struct {
	scanned int
	cleared int
	skipped int
	failed  int
}

// clearableReporter reports whether this row still holds an email that belongs
// to an authenticated account. Both fields are optional, and upstream only
// clears when both are present, so the same rule is spelled out here instead of
// hiding it in SQL.
func clearableReporter(row map[string]any) (reporterEmail, reporterID string, ok bool) {
	reporterEmail = typedString(row["reporter_email"])
	reporterID = typedString(row["reporter_id"])
	return reporterEmail, reporterID, reporterEmail != "" && reporterID != ""
}

func taskClearAuthenticatedReporterEmails(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("clearAuthenticatedReporterEmails: %w", err)
		}
	}
	var summary reporterEmailSummary
	cursor := ""
	for {
		page, lastKey, err := reporterEmailsPage(ctx, cursor, reporterEmailPageSize)
		if err != nil {
			return fmt.Errorf("clearAuthenticatedReporterEmails: page after %q: %w", cursor, err)
		}
		if len(page) == 0 {
			break
		}
		for _, rowKey := range page {
			summary.scanned++
			row, err := readKVRow(ctx, reporterEmailTable, rowKey)
			if err != nil {
				summary.failed++
				log.Printf("task clearAuthenticatedReporterEmails: read %s: %v (continuing)", rowKey, err)
				continue
			}
			if _, _, ok := clearableReporter(row); !ok {
				summary.skipped++
				continue
			}
			if err := clearReporterEmail(ctx, rowKey); err != nil {
				summary.failed++
				log.Printf("task clearAuthenticatedReporterEmails: clear %s: %v (continuing)", rowKey, err)
				continue
			}
			summary.cleared++
		}
		if len(page) < reporterEmailPageSize {
			break
		}
		cursor = lastKey
	}
	if summary.failed > 0 {
		log.Printf("task clearAuthenticatedReporterEmails: scanned=%d cleared=%d skipped=%d failed=%d",
			summary.scanned, summary.cleared, summary.skipped, summary.failed)
	} else {
		log.Printf("task clearAuthenticatedReporterEmails: scanned=%d cleared=%d skipped=%d",
			summary.scanned, summary.cleared, summary.skipped)
	}
	return nil
}

// reporterEmailsPage returns up to limit row keys after cursor, in row_key
// order, together with the last key seen. Paging by the primary key keeps the
// walk stable while rows are being patched underneath it: only reporter_email
// changes, so the order cannot move.
func reporterEmailsPage(ctx context.Context, cursor string, limit int) ([]string, string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_key FROM fluxer_kv
WHERE table_name = $1 AND ($2 = '' OR row_key > $2)
ORDER BY row_key
LIMIT $3`, reporterEmailTable, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, "", err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(keys) == 0 {
		return nil, "", nil
	}
	return keys, keys[len(keys)-1], nil
}

// clearReporterEmail sets reporter_email to JSON null, which is what Db.set(null)
// writes upstream. The rest of row_data stays byte-identical: reports carry
// legal context, and rewriting the whole document would risk dropping a field
// this worker does not know about.
func clearReporterEmail(ctx context.Context, rowKey string) error {
	res, err := db.ExecContext(ctx, `
UPDATE fluxer_kv
SET row_data = jsonb_set(row_data, '{reporter_email}', 'null'::jsonb),
    updated_at = now()
WHERE table_name = $1 AND row_key = $2
  AND jsonb_typeof(row_data->'reporter_email') <> 'null'`, reporterEmailTable, rowKey)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// decodeReporterRow is used by the tests to feed a realistic row through
// clearableReporter; it also documents the shape the api writes (typed wrappers).
func decodeReporterRow(raw []byte) (map[string]any, error) {
	var row map[string]any
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, err
	}
	return row, nil
}
