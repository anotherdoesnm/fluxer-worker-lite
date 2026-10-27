package main

// taskBulkBanFileShas ports admin_bulk/BulkBanFileShas.ts.
//
// An operator pastes a list of SHA-256 hashes; the worker puts each one into
// banned_file_shas, records one admin audit entry per hash, tells every instance
// process to reload the blocklist (a Redis publish, not a NATS message: the
// caches subscribe there), and writes a summary audit row.
//
// A malformed hash is skipped and counted instead of failing the sweep: the api
// throws for that item and the bulk runner records the failure, and the rest of
// the list still gets banned. Lower-casing happens before validation, because the
// table stores hashes lower-case and the panel accepts them in any case.

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"time"
)

const (
	bannedFileShasTable = "banned_file_shas"
	fileShaRefreshChan  = "banned_file_shas_refresh"
	// MANUAL category and BLOCK severity, straight from @fluxer/constants.
	blocklistCategoryManual = "manual"
	blocklistSeverityBlock  = 2
)

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func taskBulkBanFileShas(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("bulkBanFileShas: %w", err)
		}
	}
	adminUserID := stringField(payload, "admin_user_id")
	reason := stringField(payload, "audit_log_reason")
	if adminUserID == "" || !isDigits(adminUserID) {
		return fmt.Errorf("bulkBanFileShas: admin_user_id is required")
	}
	raw := stringSliceField(payload, "sha256_list")
	if len(raw) == 0 {
		return fmt.Errorf("bulkBanFileShas: sha256_list is empty")
	}

	banned, invalid, err := applyFileShaBans(ctx, adminUserID, reason, raw)
	if err != nil {
		return fmt.Errorf("bulkBanFileShas: %w", err)
	}
	// One publish for the whole sweep, after every row is in place: the caches
	// reload the whole list, so publishing per hash would just add churn.
	if err := valkey().Publish(ctx, fileShaRefreshChan, "refresh").Err(); err != nil {
		log.Printf("bulkBanFileShas: publish refresh: %v (rows are written, caches reload on their own schedule)", err)
	}
	metadata := [][2]string{{"count", strconv.Itoa(len(raw))}}
	if invalid > 0 {
		metadata = append(metadata, [2]string{"invalid", strconv.Itoa(invalid)})
	}
	if err := writeAdminAuditLog(ctx, adminUserID, "file_sha", "0", "bulk_ban_file_shas", reason, metadata); err != nil {
		return fmt.Errorf("bulkBanFileShas: admin audit: %w", err)
	}
	log.Printf("bulkBanFileShas: requested=%d banned=%d invalid=%d", len(raw), banned, invalid)
	return nil
}

// applyFileShaBans writes the rows and per-hash audit entries. It is separate
// from the task so the database half can be tested without NATS or Redis.
func applyFileShaBans(ctx context.Context, adminUserID, reason string, raw []string) (banned, invalid int, err error) {
	for _, candidate := range raw {
		sha := lowerASCII(candidate)
		if !sha256Pattern.MatchString(sha) {
			invalid++
			log.Printf("bulkBanFileShas: skipping malformed sha %q", candidate)
			continue
		}
		row := map[string]any{
			"sha256_hex":   sha,
			"category":     blocklistCategoryManual,
			"severity":     blocklistSeverityBlock,
			"content_type": nil,
			"source_url":   nil,
			"added_at":     kvDate(time.Now()),
			"added_by":     bigintValue(adminUserID),
			"notes":        nil,
		}
		key := stringSeg(sha)
		if err := putKV(ctx, bannedFileShasTable, key, key, row, ""); err != nil {
			return banned, invalid, fmt.Errorf("ban %s: %w", sha, err)
		}
		banned++
		// Per-hash audit, as upstream: the summary row says "a sweep happened",
		// these rows say what exactly was banned and when.
		if err := writeAdminAuditLog(ctx, adminUserID, "file_sha", "0", "ban_file_sha", reason, [][2]string{{"sha256", sha}}); err != nil {
			log.Printf("bulkBanFileShas: %s: audit: %v (row written)", sha, err)
		}
	}
	return banned, invalid, nil
}

// lowerASCII lower-cases A-Z only. Hashes are hex, so this is enough, and it
// avoids the locale-independent surprises of strings.ToLower on non-ASCII input.
func lowerASCII(s string) string {
	out := []byte(s)
	for i, b := range out {
		if b >= 'A' && b <= 'Z' {
			out[i] = b + ('a' - 'A')
		}
	}
	return string(out)
}
