package main

// taskBulkUpdateGuildFeatures ports admin_bulk/BulkUpdateGuildFeatures.ts.
//
// An operator picks guilds and a set of features to add and to remove; the worker
// applies that to each guild row, tells the guild's clients with a GUILD_UPDATE
// carrying the mapped response, records the change in the guild audit log, and
// finally writes one admin audit row summarising the sweep.
//
// What is not ported: the job's progress reporting (`helpers.reportProgress`).
// The api keeps progress on the job row for the admin panel's live view; this
// worker has no such writer yet, so a long sweep simply runs to the end. The
// summary audit row still lands, which is what survives the run.

import (
	"context"
	"fmt"
	"log"
)

func taskBulkUpdateGuildFeatures(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("bulkUpdateGuildFeatures: %w", err)
		}
	}
	guildIDs := stringSliceField(payload, "guild_ids")
	addFeatures := stringSliceField(payload, "add_features")
	removeFeatures := stringSliceField(payload, "remove_features")
	adminUserID := stringField(payload, "admin_user_id")
	reason := stringField(payload, "audit_log_reason")
	if adminUserID == "" || !isDigits(adminUserID) {
		return fmt.Errorf("bulkUpdateGuildFeatures: admin_user_id is required")
	}
	if len(guildIDs) == 0 {
		return fmt.Errorf("bulkUpdateGuildFeatures: guild_ids is empty")
	}

	changed := 0
	failed := 0
	for _, guildID := range guildIDs {
		if !isDigits(guildID) {
			failed++
			log.Printf("bulkUpdateGuildFeatures: skipping malformed guild id %q", guildID)
			continue
		}
		ok, err := applyGuildFeatureChange(ctx, guildID, addFeatures, removeFeatures)
		if err != nil {
			failed++
			log.Printf("bulkUpdateGuildFeatures: guild %s: %v", guildID, err)
			continue
		}
		if ok {
			changed++
		}
	}

	// The summary row is the record of the sweep; a failure to write it must fail
	// the task, because an admin action without a trace is the thing this table
	// exists to prevent.
	metadata := [][2]string{
		{"guild_count", fmt.Sprintf("%d", len(guildIDs))},
		{"add_features", joinComma(addFeatures)},
		{"remove_features", joinComma(removeFeatures)},
	}
	if failed > 0 {
		metadata = append(metadata, [2]string{"failed", fmt.Sprintf("%d", failed)})
	}
	if err := writeAdminAuditLog(ctx, adminUserID, "guild", guildIDs[0], "bulk_update_guild_features", reason, metadata); err != nil {
		return fmt.Errorf("bulkUpdateGuildFeatures: admin audit: %w", err)
	}
	log.Printf("bulkUpdateGuildFeatures: guilds=%d changed=%d failed=%d", len(guildIDs), changed, failed)
	return nil
}

// applyGuildFeatureChange rewrites one guild's features and notifies its clients.
// A false result means the guild does not exist, or the change was already in
// place and nothing had to be written.
func applyGuildFeatureChange(ctx context.Context, guildID string, add, remove []string) (bool, error) {
	rowKey := bigintSeg(guildID)
	row, err := readKV(ctx, guildsTable, rowKey)
	if err != nil {
		return false, fmt.Errorf("read guild: %w", err)
	}
	if row == nil {
		log.Printf("bulkUpdateGuildFeatures: guild %s does not exist", guildID)
		return false, nil
	}
	before := stringSet(row["features"])
	after := applyFeatureDiff(before, add, remove)
	if equalStringSets(before, after) {
		return false, nil
	}

	var featuresValue any
	if len(after) > 0 {
		featuresValue = map[string]any{"__fluxer_type": "set", "value": stringSliceToAny(after)}
	}
	if _, err := db.ExecContext(ctx, `
UPDATE fluxer_kv
SET row_data = row_data || jsonb_build_object('features', $2::jsonb), updated_at = now()
WHERE table_name = $1 AND row_key = $3`, guildsTable, string(mustJSON(featuresValue)), rowKey); err != nil {
		return false, fmt.Errorf("patch features: %w", err)
	}

	if nc := workerNATS(); nc != nil {
		if updated, err := readKV(ctx, guildsTable, rowKey); err == nil && updated != nil {
			if err := dispatchGuild(nc, guildID, "GUILD_UPDATE", guildResponse(updated)); err != nil {
				log.Printf("bulkUpdateGuildFeatures: guild %s: dispatch: %v (change applied)", guildID, err)
			}
		}
	}
	if err := recordGuildFeatureChange(ctx, guildID, before, after); err != nil {
		log.Printf("bulkUpdateGuildFeatures: guild %s: audit log: %v (change applied)", guildID, err)
	}
	return true, nil
}

// applyFeatureDiff adds, then removes, so an id listed on both sides ends up
// removed — the same order the api uses, where the removal wins.
func applyFeatureDiff(before, add, remove []string) []string {
	seen := make(map[string]struct{}, len(before)+len(add))
	out := make([]string, 0, len(before)+len(add))
	for _, f := range before {
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		out = append(out, f)
	}
	for _, f := range add {
		if _, ok := seen[f]; ok {
			continue
		}
		seen[f] = struct{}{}
		out = append(out, f)
	}
	kept := out[:0]
	for _, f := range out {
		if containsString(remove, f) {
			continue
		}
		kept = append(kept, f)
	}
	return kept
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, item := range a {
		if !containsString(b, item) {
			return false
		}
	}
	return true
}

func stringSliceToAny(in []string) []any {
	out := make([]any, 0, len(in))
	for _, s := range in {
		out = append(out, s)
	}
	return out
}

func joinComma(in []string) string {
	out := ""
	for i, s := range in {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}
