package main

// Admin audit log rows.
//
// The admin panel records every action an operator takes in admin_audit_logs:
// who did it, what was touched, which action name, an optional reason, and a map
// of key/value metadata that the panel renders as a table. The worker tasks that
// exist to serve the panel (the bulk operations) must write the same row, or the
// action happens with no trace in the one place people look for traces.
//
// Attribution (request IP, user agent, path) comes from a request context the api
// has and a worker job does not, so the metadata written here is only what the
// task itself knows; upstream behaves the same way for jobs without that context.

import (
	"context"
	"fmt"
	"time"
)

const adminAuditLogsTable = "admin_audit_logs"

// writeAdminAuditLog stores one admin action. metadata is ordered pairs because
// the panel keeps insertion order (it reads as a table, not a dictionary).
func writeAdminAuditLog(ctx context.Context, adminUserID, targetType, targetID, action string, reason string, metadata [][2]string) error {
	logID, err := nextSnowflakeID()
	if err != nil {
		return fmt.Errorf("admin audit log: snowflake: %w", err)
	}
	pairs := make([]any, 0, len(metadata))
	for _, kv := range metadata {
		pairs = append(pairs, []any{kv[0], kv[1]})
	}
	var reasonValue any
	if reason != "" {
		reasonValue = reason
	}
	row := map[string]any{
		"log_id":           bigintValue(logID),
		"admin_user_id":    bigintValue(adminUserID),
		"target_type":      targetType,
		"target_id":        bigintValue(targetID),
		"action":           action,
		"audit_log_reason": reasonValue,
		"metadata":         map[string]any{"__fluxer_type": "map", "value": pairs},
		"created_at":       kvDate(time.Now()),
	}
	return putKV(ctx, adminAuditLogsTable, bigintSeg(logID), bigintSeg(logID), row, "")
}

// stringSliceField reads an array-of-strings payload field. Jobs carry JSON, so
// every element arrives as an any; anything that is not a string is ignored
// rather than silently turned into an empty feature name.
func stringSliceField(m map[string]any, key string) []string {
	raw, ok := m[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}
