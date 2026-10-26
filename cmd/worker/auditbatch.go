package main

// batchGuildAuditLogMessageDeletes: collapse runs of consecutive MESSAGE_DELETE
// audit entries into one MESSAGE_BULK_DELETE entry.
//
// Ported from src/api/guild/services/GuildAuditLogService.ts of release
// 2026.1026.151750, where the shape is:
//
//   - every time a MESSAGE_DELETE (72) entry is written, createAuditLog schedules
//     this job with jobKey batch-audit-log-message-deletes:<guild>:<window>,
//     runAt = (window+2) * 30s and maxAttempts 3, so the API debounces a burst of
//     deletions into one job two 30-second windows later;
//   - the job calls batchRecentMessageDeleteLogs(guildId, 250): it first asks for
//     the newest 250 MESSAGE_DELETE entries and gives up when there are fewer
//     than two (a single deletion is not worth a batch);
//   - then it reads the newest 250 entries of *every* action type, because
//     "consecutive" is defined inside that window, not across the table;
//   - grouping: walk the list in log_id DESC order; a run continues while the
//     entry is MESSAGE_DELETE, carries options.channel_id, and both user_id and
//     channel_id match the run; anything else closes it. Runs of two or more
//     become a batch;
//   - a batch becomes one MESSAGE_BULK_DELETE (73) entry with options
//     {channel_id, count} (plus the internal thread_scoped flag when any original
//     carried it), the originals are deleted from all four tables and the new row
//     is inserted into all four in one transaction, all with a 45-day TTL;
//   - finally GUILD_AUDIT_LOG_ENTRY_CREATE is dispatched, with __thread_scoped set
//     on the payload when the batch is thread scoped. A dispatch failure is logged
//     and ignored upstream, so it must not fail the job here either.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	auditActionMessageDelete     = 72
	auditActionMessageBulkDelete = 73

	auditLogListLimit   = 250
	auditLogTTLSeconds  = 3888000 // 45 days, AUDIT_LOG_TTL_SECONDS upstream
	auditLogThreadScope = "thread_scoped"

	auditLogsTable           = "guild_audit_logs_v2"
	auditLogsByUserTable     = "guild_audit_logs_v2_by_user"
	auditLogsByActionTable   = "guild_audit_logs_v2_by_action"
	auditLogsByUserActTable  = "guild_audit_logs_v2_by_user_action"
	auditLogEntryCreateEvent = "GUILD_AUDIT_LOG_ENTRY_CREATE"
)

// auditLogEntry is the part of a guild_audit_logs_v2 row this task reasons about.
type auditLogEntry struct {
	LogID        string
	UserID       string
	ActionType   int64
	ChannelID    string
	ThreadScoped bool
}

// auditDeleteGroup is one run of consecutive deletions worth collapsing.
type auditDeleteGroup struct {
	UserID    string
	ChannelID string
	Logs      []auditLogEntry
}

func taskBatchGuildAuditLogMessageDeletes(ctx context.Context, payload map[string]any) error {
	guildID := typedString(payload["guildId"])
	if guildID == "" {
		guildID = typedString(payload["guild_id"])
	}
	if guildID == "" {
		return errors.New("batchGuildAuditLogMessageDeletes: payload has no guildId")
	}

	recent, err := listGuildAuditLogs(ctx, guildID, auditActionMessageDelete, auditLogListLimit)
	if err != nil {
		return err
	}
	if len(recent) < 2 {
		return nil
	}

	all, err := listGuildAuditLogs(ctx, guildID, 0, auditLogListLimit)
	if err != nil {
		return err
	}
	groups := groupConsecutiveMessageDeletes(all)
	if len(groups) == 0 {
		return nil
	}

	nc := workerNATS()
	ids, err := snowflakeBatch(nc, len(groups))
	if err != nil {
		return err
	}
	for i, group := range groups {
		if err := batchOneDeleteGroup(ctx, nc, guildID, ids[i], group); err != nil {
			return err
		}
	}
	return nil
}

// groupConsecutiveMessageDeletes mirrors findConsecutiveMessageDeleteGroups: the
// input is in log_id DESC order, and only runs of at least two entries are
// returned.
func groupConsecutiveMessageDeletes(logs []auditLogEntry) []auditDeleteGroup {
	var groups []auditDeleteGroup
	var current *auditDeleteGroup
	close := func() {
		if current != nil && len(current.Logs) >= 2 {
			groups = append(groups, *current)
		}
		current = nil
	}
	for _, entry := range logs {
		if entry.ActionType != auditActionMessageDelete || entry.ChannelID == "" {
			close()
			continue
		}
		if current != nil && current.UserID == entry.UserID && current.ChannelID == entry.ChannelID {
			current.Logs = append(current.Logs, entry)
			continue
		}
		close()
		current = &auditDeleteGroup{
			UserID:    entry.UserID,
			ChannelID: entry.ChannelID,
			Logs:      []auditLogEntry{entry},
		}
	}
	close()
	return groups
}

func batchOneDeleteGroup(ctx context.Context, nc *nats.Conn, guildID, logID string, group auditDeleteGroup) error {
	threadScoped := false
	for _, entry := range group.Logs {
		if entry.ThreadScoped {
			threadScoped = true
			break
		}
	}

	options := [][2]string{
		{"channel_id", group.ChannelID},
		{"count", strconv.Itoa(len(group.Logs))},
	}
	if threadScoped {
		options = append(options, [2]string{auditLogThreadScope, "1"})
	}
	row := map[string]any{
		"log_id":      bigintValue(logID),
		"guild_id":    bigintValue(guildID),
		"user_id":     bigintValue(group.UserID),
		"target_id":   nil,
		"action_type": auditActionMessageBulkDelete,
		"reason":      nil,
		"options":     auditOptionsValue(options),
		"changes":     nil,
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for _, entry := range group.Logs {
		for _, key := range auditLogKeys(guildID, entry) {
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, key.table, key.rowKey); err != nil {
				return fmt.Errorf("delete %s %s: %w", key.table, entry.LogID, err)
			}
		}
	}

	expiresAt := time.Now().Add(time.Duration(auditLogTTLSeconds) * time.Second).UTC()
	for _, key := range auditLogKeys(guildID, auditLogEntry{
		LogID:      logID,
		UserID:     group.UserID,
		ActionType: auditActionMessageBulkDelete,
	}) {
		if err := insertAuditLogTx(ctx, tx, key.table, key.partition, key.rowKey, row, expiresAt); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	data := map[string]any{
		"id":          logID,
		"action_type": auditActionMessageBulkDelete,
		"user_id":     group.UserID,
		"target_id":   nil,
		"options": map[string]any{
			"channel_id": group.ChannelID,
			"count":      len(group.Logs),
		},
		"guild_id": guildID,
	}
	if threadScoped {
		data[auditLogThreadScope] = guildID
	}
	// nc is nil when the SQL half of this task runs under test without a NATS
	// connection; in production it is always set.
	if nc != nil {
		if err := dispatchGuild(nc, guildID, auditLogEntryCreateEvent, data); err != nil {
			// Upstream swallows this: the entry is already stored, only the live
			// update of an open audit log view is lost.
			log.Printf("batchGuildAuditLogMessageDeletes: guild %s: dispatch: %v", guildID, err)
		}
	}
	log.Printf("batchGuildAuditLogMessageDeletes: guild %s: collapsed %d deletions by %s in %s into %s",
		guildID, len(group.Logs), group.UserID, group.ChannelID, logID)
	return nil
}

type auditLogKey struct {
	table     string
	partition string
	rowKey    string
}

// auditLogKeys returns the four rows one audit entry occupies. Every one of these
// tables uses the full primary key as its partition key (they are declared without
// an explicit partitionKey upstream), so partition == row_key.
func auditLogKeys(guildID string, entry auditLogEntry) []auditLogKey {
	action := strconv.FormatInt(entry.ActionType, 10)
	base := bigintSeg(guildID) + kvSep
	byUser := base + bigintSeg(entry.UserID) + kvSep
	byAction := base + action + kvSep
	byUserAction := byUser + action + kvSep
	byLog := base + bigintSeg(entry.LogID)
	return []auditLogKey{
		{auditLogsTable, byLog, byLog},
		{auditLogsByUserTable, byUser + bigintSeg(entry.LogID), byUser + bigintSeg(entry.LogID)},
		{auditLogsByActionTable, byAction + bigintSeg(entry.LogID), byAction + bigintSeg(entry.LogID)},
		{auditLogsByUserActTable, byUserAction + bigintSeg(entry.LogID), byUserAction + bigintSeg(entry.LogID)},
	}
}

func insertAuditLogTx(ctx context.Context, tx *sql.Tx, table, partition, rowKey string, row map[string]any, expiresAt time.Time) error {
	body, err := json.Marshal(row)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO fluxer_kv (table_name, partition_key, row_key, row_data, expires_at, updated_at)
VALUES ($1, $2, $3, $4::jsonb, $5, now())
ON CONFLICT (table_name, row_key) DO UPDATE SET
	partition_key = EXCLUDED.partition_key,
	row_data      = EXCLUDED.row_data,
	expires_at    = EXCLUDED.expires_at,
	updated_at    = now()`,
		table, partition, rowKey, string(body), expiresAt)
	if err != nil {
		return fmt.Errorf("insert %s %s: %w", table, rowKey, err)
	}
	return nil
}

// auditOptionsValue encodes an options Map the way the KV layer stores it:
// {"__fluxer_type":"map","value":[[k,v],...]}. An empty list stays null, matching
// `options: metadataMap.size > 0 ? metadataMap : null`.
func auditOptionsValue(pairs [][2]string) any {
	if len(pairs) == 0 {
		return nil
	}
	value := make([]any, 0, len(pairs))
	for _, pair := range pairs {
		value = append(value, []any{pair[0], pair[1]})
	}
	return map[string]any{"__fluxer_type": "map", "value": value}
}

// auditOption reads one key out of a stored options Map.
func auditOption(options any, key string) (string, bool) {
	typed, ok := options.(map[string]any)
	if !ok || typed["__fluxer_type"] != "map" {
		return "", false
	}
	pairs, ok := typed["value"].([]any)
	if !ok {
		return "", false
	}
	for _, raw := range pairs {
		pair, ok := raw.([]any)
		if !ok || len(pair) != 2 {
			continue
		}
		name, ok := pair[0].(string)
		if !ok || name != key {
			continue
		}
		switch typedValue := pair[1].(type) {
		case string:
			return typedValue, true
		case float64:
			return strconv.FormatFloat(typedValue, 'f', -1, 64), true
		}
	}
	return "", false
}

// listGuildAuditLogs reads the newest entries of one guild in log_id DESC order.
// An actionType of 0 means "every action type".
//
// The API picks one of four tables depending on the filters; here the guild table
// is enough, because the four hold the same rows and the index that a filter would
// use only changes the plan, not the answer. Ordering relies on the same property
// the KV layer relies on: row_key is a fixed-width prefix (guild id) followed by
// the snowflake, so byte order equals numeric order for rows of one guild.
func listGuildAuditLogs(ctx context.Context, guildID string, actionType int64, limit int) ([]auditLogEntry, error) {
	prefix := bigintSeg(guildID) + kvSep
	query := `SELECT row_key, row_data::text FROM fluxer_kv
WHERE table_name = $1 AND row_key LIKE $2 || '%'`
	args := []any{auditLogsTable, prefix}
	if actionType != 0 {
		query += ` AND row_data->>'action_type' = $3`
		args = append(args, strconv.FormatInt(actionType, 10))
	}
	query += ` ORDER BY row_key COLLATE "C" DESC LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []auditLogEntry
	for rows.Next() {
		var rowKey, raw string
		if err := rows.Scan(&rowKey, &raw); err != nil {
			return nil, err
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(raw), &data); err != nil {
			return nil, fmt.Errorf("audit log %s: %w", rowKey, err)
		}
		entry := auditLogEntry{
			LogID:      typedString(data["log_id"]),
			UserID:     typedString(data["user_id"]),
			ActionType: intField(data, "action_type"),
		}
		if channelID, ok := auditOption(data["options"], "channel_id"); ok {
			entry.ChannelID = channelID
		}
		if _, ok := auditOption(data["options"], auditLogThreadScope); ok {
			entry.ThreadScoped = true
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}
