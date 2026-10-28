package main

// The admin reindex task: RefreshSearchIndex.ts.
//
// It is the "rebuild everything" button in the admin panel. The panel polls a
// valkey key for progress (`index_refresh_status:<job_id>`, an hour of TTL) and
// this worker is the only thing that writes it, so the shape of that JSON has to
// match what the panel reads: status, index_type, phase, total, indexed and the
// timestamps.
//
// Scope: the types whose indexes this worker can actually fill — guilds, users,
// discovery, channel messages, guild members and threads. The reports and audit
// log indexes are not ported (no document builders, no index here): asking for
// one logs why and finishes with zero instead of failing the job, so "reindex
// all" still works on an instance that has neither.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	indexRefreshStatusPrefix = "index_refresh_status:"
	indexRefreshAllLockKey   = "index_refresh:all"
	indexRefreshTTL          = time.Hour
	refreshPageSize          = 1000
)

var refreshIndexTypes = map[string]bool{
	"guilds": true, "users": true, "reports": true, "audit_logs": true,
	"channel_messages": true, "guild_members": true, "discovery": true,
	"threads": true, "all": true,
}

// refreshProgress writes the panel's progress document. A failure to write it
// must not abort the reindex: the documents are the point, the bar is cosmetic.
func refreshProgress(ctx context.Context, jobID, indexType string, indexed, total int, phase string, status string) {
	if valkey() == nil {
		return
	}
	doc := map[string]any{
		"status":     status,
		"index_type": indexType,
		"indexed":    indexed,
		"total":      total,
		"updated_at": time.Now().UTC().Format(time.RFC3339),
	}
	if phase != "" {
		doc["phase"] = phase
	}
	switch status {
	case "in_progress":
		doc["started_at"] = time.Now().UTC().Format(time.RFC3339)
	case "completed":
		doc["completed_at"] = time.Now().UTC().Format(time.RFC3339)
	case "failed":
		doc["failed_at"] = time.Now().UTC().Format(time.RFC3339)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if err := valkey().Set(ctx, indexRefreshStatusPrefix+jobID, string(raw), indexRefreshTTL).Err(); err != nil {
		log.Printf("refreshSearchIndex: progress %s: %v", jobID, err)
	}
}

// taskRefreshSearchIndex ports RefreshSearchIndex.ts: dispatch on index_type,
// report progress, and turn a failure into a "failed" progress document.
func taskRefreshSearchIndex(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return err
		}
	}
	indexType := stringField(payload, "index_type")
	jobID := stringField(payload, "job_id")
	adminUserID := stringField(payload, "admin_user_id")
	if !refreshIndexTypes[indexType] {
		return fmt.Errorf("refreshSearchIndex: unknown index_type %q", indexType)
	}
	if jobID == "" || adminUserID == "" {
		return errors.New("refreshSearchIndex: job_id and admin_user_id are required")
	}
	guildID := stringField(payload, "guild_id")
	switch indexType {
	case "channel_messages", "guild_members", "threads":
		if guildID == "" {
			return fmt.Errorf("refreshSearchIndex: index_type %q needs guild_id", indexType)
		}
	}

	// Only one "all" run at a time; the panel's "reindex everything" button must
	// not start a second pass over the same tables.
	if indexType == "all" && valkey() != nil {
		claimed, err := valkey().SetNX(ctx, indexRefreshAllLockKey, jobID, indexRefreshTTL).Result()
		if err != nil {
			return err
		}
		if !claimed {
			log.Printf("refreshSearchIndex: another reindex holds %s, stopping job %s", indexRefreshAllLockKey, jobID)
			refreshProgress(ctx, jobID, indexType, 0, 0, "", "completed")
			return nil
		}
		defer func() {
			if valkey() != nil {
				_ = valkey().Del(context.Background(), indexRefreshAllLockKey).Err()
			}
		}()
	}

	refreshProgress(ctx, jobID, indexType, 0, 0, "", "in_progress")
	indexed, err := runRefreshSearchIndex(ctx, jobID, indexType, guildID)
	if err != nil {
		refreshProgress(ctx, jobID, indexType, indexed, indexed, "", "failed")
		return err
	}
	refreshProgress(ctx, jobID, indexType, indexed, indexed, "", "completed")
	log.Printf("refreshSearchIndex: job=%s type=%s guild=%s indexed=%d", jobID, indexType, guildID, indexed)
	return nil
}

func runRefreshSearchIndex(ctx context.Context, jobID, indexType, guildID string) (int, error) {
	report := func(indexed, total int, phase string) {
		refreshProgress(ctx, jobID, indexType, indexed, total, phase, "in_progress")
	}
	switch indexType {
	case "guilds":
		return reindexGuilds(ctx, report)
	case "users":
		return reindexUsers(ctx, report)
	case "discovery":
		return reindexDiscovery(ctx, report)
	case "channel_messages":
		return reindexGuildMessages(ctx, guildID, report)
	case "guild_members":
		return reindexGuildMembers(ctx, guildID, report)
	case "threads":
		return reindexThreads(ctx, guildID, report)
	case "audit_logs":
		return reindexAuditLogs(ctx, report)
	case "reports":
		return reindexReports(ctx, report)
	case "all":
		return reindexAll(ctx, jobID, report)
	default:
		return 0, fmt.Errorf("refreshSearchIndex: unhandled index_type %q", indexType)
	}
}

func deleteAllSearchDocs(ctx context.Context, uid string) error {
	store := &pgSearchStore{}
	if err := store.ensure(ctx); err != nil {
		return err
	}
	_, err := store.deleteAll(ctx, uid)
	return err
}

func deleteSearchDocsByFilter(ctx context.Context, uid, filter string) error {
	store := &pgSearchStore{}
	if err := store.ensure(ctx); err != nil {
		return err
	}
	_, err := store.deleteByFilter(ctx, uid, filter)
	return err
}

// reindexGuilds rebuilds the guilds index from the guild rows. Discovery context
// is not attached here: the discovery pass owns those fields, and a guild that is
// not discoverable must not carry them.
func reindexGuilds(ctx context.Context, report func(int, int, string)) (int, error) {
	if err := deleteAllSearchDocs(ctx, searchIndexGuilds); err != nil {
		return 0, err
	}
	writer := searchWriterFor()
	total := 0
	cursor := ""
	for {
		rows, err := readGuildPage(ctx, cursor, refreshPageSize)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			break
		}
		docs := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			docs = append(docs, guildSearchDoc(row, nil))
		}
		if _, err := writer.upsert(ctx, searchIndexGuilds, docs); err != nil {
			return total, err
		}
		total += len(docs)
		cursor = typedString(rows[len(rows)-1]["guild_id"])
		report(total, total, "guilds")
		if len(rows) < refreshPageSize {
			break
		}
	}
	return total, nil
}

func readGuildPage(ctx context.Context, afterGuildID string, limit int) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data
FROM fluxer_kv
WHERE table_name = 'guilds' AND row_data->'guild_id'->>'value' > $1
ORDER BY row_data->'guild_id'->>'value'
LIMIT $2`, afterGuildID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		data := map[string]any{}
		if err := json.Unmarshal(raw, &data); err != nil {
			continue
		}
		out = append(out, data)
	}
	return out, rows.Err()
}

func reindexUsers(ctx context.Context, report func(int, int, string)) (int, error) {
	if err := deleteAllSearchDocs(ctx, searchIndexUsers); err != nil {
		return 0, err
	}
	writer := searchWriterFor()
	total := 0
	cursor := ""
	for {
		users, err := readUserPage(ctx, cursor, refreshPageSize)
		if err != nil {
			return total, err
		}
		if len(users) == 0 {
			break
		}
		docs := make([]map[string]any, 0, len(users))
		for _, user := range users {
			docs = append(docs, userSearchDoc(user))
		}
		if _, err := writer.upsert(ctx, searchIndexUsers, docs); err != nil {
			return total, err
		}
		total += len(docs)
		cursor = typedString(users[len(users)-1]["user_id"])
		report(total, total, "users")
		if len(users) < refreshPageSize {
			break
		}
	}
	return total, nil
}

func reindexDiscovery(ctx context.Context, report func(int, int, string)) (int, error) {
	applications, err := readApprovedDiscoveryApplications(ctx)
	if err != nil {
		return 0, err
	}
	if len(applications) == 0 {
		return 0, nil
	}
	writer := searchWriterFor()
	synced := 0
	for start := 0; start < len(applications); start += discoveryBatchSize {
		end := start + discoveryBatchSize
		if end > len(applications) {
			end = len(applications)
		}
		batch := applications[start:end]
		guildIDs := make([]string, 0, len(batch))
		for _, app := range batch {
			if id := typedString(app["guild_id"]); id != "" {
				guildIDs = append(guildIDs, id)
			}
		}
		guilds, err := readGuildsByID(ctx, guildIDs)
		if err != nil {
			return synced, err
		}
		docs := make([]map[string]any, 0, len(batch))
		for _, app := range batch {
			if guild, ok := guilds[typedString(app["guild_id"])]; ok {
				docs = append(docs, guildSearchDoc(guild, app))
			}
		}
		if len(docs) > 0 {
			if _, err := writer.upsert(ctx, searchIndexGuilds, docs); err != nil {
				return synced, err
			}
			synced += len(docs)
		}
		report(synced, len(applications), "discovery")
	}
	return synced, nil
}

func reindexGuildMembers(ctx context.Context, guildID string, report func(int, int, string)) (int, error) {
	if err := deleteSearchDocsByFilter(ctx, searchIndexGuildMembers, "guildId = "+guildID); err != nil {
		return 0, err
	}
	writer := searchWriterFor()
	cursor := ""
	total := 0
	for {
		members, err := readGuildMemberPage(ctx, guildID, cursor, refreshPageSize)
		if err != nil {
			return total, err
		}
		if len(members) == 0 {
			break
		}
		userIDs := make([]string, 0, len(members))
		for _, m := range members {
			if id := typedString(m["user_id"]); id != "" {
				userIDs = append(userIDs, id)
			}
		}
		users, err := readUsersByID(ctx, userIDs)
		if err != nil {
			return total, err
		}
		docs := make([]map[string]any, 0, len(members))
		for _, m := range members {
			userID := typedString(m["user_id"])
			if user, ok := users[userID]; ok {
				docs = append(docs, guildMemberSearchDoc(guildID, m, user))
			}
		}
		if len(docs) > 0 {
			if _, err := writer.upsert(ctx, searchIndexGuildMembers, docs); err != nil {
				return total, err
			}
			total += len(docs)
		}
		cursor = typedString(members[len(members)-1]["user_id"])
		report(total, total, "guild_members")
		if len(members) < refreshPageSize {
			break
		}
	}
	if err := patchGuildMembersIndexedAt(ctx, guildID); err != nil {
		log.Printf("refreshSearchIndex: %s: members_indexed_at: %v", guildID, err)
	}
	return total, nil
}

func reindexThreads(ctx context.Context, guildID string, report func(int, int, string)) (int, error) {
	// Clearing the marker is what makes the backfill run again; without it the
	// task would return immediately and the index would stay stale.
	if _, err := db.ExecContext(ctx, `
UPDATE fluxer_kv
SET row_data = row_data - 'search_backfilled_at', updated_at = now()
WHERE table_name = $1 AND row_key = $2`, guildThreadStateTable, bigintSeg(guildID)); err != nil {
		return 0, err
	}
	if err := taskBackfillThreadSearch(ctx, map[string]any{"guildId": guildID}); err != nil {
		return 0, err
	}
	report(0, 0, "threads")
	return 0, nil
}

// reindexGuildMessages rebuilds the message documents of one guild: every channel
// of the guild plus its threads, one indexChannelMessages pass per channel.
func reindexGuildMessages(ctx context.Context, guildID string, report func(int, int, string)) (int, error) {
	channels, err := listGuildChannelIDs(ctx, guildID)
	if err != nil {
		return 0, err
	}
	threads, err := listGuildThreadIDs(ctx, guildID)
	if err != nil {
		return 0, err
	}
	channels = append(channels, threads...)
	writer := searchWriterFor()
	total := 0
	for i, channelID := range channels {
		if err := deleteSearchDocsByFilter(ctx, searchIndexMessages, "channelId = "+channelID); err != nil {
			return total, err
		}
		indexed, err := indexChannelHistory(ctx, writer, channelID, guildID)
		if err != nil {
			return total, err
		}
		total += indexed
		report(i+1, len(channels), "channel_messages")
	}
	return total, nil
}

func listGuildChannelIDs(ctx context.Context, guildID string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'channel_id'->>'value'
FROM fluxer_kv
WHERE table_name = 'channels' AND row_data->'guild_id'->>'value' = $1
ORDER BY row_key`, guildID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id *string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != nil && *id != "" {
			out = append(out, *id)
		}
	}
	return out, rows.Err()
}

// reindexAll is the "everything" branch: the global indexes, then the per-guild
// ones, then every channel that has messages at all (DMs included, which is why
// it does not walk guilds for that part).
func reindexAll(ctx context.Context, jobID string, report func(int, int, string)) (int, error) {
	total := 0
	global := []struct {
		name string
		fn   func(context.Context, func(int, int, string)) (int, error)
	}{
		{"users", reindexUsers},
		{"guilds", reindexGuilds},
		{"discovery", reindexDiscovery},
		{"audit_logs", reindexAuditLogs},
		{"reports", reindexReports},
	}
	guildCount := 0
	for _, step := range global {
		n, err := step.fn(ctx, func(indexed, all int, phase string) {
			report(indexed, all, phase)
		})
		if err != nil {
			return total, err
		}
		if step.name == "guilds" {
			guildCount = n
		}
		total += n
	}

	guildIDs, err := listAllGuildIDs(ctx)
	if err != nil {
		return total, err
	}
	for i, guildID := range guildIDs {
		if err := deleteSearchDocsByFilter(ctx, searchIndexGuildMembers, "guildId = "+guildID); err != nil {
			return total, err
		}
		n, err := reindexGuildMembers(ctx, guildID, func(int, int, string) {})
		if err != nil {
			return total, err
		}
		total += n
		m, err := reindexThreads(ctx, guildID, func(int, int, string) {})
		if err != nil {
			return total, err
		}
		total += m
		report(i+1, guildCount, "guild_members")
	}

	channels, err := listChannelsWithMessages(ctx)
	if err != nil {
		return total, err
	}
	writer := searchWriterFor()
	for i, channelID := range channels {
		n, err := indexChannelHistory(ctx, writer, channelID, "")
		if err != nil {
			return total, err
		}
		total += n
		report(i+1, len(channels), "messages")
	}
	return total, nil
}

func listAllGuildIDs(ctx context.Context) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'guild_id'->>'value' FROM fluxer_kv WHERE table_name = 'guilds' ORDER BY row_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id *string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != nil && *id != "" {
			out = append(out, *id)
		}
	}
	return out, rows.Err()
}

// listChannelsWithMessages reads the message partitions directly: every channel
// that has at least one message is exactly one partition key, and its first
// segment is the channel id.
func listChannelsWithMessages(ctx context.Context) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT DISTINCT split_part(partition_key, chr(31), 1) AS channel
FROM fluxer_kv WHERE table_name = 'messages'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	var out []string
	for rows.Next() {
		var segment string
		if err := rows.Scan(&segment); err != nil {
			return nil, err
		}
		id := bigintValueFromSegment(segment)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, rows.Err()
}

// bigintValueFromSegment unwraps the KV bigint segment
// {"__fluxer_type":"bigint","value":"…"} without a full JSON parse.
func bigintValueFromSegment(segment string) string {
	const marker = `"value":"`
	idx := indexOf(segment, marker)
	if idx < 0 {
		if isDigits(segment) {
			return segment
		}
		return ""
	}
	rest := segment[idx+len(marker):]
	end := indexOf(rest, `"`)
	if end < 0 {
		return ""
	}
	value := rest[:end]
	if !isDigits(value) {
		return ""
	}
	return value
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

// reindexAuditLogs rebuilds the admin audit log index from admin_audit_logs.
func reindexAuditLogs(ctx context.Context, report func(int, int, string)) (int, error) {
	if err := deleteAllSearchDocs(ctx, searchIndexAuditLogs); err != nil {
		return 0, err
	}
	writer := searchWriterFor()
	total := 0
	cursor := ""
	for {
		rows, err := readKVPage(ctx, "admin_audit_logs", cursor, refreshPageSize)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			break
		}
		docs := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			docs = append(docs, auditLogSearchDoc(row.data))
		}
		if _, err := writer.upsert(ctx, searchIndexAuditLogs, docs); err != nil {
			return total, err
		}
		total += len(docs)
		cursor = rows[len(rows)-1].key
		report(total, total, "audit_logs")
		if len(rows) < refreshPageSize {
			break
		}
	}
	return total, nil
}

// reindexReports rebuilds the report index from iar_submissions.
func reindexReports(ctx context.Context, report func(int, int, string)) (int, error) {
	if err := deleteAllSearchDocs(ctx, searchIndexReports); err != nil {
		return 0, err
	}
	writer := searchWriterFor()
	total := 0
	cursor := ""
	for {
		rows, err := readKVPage(ctx, "iar_submissions", cursor, refreshPageSize)
		if err != nil {
			return total, err
		}
		if len(rows) == 0 {
			break
		}
		docs := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			docs = append(docs, reportSearchDoc(row.data))
		}
		if _, err := writer.upsert(ctx, searchIndexReports, docs); err != nil {
			return total, err
		}
		total += len(docs)
		cursor = rows[len(rows)-1].key
		report(total, total, "reports")
		if len(rows) < refreshPageSize {
			break
		}
	}
	return total, nil
}

// kvRow is one row of a KV table, in row-key order.
type kvRow struct {
	key  string
	data map[string]any
}

// readKVPage pages any KV table by row key. The tables that are indexed as a whole
// (admin audit logs, reports) share the same shape, so one helper covers both.
func readKVPage(ctx context.Context, table, afterKey string, limit int) ([]kvRow, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_key, row_data
FROM fluxer_kv
WHERE table_name = $1 AND row_key > $2
ORDER BY row_key
LIMIT $3`, table, afterKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []kvRow
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			return nil, err
		}
		data := map[string]any{}
		if err := json.Unmarshal(raw, &data); err != nil {
			continue
		}
		out = append(out, kvRow{key: key, data: data})
	}
	return out, rows.Err()
}
