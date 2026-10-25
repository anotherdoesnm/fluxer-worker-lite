package main

// Mention fanout: who gets a mention, and what the mention does to their state.
//
// Two tasks, ported from the release's sources:
//
//	handleMentions      src/api/worker/tasks/HandleMentions.ts
//	handleMentionChunk  src/api/worker/tasks/HandleMentionChunk.ts
//
// The split exists because "who is mentioned" is not a local question in a guild:
// @everyone, @here and role mentions have to be resolved against the guild's
// member list and channel permissions, and a large guild has more members than
// one job should carry. So handleMentions asks the gateway for pages of users and
// publishes handleMentionChunk jobs of 250 users each; the chunk is what actually
// writes state.
//
// What a chunk writes, and why both halves matter:
//
//   - the read state's mention_count goes up, so the client shows an unread
//     mention badge on that channel. Upstream guards this: a mention is only
//     counted if the message is newer than what the user already read, otherwise
//     scrolling back through old messages would keep re-incrementing the badge.
//   - for guild channels, a row goes into recent_mentions (and its by-guild
//     twin), which is the "mentions" inbox. Both carry a 7-day TTL.
//
// Suppression is per-user guild settings: someone with suppress_everyone set does
// not get an @everyone mention counted. A missing settings row means "no
// suppression", which is also upstream's default (`settings?.suppressEveryone ??
// false`).

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	// mentionChunkSize is MENTION_CHUNK_SIZE: how many users one chunk job carries.
	mentionChunkSize = 250
	// mentionSourcePageSize is MENTION_SOURCE_PAGE_SIZE: how many users the
	// gateway resolves per page.
	mentionSourcePageSize = 5000
	// mentionSettingsConcurrency bounds the per-user guild-settings lookups.
	mentionSettingsConcurrency = 16
	// mentionChunkKeyDigest length, truncated like upstream's job key.
	mentionChunkKeyLen = 16
	// recentMentionTTL is the tables' defaultTtlSeconds (7 days).
	recentMentionTTL = 7 * 24 * time.Hour

	readStateTable         = "read_states"
	recentMentionsTable    = "recent_mentions"
	recentMentionsByGuild  = "recent_mentions_by_guild"
	userGuildSettingsTable = "user_guild_settings"

	mentionSourcesSubject = "rpc.gateway.guild.resolve_mention_sources_page"

	// ReadStateFlags: IS_GUILD_CHANNEL | IS_THREAD, the marker a thread mention
	// writes (ReadStateRepository.threadReadStateMarker).
	threadReadStateFlags = 1 | 2
)

type mentionPayload struct {
	ChannelID       string   `json:"channelId"`
	MessageID       string   `json:"messageId"`
	AuthorID        string   `json:"authorId"`
	GuildID         string   `json:"guildId"`
	MentionHere     bool     `json:"mentionHere"`
	MentionEveryone bool     `json:"mentionEveryone"`
	MentionUserIDs  []string `json:"mentionUserIds"`
	MentionRoleIDs  []string `json:"mentionRoleIds"`
}

type mentionEntry struct {
	UserID   string `json:"userId"`
	Direct   bool   `json:"direct"`
	Role     bool   `json:"role"`
	Everyone bool   `json:"everyone"`
}

type mentionChunkPayload struct {
	ChannelID  string         `json:"channelId"`
	MessageID  string         `json:"messageId"`
	GuildID    string         `json:"guildId"`
	ChunkIndex int            `json:"chunkIndex"`
	ChunkCount int            `json:"chunkCount"`
	Thread     bool           `json:"thread"`
	Mentions   []mentionEntry `json:"mentions"`
}

// decodePayload turns the decoded job payload into a typed struct. The job comes
// from JSON, so a round trip is the cheapest faithful conversion and it also
// rejects payloads with the wrong shape instead of silently zero-filling them.
func decodePayload[T any](payload map[string]any, out *T) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// ---------- KV key helpers ----------
//
// fluxer_kv addresses a row by (table_name, row_key): row_key is every primary
// key part encoded and joined with the 0x1f separator, and partition_key is the
// first one or two parts, because the table is partitioned by those. Bigint parts
// are stored as the {"__fluxer_type":"bigint","value":"…"} marker object, exactly
// as the API writes them — the text has to match byte for byte, not just parse
// to the same value.

var publishPrefix string

func jobPrefixForPublish() string {
	if publishPrefix != "" {
		return publishPrefix
	}
	return env("JOB_PREFIX", "jobs.go.")
}

func kvRowKey(parts ...string) string { return strings.Join(parts, kvSeparator) }

func kvBigintJSON(id string) map[string]any {
	return map[string]any{"__fluxer_type": "bigint", "value": id}
}

// ---------- handleMentions ----------

// resolveMentionSourcesPage mirrors GatewayService.resolveMentionSourcesPage: one
// page of "who did this message mention", as decided by the gateway.
// The gateway answers RPCs with an envelope: {"ok":true,"result":{…}}. Reading
// the page object straight off the reply body yields an empty page and the
// handler concludes "nobody was mentioned", which is the quietest possible way to
// be wrong.
type gatewayMentionSourcesReply struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
	Result struct {
		Mentions []struct {
			UserID   string `json:"user_id"`
			Direct   bool   `json:"direct"`
			Role     bool   `json:"role"`
			Everyone bool   `json:"everyone"`
		} `json:"mentions"`
		NextCursor *string `json:"next_cursor"`
	} `json:"result"`
}

func resolveMentionSourcesPage(ctx context.Context, p mentionPayload, cursor string) ([]mentionEntry, string, error) {
	req := map[string]any{
		"guild_id":         p.GuildID,
		"channel_id":       p.ChannelID,
		"author_id":        p.AuthorID,
		"mention_everyone": p.MentionEveryone,
		"mention_here":     p.MentionHere,
		"role_ids":         p.MentionRoleIDs,
		"user_ids":         p.MentionUserIDs,
		"limit":            mentionSourcePageSize,
	}
	if req["role_ids"] == nil {
		req["role_ids"] = []string{}
	}
	if req["user_ids"] == nil {
		req["user_ids"] = []string{}
	}
	if cursor != "" {
		req["cursor"] = cursor
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, "", err
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	msg, err := workerNATS().RequestWithContext(reqCtx, mentionSourcesSubject, body)
	if err != nil {
		return nil, "", fmt.Errorf("gateway mention sources: %w", err)
	}
	var reply gatewayMentionSourcesReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return nil, "", fmt.Errorf("gateway mention sources: %w", err)
	}
	if !reply.OK {
		return nil, "", fmt.Errorf("gateway mention sources: %s", strings.TrimSpace(reply.Error))
	}
	out := make([]mentionEntry, 0, len(reply.Result.Mentions))
	for _, m := range reply.Result.Mentions {
		out = append(out, mentionEntry{UserID: m.UserID, Direct: m.Direct, Role: m.Role, Everyone: m.Everyone})
	}
	next := ""
	if reply.Result.NextCursor != nil {
		next = *reply.Result.NextCursor
	}
	return out, next, nil
}

// mentionChunkJobKey is mentionChunkJobKey from upstream: the chunk's identity is
// its content, so a retried fanout publishes the same message id and JetStream
// deduplicates it instead of mentioning everybody twice.
func mentionChunkJobKey(messageID string, entries []mentionEntry) string {
	h := sha256.New()
	for _, e := range entries {
		bit := func(b bool) string {
			if b {
				return "1"
			}
			return "0"
		}
		fmt.Fprintf(h, "%s:%s:%s:%s\n", e.UserID, bit(e.Direct), bit(e.Role), bit(e.Everyone))
	}
	return "mention-chunk:" + messageID + ":" + hex.EncodeToString(h.Sum(nil))[:mentionChunkKeyLen]
}

func publishMentionChunks(ctx context.Context, js nats.JetStreamContext, prefix string, p mentionPayload, entries []mentionEntry, thread bool) error {
	for i := 0; i < len(entries); i += mentionChunkSize {
		end := i + mentionChunkSize
		if end > len(entries) {
			end = len(entries)
		}
		chunk := entries[i:end]
		payload := mentionChunkPayload{
			ChannelID:  p.ChannelID,
			MessageID:  p.MessageID,
			GuildID:    p.GuildID,
			ChunkIndex: i / mentionChunkSize,
			ChunkCount: (len(entries) + mentionChunkSize - 1) / mentionChunkSize,
			Thread:     thread,
			Mentions:   chunk,
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		var asMap map[string]any
		if err := json.Unmarshal(raw, &asMap); err != nil {
			return err
		}
		if err := publishJobWithID(js, prefix, "handleMentionChunk", asMap, mentionChunkJobKey(p.MessageID, chunk)); err != nil {
			return err
		}
	}
	return nil
}

// channelGuildID reads the channel row to answer "is this a guild channel" when
// the payload does not say. Upstream loads the channel anyway (it needs the
// message's stored mention fields); we already have them in the payload, so only
// the guild id is worth a query.
func channelGuildID(ctx context.Context, channelID string) (string, error) {
	if err := openDB(); err != nil {
		return "", err
	}
	key := kvBigint(channelID)
	var guild string
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(row_data->'guild_id'->>'value', '')
		FROM fluxer_kv WHERE table_name = 'channels' AND row_key = $1`, key).Scan(&guild)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return guild, nil
}

func taskHandleMentions(ctx context.Context, payload map[string]any) error {
	var p mentionPayload
	if err := decodePayload(payload, &p); err != nil {
		return fmt.Errorf("handleMentions payload: %w", err)
	}
	if p.ChannelID == "" || p.MessageID == "" {
		return fmt.Errorf("handleMentions: channelId and messageId are required")
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return err
	}
	if p.GuildID == "" {
		guild, err := channelGuildID(ctx, p.ChannelID)
		if err != nil {
			return fmt.Errorf("handleMentions: channel lookup: %w", err)
		}
		p.GuildID = guild
	}

	if p.GuildID == "" {
		// DM or group DM: the mentioned users are the payload, minus the author.
		// Upstream does not resolve anything through the gateway here.
		seen := map[string]bool{}
		entries := make([]mentionEntry, 0, len(p.MentionUserIDs))
		for _, uid := range p.MentionUserIDs {
			if uid == p.AuthorID || seen[uid] {
				continue
			}
			seen[uid] = true
			entries = append(entries, mentionEntry{UserID: uid, Direct: true})
		}
		if len(entries) == 0 {
			return nil
		}
		return publishMentionChunks(ctx, js, jobPrefixForPublish(), p, entries, false)
	}

	// Guild: ask the gateway page by page. The cursor must advance, or the loop
	// would spin: upstream treats a non-advancing cursor as a hard error.
	cursor := ""
	total := 0
	for page := 0; ; page++ {
		entries, next, err := resolveMentionSourcesPage(ctx, p, cursor)
		if err != nil {
			return err
		}
		total += len(entries)
		if len(entries) > 0 {
			if err := publishMentionChunks(ctx, js, jobPrefixForPublish(), p, entries, false); err != nil {
				return err
			}
		}
		if next == "" {
			break
		}
		if next == cursor {
			return fmt.Errorf("handleMentions: gateway cursor did not advance")
		}
		cursor = next
		if page > 1000 {
			return fmt.Errorf("handleMentions: too many pages (%d), refusing to loop", page)
		}
	}
	if total == 0 {
		log.Printf("handleMentions: message %s in %s mentioned nobody", p.MessageID, p.ChannelID)
	}
	return nil
}

// ---------- handleMentionChunk ----------

type guildSuppression struct {
	suppressEveryone bool
	suppressRoles    bool
}

func guildSuppressions(ctx context.Context, guildID string, entries []mentionEntry) (map[string]guildSuppression, error) {
	need := make([]mentionEntry, 0, len(entries))
	for _, e := range entries {
		if e.Everyone || e.Role {
			need = append(need, e)
		}
	}
	out := make(map[string]guildSuppression, len(need))
	if len(need) == 0 {
		return out, nil
	}
	if err := openDB(); err != nil {
		return nil, err
	}
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, mentionSettingsConcurrency)
	var wg sync.WaitGroup
	for _, e := range need {
		wg.Add(1)
		sem <- struct{}{}
		go func(userID string) {
			defer wg.Done()
			defer func() { <-sem }()
			var everyone, roles bool
			err := db.QueryRowContext(ctx, `
				SELECT COALESCE((row_data->>'suppress_everyone')::boolean, false),
				       COALESCE((row_data->>'suppress_roles')::boolean, false)
				FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
				userGuildSettingsTable, kvRowKey(kvBigint(userID), kvBigint(guildID))).Scan(&everyone, &roles)
			if err != nil && err != sql.ErrNoRows {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			out[userID] = guildSuppression{suppressEveryone: everyone, suppressRoles: roles}
			mu.Unlock()
		}(e.UserID)
	}
	wg.Wait()
	return out, firstErr
}

// incrementReadState applies the mention to one user's read state.
//
// The guard is the important part: increment only when this message is newer than
// what the user has already read. Upstream expresses it as a conditional CQL
// update with a read-modify-write fallback; in the Postgres KV table the same
// intent is an UPDATE with a predicate on the stored message_id.
func incrementReadState(ctx context.Context, tx *sql.Tx, userID, channelID, messageID string, marker *readStateMarker) (bool, error) {
	key := kvRowKey(kvBigint(userID), kvBigint(channelID))
	markerJSON := []byte("{}")
	if marker != nil {
		markerJSON, _ = json.Marshal(map[string]any{
			"flags":    marker.flags,
			"guild_id": kvBigintJSON(marker.guildID),
		})
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE fluxer_kv
		SET row_data = jsonb_set(row_data, '{mention_count}',
		                        to_jsonb(COALESCE((row_data->>'mention_count')::int, 0) + 1)) || $2::jsonb,
		    updated_at = now()
		WHERE table_name = $1 AND row_key = $3
		  AND COALESCE((row_data->'message_id'->>'value')::numeric, 0) < $4::numeric`,
		readStateTable, markerJSON, key, messageID)
	if err != nil {
		return false, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return false, err
	} else if n > 0 {
		return true, nil
	}

	// No row matched: either the read state does not exist yet, or its message_id
	// is already at or past this message. Insert the baseline row only in the
	// first case — mentionBaseline() is the channel's own id, which is older than
	// any message in it.
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM fluxer_kv WHERE table_name = $1 AND row_key = $2)`,
		readStateTable, key).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	baseline, ok := compareDecimal(channelID, messageID)
	if !ok || baseline >= 0 {
		// channelID >= messageID: nothing this old can be a new mention.
		return false, nil
	}
	row := map[string]any{
		"user_id":       kvBigintJSON(userID),
		"channel_id":    kvBigintJSON(channelID),
		"message_id":    kvBigintJSON(channelID),
		"mention_count": 1,
	}
	if marker != nil {
		row["flags"] = marker.flags
		row["guild_id"] = kvBigintJSON(marker.guildID)
	}
	body, err := json.Marshal(row)
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO fluxer_kv (table_name, partition_key, row_key, row_data, expires_at, updated_at)
		VALUES ($1, $2, $2, $3::jsonb, NULL, now())
		ON CONFLICT (table_name, row_key) DO NOTHING`,
		readStateTable, key, string(body)); err != nil {
		return false, err
	}
	return true, nil
}

type readStateMarker struct {
	flags   int
	guildID string
}

// compareDecimal compares two unsigned decimal strings without converting them to
// anything smaller than they are.
func compareDecimal(a, b string) (int, bool) {
	ta := strings.TrimLeft(a, "0")
	tb := strings.TrimLeft(b, "0")
	if ta == "" {
		ta = "0"
	}
	if tb == "" {
		tb = "0"
	}
	if !isDigits(ta) || !isDigits(tb) {
		return 0, false
	}
	if len(ta) != len(tb) {
		if len(ta) < len(tb) {
			return -1, true
		}
		return 1, true
	}
	return strings.Compare(ta, tb), true
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0
}

func insertRecentMention(ctx context.Context, tx *sql.Tx, userID, channelID, messageID, guildID string, isEveryone, isRole bool) error {
	expires := time.Now().Add(recentMentionTTL)
	base := map[string]any{
		"user_id":     kvBigintJSON(userID),
		"channel_id":  kvBigintJSON(channelID),
		"message_id":  kvBigintJSON(messageID),
		"is_everyone": isEveryone,
		"is_role":     isRole,
	}
	byUser := map[string]any{}
	for k, v := range base {
		byUser[k] = v
	}
	byUser["guild_id"] = kvBigintJSON(guildID)
	body, err := json.Marshal(byUser)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO fluxer_kv (table_name, partition_key, row_key, row_data, expires_at, updated_at)
		VALUES ($1, $2, $2, $3::jsonb, $4, now())
		ON CONFLICT (table_name, row_key) DO UPDATE
		SET row_data = EXCLUDED.row_data, expires_at = EXCLUDED.expires_at, updated_at = now()`,
		recentMentionsTable, kvRowKey(kvBigint(userID), kvBigint(messageID)), string(body), expires); err != nil {
		return err
	}

	byGuild := map[string]any{
		"user_id":     kvBigintJSON(userID),
		"guild_id":    kvBigintJSON(guildID),
		"message_id":  kvBigintJSON(messageID),
		"channel_id":  kvBigintJSON(channelID),
		"is_everyone": isEveryone,
		"is_role":     isRole,
	}
	body, err = json.Marshal(byGuild)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO fluxer_kv (table_name, partition_key, row_key, row_data, expires_at, updated_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, now())
		ON CONFLICT (table_name, row_key) DO UPDATE
		SET row_data = EXCLUDED.row_data, expires_at = EXCLUDED.expires_at, updated_at = now()`,
		recentMentionsByGuild,
		kvRowKey(kvBigint(userID), kvBigint(guildID)),
		kvRowKey(kvBigint(userID), kvBigint(guildID), kvBigint(messageID)),
		string(body), expires)
	return err
}

func taskHandleMentionChunk(ctx context.Context, payload map[string]any) error {
	var p mentionChunkPayload
	if err := decodePayload(payload, &p); err != nil {
		return fmt.Errorf("handleMentionChunk payload: %w", err)
	}
	if p.ChannelID == "" || p.MessageID == "" || len(p.Mentions) == 0 {
		return nil
	}
	// Merge duplicates: a user can be mentioned directly and by role at once, and
	// the chunk must count one mention, not two.
	merged := map[string]mentionEntry{}
	order := make([]string, 0, len(p.Mentions))
	for _, e := range p.Mentions {
		if e.UserID == "" {
			continue
		}
		if prev, ok := merged[e.UserID]; ok {
			prev.Direct = prev.Direct || e.Direct
			prev.Role = prev.Role || e.Role
			prev.Everyone = prev.Everyone || e.Everyone
			merged[e.UserID] = prev
			continue
		}
		merged[e.UserID] = e
		order = append(order, e.UserID)
	}
	entries := ordered(merged, order)
	if len(entries) == 0 {
		return nil
	}

	var suppress map[string]guildSuppression
	if p.GuildID != "" {
		var err error
		suppress, err = guildSuppressions(ctx, p.GuildID, entries)
		if err != nil {
			return fmt.Errorf("handleMentionChunk: guild settings: %w", err)
		}
	}

	var marker *readStateMarker
	if p.Thread && p.GuildID != "" {
		marker = &readStateMarker{flags: threadReadStateFlags, guildID: p.GuildID}
	}

	counted := make([]mentionEntry, 0, len(entries))
	suppressed := 0
	for _, e := range entries {
		s := suppress[e.UserID]
		if e.Direct {
			counted = append(counted, e)
			continue
		}
		if e.Everyone && !s.suppressEveryone {
			counted = append(counted, e)
			continue
		}
		if e.Role && !s.suppressRoles {
			counted = append(counted, e)
			continue
		}
		suppressed++
	}
	if len(counted) == 0 {
		return nil
	}

	if err := openDB(); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	applied := 0
	for _, e := range counted {
		ok, err := incrementReadState(ctx, tx, e.UserID, p.ChannelID, p.MessageID, marker)
		if err != nil {
			return fmt.Errorf("handleMentionChunk: read state for %s: %w", e.UserID, err)
		}
		if !ok {
			continue
		}
		applied++
		if p.GuildID != "" {
			s := suppress[e.UserID]
			if err := insertRecentMention(ctx, tx, e.UserID, p.ChannelID, p.MessageID, p.GuildID,
				e.Everyone && !s.suppressEveryone, e.Role && !s.suppressRoles); err != nil {
				return fmt.Errorf("handleMentionChunk: recent mention for %s: %w", e.UserID, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Printf("handleMentionChunk: message %s in %s: %d mentioned, %d suppressed, %d counted (chunk %d)",
		p.MessageID, p.ChannelID, len(entries), suppressed, applied, p.ChunkIndex)
	return nil
}

// ordered returns the merged entries in first-seen order (stable output makes the
// log lines and the chunk contents reproducible).
func ordered(m map[string]mentionEntry, order []string) []mentionEntry {
	out := make([]mentionEntry, 0, len(m))
	seen := make(map[string]bool, len(m))
	for _, id := range order {
		if seen[id] {
			continue
		}
		seen[id] = true
		if e, ok := m[id]; ok {
			out = append(out, e)
		}
	}
	return out
}
