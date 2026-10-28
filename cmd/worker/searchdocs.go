package main

// Search documents: which indexes exist, how a row becomes a document, and how a
// document reaches the store.
//
// The search surface in searchapi.go serves queries; this file is the writing
// side, used by the indexing tasks. Both talk to the same two tables, so the
// worker is the only writer and the shim the only reader — there is no HTTP hop
// between them, which is also why the tasks do not need Meilisearch's async task
// model: an upsert is finished when it returns.
//
// Index definitions mirror MEILISEARCH_INDEX_DEFINITIONS in the release this
// worker is pinned to (uid, primary key, searchable attributes). The attributes
// matter twice: they decide what the shim puts into the tsvector, and the filter
// fields the API sends (channelId, authorId, createdAt, ...) must exist on the
// documents at all — see below for the one deliberate deviation.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	searchIndexMessages     = "messages"
	searchIndexGuilds       = "guilds"
	searchIndexUsers        = "users"
	searchIndexThreads      = "threads"
	searchIndexGuildMembers = "guild_members"
	searchIndexAuditLogs    = "audit_logs"
	searchIndexReports      = "reports"
)

// searchIndexSpec is the Go mirror of MeilisearchIndexDefinition.
type searchIndexSpec struct {
	UID        string
	PrimaryKey string
	Searchable []string
	Filterable []string
	Sortable   []string
}

var searchIndexSpecs = map[string]searchIndexSpec{
	searchIndexMessages: {
		UID: searchIndexMessages, PrimaryKey: "id",
		Searchable: []string{"content", "embedContent"},
		Filterable: []string{
			"id", "createdAt", "channelId", "guildId", "authorId", "authorType",
			"mentionEveryone", "isPinned", "mentionedUserIds", "hasLink", "hasEmbed",
			"hasPoll", "hasFile", "hasVideo", "hasImage", "hasSound", "hasSticker",
			"hasForward", "embedTypes", "embedProviders", "linkHostnames",
			"attachmentFilenames", "attachmentExtensions",
		},
		Sortable: []string{"createdAt", "id"},
	},
	searchIndexThreads: {
		UID: searchIndexThreads, PrimaryKey: "id",
		Searchable: []string{"name"},
		Filterable: []string{
			"id", "guildId", "parentId", "type", "archived", "appliedTagIds",
			"createdAt", "idSequence",
		},
		Sortable: []string{"lastMessageAt", "archivedAt", "createdAt", "idSequence", "id"},
	},
	searchIndexGuilds: {
		UID: searchIndexGuilds, PrimaryKey: "id",
		Searchable: []string{"name", "discoveryTags", "vanityUrlCode", "discoveryDescription"},
		Filterable: []string{
			"ownerId", "verificationLevel", "mfaLevel", "nsfwLevel", "features",
			"isDiscoverable", "discoveryCategory", "discoveryPrimaryLanguage", "discoveryTags",
		},
		Sortable: []string{"createdAt", "memberCount", "id"},
	},
	searchIndexUsers: {
		UID: searchIndexUsers, PrimaryKey: "id",
		Searchable: []string{"username", "email", "id"},
		Filterable: []string{
			"isBot", "isSystem", "emailVerified", "emailBounced", "premiumType",
			"tempBannedUntil", "pendingDeletionAt", "acls", "createdAt",
		},
		Sortable: []string{"createdAt", "lastActiveAt", "id"},
	},
	searchIndexAuditLogs: {
		UID: searchIndexAuditLogs, PrimaryKey: "id",
		Searchable: []string{"action", "targetType", "targetId", "auditLogReason"},
		Filterable: []string{"adminUserId", "targetType", "targetId", "action"},
		Sortable:   []string{"createdAt", "id"},
	},
	searchIndexReports: {
		UID: searchIndexReports, PrimaryKey: "id",
		Searchable: []string{"category", "additionalInfo", "reportedGuildName", "reportedChannelName"},
		Filterable: []string{
			"reporterId", "status", "reportType", "category", "reason",
			"reportedUserId", "reportedWebhookId", "reportedGuildId",
			"reportedMessageId", "reportedChannelId", "guildContextId",
			"resolvedByAdminId", "resolvedAt",
		},
		Sortable: []string{"createdAt", "reportedAt", "resolvedAt", "id"},
	},
	searchIndexGuildMembers: {
		UID: searchIndexGuildMembers, PrimaryKey: "id",
		Searchable: []string{"username", "usernameSearch", "discriminator", "globalName", "nickname", "userId"},
		Filterable: []string{
			"guildId", "roleIds", "joinedAt", "joinSourceType", "sourceInviteCode",
			"userCreatedAt", "isBot",
		},
		Sortable: []string{"joinedAt", "id"},
	},
}

// searchWriter is the store the indexing tasks write through. It is an interface
// so the tasks can be tested without a database, exactly like the shim's store.
type searchWriter interface {
	upsert(ctx context.Context, uid string, docs []map[string]any) (int, error)
	deleteIDs(ctx context.Context, uid string, ids []string) (int, error)
}

type pgSearchWriter struct {
	prepared map[string]bool
}

func (w *pgSearchWriter) upsert(ctx context.Context, uid string, docs []map[string]any) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	store := &pgSearchStore{}
	if err := store.ensure(ctx); err != nil {
		return 0, err
	}
	if err := ensureSearchIndex(ctx, uid, w); err != nil {
		return 0, err
	}
	raws := make([]json.RawMessage, 0, len(docs))
	for _, doc := range docs {
		raw, err := json.Marshal(doc)
		if err != nil {
			return 0, fmt.Errorf("marshal %s document: %w", uid, err)
		}
		raws = append(raws, raw)
	}
	return store.upsertDocuments(ctx, uid, raws)
}

func (w *pgSearchWriter) deleteIDs(ctx context.Context, uid string, ids []string) (int, error) {
	store := &pgSearchStore{}
	if err := store.ensure(ctx); err != nil {
		return 0, err
	}
	return store.deleteByIDs(ctx, uid, ids)
}

// ensureSearchIndex creates the index row and its searchable attributes once per
// process. The attributes are written as settings, which is what the shim reads
// when it builds a document's text: without them it would index every field,
// including ids and timestamps.
func ensureSearchIndex(ctx context.Context, uid string, w *pgSearchWriter) error {
	spec, ok := searchIndexSpecs[uid]
	if !ok {
		return fmt.Errorf("search: unknown index %q", uid)
	}
	if w.prepared == nil {
		w.prepared = map[string]bool{}
	}
	if w.prepared[uid] {
		return nil
	}
	store := &pgSearchStore{}
	exists, err := store.indexExists(ctx, uid)
	if err != nil {
		return err
	}
	if !exists {
		if err := store.createIndex(ctx, uid, spec.PrimaryKey); err != nil {
			return err
		}
	}
	attrs, err := json.Marshal(spec.Searchable)
	if err != nil {
		return err
	}
	if err := store.putSetting(ctx, uid, "searchableAttributes", attrs); err != nil {
		return err
	}
	w.prepared[uid] = true
	return nil
}

// searchWriterFor returns the writer the handlers use.
func searchWriterFor() *pgSearchWriter { return &pgSearchWriter{} }

// --- message documents -----------------------------------------------------

// messageSearchDoc ports convertToSearchableMessage from MessageSearchSerializer.ts.
//
// Deviation from upstream, on purpose: upstream stores `guildId: null` for every
// message, so a filter on guildId can never match. The channel row is right here,
// so the document carries the real guild id; a guild-scoped search then works
// instead of silently returning nothing.
func messageSearchDoc(msg map[string]any, guildID string) map[string]any {
	id := typedString(msg["message_id"])
	channelID := typedString(msg["channel_id"])
	authorID := typedString(msg["author_id"])

	acc := newMessageAccumulator()
	content := typedString(msg["content"])
	embeds := sliceOfMaps(msg["embeds"])
	attachments := sliceOfMaps(msg["attachments"])
	acc.addContent(content, embeds, attachments, typedStringSlice(msg["mentioned_user_ids"]), len(sliceOfMaps(msg["stickers"])))

	isForward := false
	if ref, ok := msg["reference"].(map[string]any); ok {
		if intOrZero(ref["type"]) == 1 {
			isForward = true
			for _, snap := range sliceOfMaps(msg["message_snapshots"]) {
				acc.addContent(
					typedString(snap["content"]),
					sliceOfMaps(snap["embeds"]),
					sliceOfMaps(snap["attachments"]),
					typedStringSlice(snap["mentioned_user_ids"]),
					len(sliceOfMaps(snap["stickers"])),
				)
			}
		}
	}

	authorType := "user"
	if typedString(msg["webhook_id"]) != "" {
		authorType = "webhook"
	}
	doc := map[string]any{
		"id":                   id,
		"channelId":            channelID,
		"guildId":              nullIfEmpty(guildID),
		"authorId":             nullIfEmpty(authorID),
		"authorType":           authorType,
		"content":              nullIfEmpty(content),
		"createdAt":            snowflakeSeconds(id),
		"editedAt":             secondsOrNil(msg["edited_timestamp"]),
		"isPinned":             typedString(msg["pinned_timestamp"]) != "",
		"mentionedUserIds":     acc.mentionedUserIDs,
		"mentionEveryone":      boolField(msg, "mention_everyone"),
		"hasLink":              acc.hasLink,
		"hasEmbed":             acc.hasEmbed,
		"hasPoll":              false,
		"hasFile":              acc.hasFile,
		"hasVideo":             acc.hasVideo,
		"hasImage":             acc.hasImage,
		"hasSound":             acc.hasSound,
		"hasSticker":           acc.hasSticker,
		"hasForward":           isForward,
		"embedTypes":           acc.embedTypes,
		"embedProviders":       acc.embedProviders,
		"embedContent":         acc.embedContent,
		"linkHostnames":        acc.linkHostnames,
		"attachmentFilenames":  acc.attachmentFilenames,
		"attachmentExtensions": acc.attachmentExtensions,
	}
	return doc
}

// messageAccumulator mirrors the IndexAccumulator in MessageSearchSerializer.ts.
type messageAccumulator struct {
	contentChunks        []string
	embedTypes           []string
	embedProviders       []string
	embedContent         []string
	linkHostnames        []string
	attachmentFilenames  []string
	attachmentExtensions []string
	mentionedUserIDs     []string
	hasLink              bool
	hasEmbed             bool
	hasFile              bool
	hasImage             bool
	hasVideo             bool
	hasSound             bool
	hasSticker           bool
}

func newMessageAccumulator() *messageAccumulator { return &messageAccumulator{} }

func (a *messageAccumulator) addContent(content string, embeds, attachments []map[string]any, mentioned []string, stickerCount int) {
	if content != "" {
		a.contentChunks = append(a.contentChunks, content)
		if hasLinkRegex.MatchString(content) {
			a.hasLink = true
		}
	}
	if len(embeds) > 0 {
		a.hasEmbed = true
		for _, embed := range embeds {
			if t := typedString(embed["type"]); t != "" {
				a.pushUnique(&a.embedTypes, t)
			}
			if provider, ok := embed["provider"].(map[string]any); ok {
				if name := typedString(provider["name"]); name != "" {
					a.pushUnique(&a.embedProviders, name)
				}
			}
			collectEmbedText(embed, &a.embedContent)
		}
	}
	if len(attachments) > 0 {
		a.hasFile = true
		for _, att := range attachments {
			contentType := strings.ToLower(typedString(att["content_type"]))
			switch {
			case strings.HasPrefix(contentType, "image/"):
				a.hasImage = true
			case strings.HasPrefix(contentType, "video/"):
				a.hasVideo = true
			case strings.HasPrefix(contentType, "audio/"):
				a.hasSound = true
			}
			filename := typedString(att["filename"])
			if filename != "" {
				a.pushUnique(&a.attachmentFilenames, filename)
				if ext := fileExtension(filename); ext != "" {
					a.pushUnique(&a.attachmentExtensions, ext)
				}
			}
		}
	}
	if stickerCount > 0 {
		a.hasSticker = true
	}
	for _, rawURL := range linkRegex.FindAllString(content, -1) {
		rawURL = trailingURLPunctuation.ReplaceAllString(rawURL, "")
		if host := urlHostname(rawURL); host != "" {
			a.pushUnique(&a.linkHostnames, host)
		}
	}
	for _, embed := range embeds {
		if host := urlHostname(typedString(embed["url"])); host != "" {
			a.pushUnique(&a.linkHostnames, host)
		}
	}
	for _, userID := range mentioned {
		if userID != "" {
			a.pushUnique(&a.mentionedUserIDs, userID)
		}
	}
}

func (a *messageAccumulator) pushUnique(sink *[]string, value string) {
	if value == "" {
		return
	}
	for _, existing := range *sink {
		if existing == value {
			return
		}
	}
	*sink = append(*sink, value)
}

// collectEmbedText walks an embed and its children, mirroring collectEmbedText.
func collectEmbedText(embed map[string]any, sink *[]string) {
	for _, key := range []string{"title", "description", "url"} {
		if v := typedString(embed[key]); v != "" {
			*sink = append(*sink, v)
		}
	}
	if author, ok := embed["author"].(map[string]any); ok {
		if v := typedString(author["name"]); v != "" {
			*sink = append(*sink, v)
		}
	}
	if provider, ok := embed["provider"].(map[string]any); ok {
		if v := typedString(provider["name"]); v != "" {
			*sink = append(*sink, v)
		}
	}
	if footer, ok := embed["footer"].(map[string]any); ok {
		if v := typedString(footer["text"]); v != "" {
			*sink = append(*sink, v)
		}
	}
	for _, field := range sliceOfMaps(embed["fields"]) {
		if v := typedString(field["name"]); v != "" {
			*sink = append(*sink, v)
		}
		if v := typedString(field["value"]); v != "" {
			*sink = append(*sink, v)
		}
	}
	for _, child := range sliceOfMaps(embed["children"]) {
		collectEmbedText(child, sink)
	}
}

// --- thread documents ------------------------------------------------------

// threadSearchDoc ports toSearchableThread from search/thread/ThreadSearchService.ts:
// the channel row supplies the name and the last message, the thread state row
// supplies guild/parent/archived/tags, and the snowflake itself supplies createdAt
// and idSequence so a cursor page stays stable.
func threadSearchDoc(channel, state map[string]any) map[string]any {
	id := typedString(channel["channel_id"])
	if id == "" {
		id = typedString(state["thread_id"])
	}
	guildID := typedString(state["guild_id"])
	parentID := typedString(state["parent_id"])
	createdAt := snowflakeMillis(id)
	lastMessageAt := createdAt
	if last := typedString(channel["last_message_id"]); last != "" {
		lastMessageAt = snowflakeMillis(last)
	}
	archivedAt := createdAt
	if ts := dateSeconds(state["archive_timestamp"]); ts != nil {
		archivedAt = int64(*ts) * 1000
	}
	doc := map[string]any{
		"id":            id,
		"guildId":       guildID,
		"parentId":      parentID,
		"type":          intOrZero(channel["type"]),
		"name":          nullIfEmpty(typedString(channel["name"])),
		"ownerId":       nullIfEmpty(typedString(channel["owner_id"])),
		"archived":      boolField(state, "archived"),
		"locked":        boolField(state, "locked"),
		"appliedTagIds": typedStringSlice(state["applied_tags"]),
		"createdAt":     createdAt,
		"idSequence":    snowflakeSequence(id),
		"lastMessageAt": lastMessageAt,
		"archivedAt":    archivedAt,
	}
	return doc
}

// --- small helpers ---------------------------------------------------------

// --- admin audit log and report documents ----------------------------------

// auditLogSearchDoc ports convertToSearchableAuditLog: the admin panel's audit log
// search. `metadata` is not part of the document upstream either, so it is not
// read here.
func auditLogSearchDoc(row map[string]any) map[string]any {
	id := typedString(row["log_id"])
	return map[string]any{
		"id":             id,
		"logId":          id,
		"adminUserId":    typedString(row["admin_user_id"]),
		"targetType":     typedString(row["target_type"]),
		"targetId":       typedString(row["target_id"]),
		"action":         typedString(row["action"]),
		"auditLogReason": nullIfEmpty(typedString(row["audit_log_reason"])),
		"createdAt":      snowflakeSeconds(id),
	}
}

// reportSearchDoc ports convertToSearchableReport over an iar_submissions row.
//
// This instance has no reports (the table is empty), so this mapping is written
// from the serializer's field list and the IARSubmission interface, not verified
// against real rows: the column names are the interface's fields in snake_case.
// When reports do arrive, the first run of refreshSearchIndex with index_type
// "reports" is what confirms it.
func reportSearchDoc(row map[string]any) map[string]any {
	id := typedString(row["report_id"])
	reportedAt := int64(0)
	if ts := dateSeconds(row["reported_at"]); ts != nil {
		reportedAt = *ts
	}
	var resolvedAt any
	if ts := dateSeconds(row["resolved_at"]); ts != nil {
		resolvedAt = *ts
	}
	reporter := typedString(row["reporter_id"])
	if reporter == "" {
		reporter = "anonymous"
	}
	return map[string]any{
		"id":                  id,
		"reporterId":          reporter,
		"reportedAt":          reportedAt,
		"status":              intOrZero(row["status"]),
		"reportType":          intOrZero(row["report_type"]),
		"category":            typedString(row["category"]),
		"reason":              typedString(row["reason"]),
		"additionalInfo":      nullIfEmpty(typedString(row["additional_info"])),
		"reportedUserId":      nullIfEmpty(typedString(row["reported_user_id"])),
		"reportedWebhookId":   nullIfEmpty(typedString(row["reported_webhook_id"])),
		"reportedGuildId":     nullIfEmpty(typedString(row["reported_guild_id"])),
		"reportedGuildName":   nullIfEmpty(typedString(row["reported_guild_name"])),
		"reportedMessageId":   nullIfEmpty(typedString(row["reported_message_id"])),
		"reportedChannelId":   nullIfEmpty(typedString(row["reported_channel_id"])),
		"reportedChannelName": nullIfEmpty(typedString(row["reported_channel_name"])),
		"guildContextId":      nullIfEmpty(typedString(row["guild_context_id"])),
		"resolvedAt":          resolvedAt,
		"resolvedByAdminId":   nullIfEmpty(typedString(row["resolved_by_admin_id"])),
		"publicComment":       nullIfEmpty(typedString(row["public_comment"])),
		"createdAt":           snowflakeSeconds(id),
	}
}

// --- small helpers ---------------------------------------------------------

// readChannelByID reads a channel row. Channels are NOT keyed by the bare
// channel id: the row key is bigint(channel_id) + \x1f + a trailing boolean
// segment, so a lookup by bigintSeg alone silently finds nothing. Querying the
// row's own channel_id column is the form that works for every channel kind.
func readChannelByID(ctx context.Context, channelID string) (map[string]any, error) {
	var raw []byte
	err := db.QueryRowContext(ctx, `
SELECT row_data FROM fluxer_kv
WHERE table_name = 'channels' AND row_data->'channel_id'->>'value' = $1
LIMIT 1`, channelID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// sliceOfMaps is the typed view of a JSON array of objects: rows in fluxer_kv
// store embeds, attachments and stickers that way.
func sliceOfMaps(v any) []map[string]any {
	list, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(list))
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// typedStringSlice reads a KV list of ids. Rows store them as typed bigint
// objects ({"value": "42", "__fluxer_type": "bigint"}) but a plain string is
// accepted too, which keeps the same helper usable for tags and mentions.
func typedStringSlice(v any) []string {
	// A KV set is stored as {"value": [...], "__fluxer_type": "set"}; unwrap it
	// first so callers can pass either form.
	if m, ok := v.(map[string]any); ok {
		return typedStringSlice(m["value"])
	}
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s := typedString(item); s != "" {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	default:
		return nil
	}
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// snowflakeMillis is the snowflake epoch shift, in milliseconds.
func snowflakeMillis(id string) int64 {
	if n, ok := parseSnowflake(id); ok {
		return (n >> snowflakeTimestampBit) + snowflakeEpoch
	}
	return 0
}

// snowflakeSeconds is what the message documents store for createdAt: the API's
// filters compare seconds (it divides the snowflake timestamp by 1000).
func snowflakeSeconds(id string) int64 {
	return snowflakeMillis(id) / 1000
}

// snowflakeSequence is the low 22 bits of the snowflake, the thread cursor's
// tie-breaker inside one millisecond.
func snowflakeSequence(id string) int64 {
	if n, ok := parseSnowflake(id); ok {
		return n & ((1 << snowflakeTimestampBit) - 1)
	}
	return 0
}

func parseSnowflake(id string) (int64, bool) {
	if id == "" || !isDigits(id) {
		return 0, false
	}
	var n int64
	for _, r := range id {
		n = n*10 + int64(r-'0')
		if n < 0 {
			return 0, false
		}
	}
	return n, true
}

// secondsOrNil is the document-side form of dateSeconds: a number, or nil so the
// JSON document carries null rather than a missing field.
func secondsOrNil(v any) any {
	if ts := dateSeconds(v); ts != nil {
		return *ts
	}
	return nil
}

// dateSeconds reads a date-typed KV value and returns unix seconds, or nil when
// the field is absent or unparseable.
func dateSeconds(v any) *int64 {
	if s := typedString(v); s != "" {
		if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
			unix := ts.Unix()
			return &unix
		}
	}
	return nil
}

func fileExtension(filename string) string {
	idx := strings.LastIndex(filename, ".")
	if idx < 0 || idx == len(filename)-1 {
		return ""
	}
	ext := strings.ToLower(filename[idx+1:])
	if len(ext) > 10 {
		return ""
	}
	return ext
}
