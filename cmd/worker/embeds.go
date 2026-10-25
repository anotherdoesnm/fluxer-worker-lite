package main

// extractEmbeds: link previews (unfurl).
//
// Ported from src/api/worker/tasks/ExtractEmbeds.ts of release 2026.1025.35621.
// The job arrives as jobs.extractEmbeds with
// {channelId, messageId, guildId, nsfwMode, expectedContentHash}, where the hash
// is sha256(message.content) and exists so a job queued before an edit cannot
// stamp previews onto newer text.
//
// Three external things are involved, and none of them is HTTP:
//
//	svc.unfurl                     NATS request: resolves a URL into embeds
//	svc.messages (BuildResponse)   NATS request: builds the API message object
//	rpc.gateway.guild.dispatch     NATS request: pushes MESSAGE_UPDATE
//
// plus a Redis lock (SET NX EX) so two jobs for one message cannot interleave
// their read-modify-write of the embeds array.
//
// Deliberately not ported yet, and why:
//   - content moderation (banned URL lists). Upstream DELETES the message when a
//     banned URL is detected; without the admin tables this cannot be reproduced,
//     and a wrong guess here deletes user data. The job proceeds unpublished.
//   - crosspost sync enqueue (jobs.syncCrosspostedMessage). Only matters for
//     crossposted messages, which this instance does not produce.
//   - the legacy bucket-0 read repair.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/idna"
)

const (
	snowflakeBucketSizeMs = int64(864000000) // 10 days
	msgEmbedMaxCount      = 10
	msgUnfurlMaxURLs      = 5
	msgUnfurlConcurrency  = 2
	msgUnfurlTimeout      = 12 * time.Second
	msgUnfurlBatchTimeout = 20 * time.Second
	msgLockTTL            = 5 * time.Second
	msgLockAttempts       = 8

	kvSeparator = "\x1f"

	// MessageFlags values we care about.
	flagCrossposted    = 1 << 0
	flagSuppressEmbeds = 1 << 2
)

var (
	urlRe         = regexp.MustCompile(`https?://[^\s]+`)
	inlineCodeRe  = regexp.MustCompile("`[^`]*`")
	fencedCodeRe  = regexp.MustCompile("(?s)```.*?```")
	spoilerRe     = regexp.MustCompile(`\|\|(.+?)\|\|`)
	markdownLink  = regexp.MustCompile(`\[[^\]]*\]\((https?://[^\s)]+)\)`)
	urlTrimSuffix = ".,;:!?)\"'»”"
)

// ---------- KV helpers ----------

func kvBigint(id string) string {
	return `{"__fluxer_type":"bigint","value":"` + id + `"}`
}

func kvDate(t time.Time) map[string]any {
	return map[string]any{"__fluxer_type": "date", "value": t.UTC().Format("2006-01-02T15:04:05.000Z")}
}

func messageBucket(messageID int64) int64 {
	return (messageID >> snowflakeTimeShift) / snowflakeBucketSizeMs
}

// messageKeys returns the trio the messages table is addressed by.
func messageKeys(channelID, messageID string) (partition, rowKey string, bucket int64) {
	id, _ := parseInt64(messageID)
	bucket = messageBucket(id)
	partition = kvBigint(channelID) + kvSeparator + fmt.Sprintf("%d", bucket)
	rowKey = partition + kvSeparator + kvBigint(messageID)
	return
}

func channelRowKey(channelID string) string {
	return kvBigint(channelID) + kvSeparator + "false"
}

// readKVRow loads one row and decodes its __fluxer_type wrappers.
func readKVRow(ctx context.Context, table, rowKey string) (map[string]any, error) {
	var raw []byte
	err := db.QueryRowContext(ctx,
		`SELECT row_data FROM `+kvTableName+` WHERE table_name = $1 AND row_key = $2
		   AND (expires_at IS NULL OR expires_at > now())`, table, rowKey).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func readMessage(ctx context.Context, channelID, messageID string) (map[string]any, error) {
	_, key, _ := messageKeys(channelID, messageID)
	return readKVRow(ctx, "messages", key)
}

func readChannel(ctx context.Context, channelID string) (map[string]any, error) {
	return readKVRow(ctx, "channels", channelRowKey(channelID))
}

// writeMessageEmbeds is the patch upstream performs: an upsert of the row_data
// with only "embeds" set. sort_id carries the message id for this table, and
// partition_key has to be present or the row becomes unreadable.
func writeMessageEmbeds(ctx context.Context, channelID, messageID string, embeds []map[string]any) error {
	partition, rowKey, _ := messageKeys(channelID, messageID)
	var payload any
	if len(embeds) > 0 {
		payload = embeds
	}
	body, err := json.Marshal(map[string]any{"embeds": payload})
	if err != nil {
		return err
	}
	const q = `
INSERT INTO fluxer_kv AS kv (table_name, partition_key, row_key, row_data, expires_at, sort_id, updated_at)
VALUES ('messages', $1, $2, $3::jsonb, NULL, $4::bigint, now())
ON CONFLICT (table_name, row_key) DO UPDATE SET
	partition_key = EXCLUDED.partition_key,
	row_data      = kv.row_data || EXCLUDED.row_data,
	sort_id       = EXCLUDED.sort_id,
	updated_at    = now()`
	_, err = db.ExecContext(ctx, q, partition, rowKey, string(body), messageID)
	return err
}

// ---------- value helpers ----------

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscan(s, &n)
	return n, err
}

func bigintString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return fmt.Sprintf("%.0f", t)
	case map[string]any:
		if s, ok := t["value"].(string); ok {
			return s
		}
	}
	return ""
}

func dateString(v any) string {
	if m, ok := v.(map[string]any); ok {
		if s, ok := m["value"].(string); ok {
			return s
		}
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func intField(m map[string]any, key string) int64 {
	switch t := m[key].(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case json.Number:
		n, _ := t.Int64()
		return n
	}
	return 0
}

// ---------- URL extraction ----------

func extractURLs(content string) []string {
	text := fencedCodeRe.ReplaceAllString(content, " ")
	text = inlineCodeRe.ReplaceAllString(text, " ")
	text = spoilerRe.ReplaceAllString(text, "$1")
	text = markdownLink.ReplaceAllString(text, "$1")

	seen := map[string]bool{}
	out := make([]string, 0, msgUnfurlMaxURLs)
	for _, raw := range urlRe.FindAllString(text, -1) {
		cleaned := strings.TrimRight(raw, urlTrimSuffix)
		canonical, ok := canonicalURL(cleaned)
		if !ok || seen[canonical] {
			continue
		}
		seen[canonical] = true
		out = append(out, canonical)
		if len(out) >= msgUnfurlMaxURLs {
			break
		}
	}
	return out
}

// canonicalURL does what the JS side does for dedupe: parse, keep http/https
// only, lowercase the host, IDNA-encode it, drop credentials, and rebuild.
func canonicalURL(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", false
	}
	if u.Host == "" {
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	if ascii, err := idna.Lookup.ToASCII(host); err == nil {
		host = ascii
	}
	if strings.Contains(host, "/invite/") {
		return "", false
	}
	if own := env("FLUXER_BASE_DOMAIN", ""); own != "" && (host == own || strings.HasSuffix(host, "."+own)) {
		// Links into this instance are not unfurled.
		return "", false
	}
	port := u.Port()
	if (u.Scheme == "https" && port == "443") || (u.Scheme == "http" && port == "80") {
		port = ""
	}
	u2 := &url.URL{Scheme: u.Scheme, Host: host, Path: u.Path, RawQuery: u.RawQuery, Fragment: u.Fragment}
	if port != "" {
		u2.Host = host + ":" + port
	}
	if u2.Path == "" {
		u2.Path = "/"
	}
	return u2.String(), true
}

// ---------- unfurl ----------

type unfurlRequest struct {
	Op            string  `json:"op"`
	URL           string  `json:"url"`
	NsfwMode      string  `json:"nsfw_mode"`
	BypassCache   bool    `json:"bypass_cache"`
	CacheOnly     bool    `json:"cache_only"`
	YoutubeAPIKey *string `json:"youtube_api_key"`
	KlipyAPIKey   *string `json:"klipy_api_key"`
}

type unfurlResponse struct {
	Resolved *struct {
		Embeds         []map[string]any `json:"embeds"`
		CacheTTLSecond *int             `json:"cache_ttl_seconds"`
	} `json:"Resolved"`
	Failed *struct {
		Message string `json:"message"`
	} `json:"Failed"`
}

// unfurlOne asks the Rust unfurl service for one URL. A `Failed` reply is not a
// job failure: the URL simply has no preview, exactly as upstream treats it.
func unfurlOne(nc *nats.Conn, rawURL, nsfwMode string) ([]map[string]any, error) {
	req := unfurlRequest{Op: "Unfurl", URL: rawURL, NsfwMode: nsfwMode}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	msg, err := nc.Request("svc.unfurl", body, msgUnfurlTimeout)
	if err != nil {
		return nil, fmt.Errorf("svc.unfurl: %w", err)
	}
	var resp unfurlResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("svc.unfurl decode: %w", err)
	}
	if resp.Failed != nil {
		return nil, nil
	}
	if resp.Resolved == nil {
		return nil, fmt.Errorf("svc.unfurl: unexpected reply variant")
	}
	return resp.Resolved.Embeds, nil
}

// unfurlAll runs up to two URLs at a time under one 20 s batch deadline, the
// same shape as upstream (a stalled URL fails the whole batch, and the job is
// retried by the queue).
func unfurlAll(ctx context.Context, nc *nats.Conn, urls []string, nsfwMode string) (map[string][]map[string]any, map[string][]string, error) {
	batchCtx, cancel := context.WithTimeout(ctx, msgUnfurlBatchTimeout)
	defer cancel()

	type result struct {
		url    string
		embeds []map[string]any
		err    error
	}
	jobs := make(chan string)
	results := make(chan result, len(urls))
	var wg sync.WaitGroup
	for i := 0; i < msgUnfurlConcurrency && i < len(urls); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				embeds, err := unfurlOne(nc, u, nsfwMode)
				results <- result{url: u, embeds: embeds, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, u := range urls {
			select {
			case jobs <- u:
			case <-batchCtx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()

	byURL := map[string][]map[string]any{}
	failed := map[string][]string{}
	for r := range results {
		if r.err != nil {
			failed[r.url] = []string{r.err.Error()}
			continue
		}
		if len(r.embeds) == 0 {
			continue
		}
		safe := make([]map[string]any, 0, len(r.embeds))
		for _, e := range r.embeds {
			if nsfwMode != "allow" && embedIsNSFW(e) {
				continue
			}
			safe = append(safe, toMessageEmbed(e))
		}
		if len(safe) > 0 {
			byURL[r.url] = safe
		}
	}
	if batchCtx.Err() != nil {
		return byURL, failed, fmt.Errorf("unfurl batch exceeded %s", msgUnfurlBatchTimeout)
	}
	return byURL, failed, nil
}

func embedIsNSFW(e map[string]any) bool {
	if v, ok := e["nsfw"].(bool); ok && v {
		return true
	}
	if children, ok := e["children"].([]any); ok {
		for _, c := range children {
			if m, ok := c.(map[string]any); ok {
				if v, ok := m["nsfw"].(bool); ok && v {
					return true
				}
			}
		}
	}
	return false
}

// toMessageEmbed converts the service's embed into the stored shape: the
// timestamp becomes a typed date wrapper, every other field is passed through.
func toMessageEmbed(e map[string]any) map[string]any {
	out := make(map[string]any, len(e))
	for k, v := range e {
		out[k] = v
	}
	if ts, ok := e["timestamp"].(string); ok && ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			out["timestamp"] = kvDate(t)
		}
	}
	if _, ok := out["type"]; !ok {
		out["type"] = "link"
	}
	return out
}

// ---------- lock ----------

var valkeyClient *redis.Client

func valkey() *redis.Client {
	if valkeyClient == nil {
		valkeyClient = redis.NewClient(&redis.Options{
			Addr:     env("VALKEY_ADDR", "valkey:6379"),
			Password: env("VALKEY_PASSWORD", ""),
			DB:       0,
		})
	}
	return valkeyClient
}

var releaseLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`)

func acquireMessageLock(ctx context.Context, channelID, messageID string) (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	token := hex.EncodeToString(buf)
	key := "lock:message:" + channelID + ":" + messageID + ":write"
	var lastErr error
	for attempt := 0; attempt < msgLockAttempts; attempt++ {
		ok, err := valkey().SetNX(ctx, key, token, msgLockTTL).Result()
		if err != nil {
			lastErr = err
		} else if ok {
			return token, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Duration(75*(attempt+1)) * time.Millisecond):
		}
	}
	if lastErr != nil {
		return "", fmt.Errorf("message lock: %w", lastErr)
	}
	return "", fmt.Errorf("timed out acquiring message write lock for %s:%s", channelID, messageID)
}

// ---------- gateway ----------

// buildMessageResponse asks svc.messages for the API-shaped message object that
// serves as the MESSAGE_UPDATE payload.
func buildMessageResponse(nc *nats.Conn, row map[string]any, viewerID string, guildID string) (map[string]any, error) {
	serialized := serializeForService(row)
	req := map[string]any{
		"op":                           "BuildResponse",
		"message":                      serialized,
		"viewer_user_id":               viewerID,
		"message_history_cutoff_ms":    nil,
		"can_read_message_history":     true,
		"include_hidden":               false,
		"media_endpoint":               env("FLUXER_MEDIA_ENDPOINT", ""),
		"media_proxy_secret_key":       env("FLUXER_MEDIA_PROXY_SECRET_KEY", ""),
		"attachment_url_secret_base64": env("FLUXER_ATTACHMENT_URL_SECRET", ""),
		"attachment_decay":             false,
		"instance_name":                env("FLUXER_INSTANCE_NAME", ""),
		"include_reactions":            false,
	}
	if guildID != "" {
		req["source_guild_id"] = guildID
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	msg, err := nc.Request("svc.messages", body, 6*time.Second)
	if err != nil {
		return nil, fmt.Errorf("svc.messages: %w", err)
	}
	var resp struct {
		FoundApi map[string]any `json:"FoundApi"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("svc.messages decode: %w", err)
	}
	if resp.FoundApi == nil {
		return nil, fmt.Errorf("svc.messages: no FoundApi in reply")
	}
	return resp.FoundApi, nil
}

// serializeForService flattens typed KV values the way upstream does before the
// RPC: bigints become strings, dates become epoch milliseconds.
func serializeForService(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = serializeValue(v)
	}
	return out
}

func serializeValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		if typ, ok := t["__fluxer_type"].(string); ok {
			switch typ {
			case "bigint":
				if s, ok := t["value"].(string); ok {
					return s
				}
			case "date":
				if s, ok := t["value"].(string); ok {
					if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
						return ts.UnixMilli()
					}
				}
			}
		}
		inner := make(map[string]any, len(t))
		for k, vv := range t {
			inner[k] = serializeValue(vv)
		}
		return inner
	case []any:
		arr := make([]any, len(t))
		for i, vv := range t {
			arr[i] = serializeValue(vv)
		}
		return arr
	}
	return v
}

func dispatchMessageUpdate(nc *nats.Conn, channel map[string]any, guildID, channelID string, data map[string]any) error {
	if guildID == "" {
		// Presence dispatch needs the DM recipients; this instance is guild-only,
		// so an unexpected DM is logged rather than guessed at.
		log.Printf("extractEmbeds: channel %s has no guild, MESSAGE_UPDATE not dispatched", channelID)
		return nil
	}
	body, err := json.Marshal(map[string]any{
		"guild_id": guildID,
		"event":    "MESSAGE_UPDATE",
		"data":     data,
	})
	if err != nil {
		return err
	}
	msg, err := nc.Request("rpc.gateway.guild.dispatch", body, 5*time.Second)
	if err != nil {
		return fmt.Errorf("gateway dispatch: %w", err)
	}
	var resp struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil || !resp.OK {
		return fmt.Errorf("gateway dispatch rejected: %s", string(msg.Data))
	}
	return nil
}

// ---------- embeds merging ----------

func prevEmbeds(row map[string]any) []map[string]any {
	raw, ok := row["embeds"].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(raw))
	for _, e := range raw {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// mergeEmbeds keeps upstream-authored "rich" embeds and appends the unfurled
// ones, capped at 10.
func mergeEmbeds(existing []map[string]any, unfurled []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(existing)+len(unfurled))
	for _, e := range existing {
		if typ, _ := e["type"].(string); typ == "rich" {
			out = append(out, e)
		}
	}
	for _, e := range unfurled {
		if len(out) >= msgEmbedMaxCount {
			break
		}
		out = append(out, e)
	}
	return out
}

func embedsEquivalent(a, b []map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	ja, err1 := json.Marshal(a)
	jb, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return string(ja) == string(jb)
}

// ---------- handler ----------

func taskExtractEmbeds(ctx context.Context, payload map[string]any) error {
	nc := workerNATS()
	if nc == nil {
		return fmt.Errorf("extractEmbeds: no NATS connection")
	}
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("extractEmbeds: %w", err)
		}
	}
	channelID, _ := payload["channelId"].(string)
	messageID, _ := payload["messageId"].(string)
	if channelID == "" || messageID == "" {
		return fmt.Errorf("extractEmbeds: channelId/messageId missing")
	}
	nsfwMode, _ := payload["nsfwMode"].(string)
	if nsfwMode == "" {
		nsfwMode = "block"
	}
	expected, _ := payload["expectedContentHash"].(string)
	payloadGuild, _ := payload["guildId"].(string)

	msg, err := messageWithRetries(ctx, channelID, messageID, expected)
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}
	content, _ := msg["content"].(string)
	if content == "" {
		return nil
	}
	channel, err := readChannel(ctx, channelID)
	if err != nil {
		return fmt.Errorf("extractEmbeds: read channel: %w", err)
	}
	if channel == nil {
		return nil
	}
	guildID := payloadGuild
	if guildID == "" || guildID == "null" {
		guildID = bigintString(channel["guild_id"])
	}
	urls := extractURLs(content)
	if len(urls) == 0 {
		return nil
	}

	unfurled, failed, err := unfurlAll(ctx, nc, urls, nsfwMode)
	if err != nil {
		return fmt.Errorf("extractEmbeds: %w", err)
	}
	for u, reasons := range failed {
		log.Printf("extractEmbeds: %s failed: %v", u, reasons)
	}
	if len(unfurled) == 0 {
		return nil
	}
	ordered := make([]map[string]any, 0, msgEmbedMaxCount)
	for _, u := range urls {
		if embeds, ok := unfurled[u]; ok {
			ordered = append(ordered, embeds...)
		}
	}

	token, err := acquireMessageLock(ctx, channelID, messageID)
	if err != nil {
		return err
	}
	defer func() {
		if _, err := releaseLockScript.Run(context.Background(), valkey(), []string{
			"lock:message:" + channelID + ":" + messageID + ":write"}, token).Result(); err != nil {
			log.Printf("extractEmbeds: lock release: %v", err)
		}
	}()

	fresh, err := readMessage(ctx, channelID, messageID)
	if err != nil {
		return fmt.Errorf("extractEmbeds: re-read: %w", err)
	}
	if fresh == nil {
		return nil
	}
	freshContent, _ := fresh["content"].(string)
	if expected != "" && hashContent(freshContent) != expected {
		log.Printf("extractEmbeds: content changed after unfurl for %s, discarding", messageID)
		return nil
	}

	existing := prevEmbeds(fresh)
	next := mergeEmbeds(existing, ordered)
	if !embedsEquivalent(existing, next) {
		if err := writeMessageEmbeds(ctx, channelID, messageID, next); err != nil {
			return fmt.Errorf("extractEmbeds: write embeds: %w", err)
		}
		fresh["embeds"] = toAnySlice(next)
	}

	if intField(fresh, "flags")&flagSuppressEmbeds != 0 {
		log.Printf("extractEmbeds: SUPPRESS_EMBEDS on %s, MESSAGE_UPDATE skipped", messageID)
		return nil
	}
	viewer := bigintString(fresh["author_id"])
	data, err := buildMessageResponse(nc, fresh, viewer, guildID)
	if err != nil {
		return fmt.Errorf("extractEmbeds: %w", err)
	}
	if err := dispatchMessageUpdate(nc, channel, guildID, channelID, data); err != nil {
		return fmt.Errorf("extractEmbeds: %w", err)
	}
	log.Printf("extractEmbeds: message %s got %d embed(s), MESSAGE_UPDATE dispatched", messageID, len(next))
	return nil
}

func toAnySlice(in []map[string]any) []any {
	out := make([]any, 0, len(in))
	for _, e := range in {
		out = append(out, e)
	}
	return out
}

func hashContent(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// messageWithRetries mirrors the upstream read loop: the producing API writes
// the message and enqueues the job right after, so a read can miss the row for
// a moment. Eight attempts, linear backoff, ~3.6 s in total.
func messageWithRetries(ctx context.Context, channelID, messageID, expected string) (map[string]any, error) {
	for attempt := 0; attempt < 8; attempt++ {
		msg, err := readMessage(ctx, channelID, messageID)
		if err != nil {
			return nil, fmt.Errorf("extractEmbeds: read message: %w", err)
		}
		if msg != nil {
			content, _ := msg["content"].(string)
			if expected == "" || hashContent(content) == expected {
				return msg, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(100*(attempt+1)) * time.Millisecond):
		}
	}
	log.Printf("extractEmbeds: message %s not visible with the expected content", messageID)
	return nil, nil
}
