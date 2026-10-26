package main

// sendSystemDm: deliver a direct message from the system account (user id 0).
//
// This is the primitive the other notification tasks sit on. notifyRegistrationApprovers
// uses it to tell admins that someone is waiting for approval, and the same job carries
// moderation and enforcement notices.
//
// Shape taken from the running api release rather than guessed:
//
//	channels            key  <bigint channel_id> \x1f false          (soft_deleted is part of the PK)
//	dm_states           key  <hi_user_id> \x1f <lo_user_id> \x1f <channel_id>
//	private_channels    key  <user_id> \x1f <channel_id>
//	messages            key  <channel_id> \x1f <bucket> \x1f <message_id>
//	messages_by_author  key  <author_id> \x1f <message_id>
//	channel_state       key  <channel_id>
//
// The message object that goes into MESSAGE_CREATE is not built here: the api asks the
// message service (`svc.messages`, op BuildResponse) to render it, and so do we. Building
// it by hand would risk a payload the client silently drops.

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
	systemUserID    = "0"
	fluxerEpochMS   = int64(1420070400000)
	snowflakeShift  = 22
	bucketSizeMS    = int64(864000000) // 10 days
	kvSep           = "\x1f"
	dmChannelType   = 1
	messageTypeUser = 0
)

func bigintSeg(id string) string {
	return `{"__fluxer_type":"bigint","value":"` + id + `"}`
}

func bigintValue(id string) map[string]any {
	return map[string]any{"__fluxer_type": "bigint", "value": id}
}

func dateValue(t time.Time) map[string]any {
	return map[string]any{"__fluxer_type": "date", "value": t.UTC().Format("2006-01-02T15:04:05.000Z")}
}

// bucketFor mirrors @fluxer/snowflake SnowflakeBuckets.makeBucket: the snowflake carries
// milliseconds since the Fluxer epoch above the 22-bit worker/sequence field, and a bucket
// is ten of those days.
func bucketFor(id string) (int64, error) {
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0, err
	}
	relative := (n >> snowflakeShift) // ms since epoch, part of the id itself
	return relative / bucketSizeMS, nil
}

// snowflakeBatch asks the snowflake service for fresh ids. Generating them locally would
// mean picking a worker id and eventually colliding with the api's own generator.
func snowflakeBatch(nc *nats.Conn, count int) ([]string, error) {
	body, err := json.Marshal(map[string]any{"op": "GenerateBatch", "count": count})
	if err != nil {
		return nil, err
	}
	msg, err := nc.Request("svc.snowflakes", body, 6*time.Second)
	if err != nil {
		return nil, fmt.Errorf("svc.snowflakes: %w", err)
	}
	var resp struct {
		IDs   []string `json:"ids"`
		Error string   `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("svc.snowflakes decode: %w", err)
	}
	if resp.Error != "" {
		return nil, errors.New("snowflake service: " + resp.Error)
	}
	if len(resp.IDs) != count {
		return nil, fmt.Errorf("snowflake service returned %d ids, want %d", len(resp.IDs), count)
	}
	return resp.IDs, nil
}

// putKV writes one fluxer_kv row, merging into an existing row_data like the api does.
func putKV(ctx context.Context, table, partition, rowKey string, data map[string]any, sortID string) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	var sort any
	if sortID != "" {
		sort = sortID
	}
	const q = `
INSERT INTO fluxer_kv AS kv (table_name, partition_key, row_key, row_data, expires_at, sort_id, updated_at)
VALUES ($1, $2, $3, $4::jsonb, NULL, $5::bigint, now())
ON CONFLICT (table_name, row_key) DO UPDATE SET
	partition_key = EXCLUDED.partition_key,
	row_data      = kv.row_data || EXCLUDED.row_data,
	sort_id       = EXCLUDED.sort_id,
	updated_at    = now()`
	_, err = db.ExecContext(ctx, q, table, partition, rowKey, string(body), sort)
	return err
}

func readKV(ctx context.Context, table, rowKey string) (map[string]any, error) {
	var raw string
	err := db.QueryRowContext(ctx,
		`SELECT row_data::text FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, table, rowKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// dmChannelFor returns the existing DM channel between two users, if any. dm_states is
// keyed by the ordered pair plus the channel id, so the lookup is a prefix match.
func dmChannelFor(ctx context.Context, a, b string) (map[string]any, error) {
	hi, lo := a, b
	if hi < lo {
		hi, lo = lo, hi
	}
	prefix := bigintSeg(hi) + kvSep + bigintSeg(lo) + kvSep
	var raw string
	err := db.QueryRowContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'dm_states' AND partition_key LIKE $1 || '%'
LIMIT 1`, prefix).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state map[string]any
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, err
	}
	channelID := typedString(state["channel_id"])
	rowKey := bigintSeg(channelID) + kvSep + "false"
	return readKV(ctx, "channels", rowKey)
}

func typedString(v any) string {
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

// ensureDMChannel creates the three rows a DM needs, or returns the existing channel.
func ensureDMChannel(ctx context.Context, nc *nats.Conn, a, b string) (map[string]any, error) {
	if ch, err := dmChannelFor(ctx, a, b); err != nil || ch != nil {
		return ch, err
	}
	ids, err := snowflakeBatch(nc, 1)
	if err != nil {
		return nil, err
	}
	channelID := ids[0]
	now := time.Now().UTC()
	row := map[string]any{
		"channel_id":            bigintValue(channelID),
		"type":                  dmChannelType,
		"version":               1,
		"soft_deleted":          false,
		"guild_id":              nil,
		"owner_id":              nil,
		"name":                  nil,
		"topic":                 nil,
		"url":                   nil,
		"nsfw":                  nil,
		"nicks":                 nil,
		"icon_hash":             nil,
		"parent_id":             nil,
		"position":              nil,
		"bitrate":               nil,
		"user_limit":            nil,
		"rtc_region":            nil,
		"rtc_p2p":               nil,
		"rate_limit_per_user":   nil,
		"permission_overwrites": nil,
		"voice_connection_limit": nil,
		"content_warning_text":  nil,
		"content_warning_level": nil,
		"indexed_at":            nil,
		"last_message_id":       nil,
		"last_pin_timestamp":    nil,
		"recipient_ids": map[string]any{
			"__fluxer_type": "set",
			"value":         []any{bigintValue(a), bigintValue(b)},
		},
	}
	key := bigintSeg(channelID) + kvSep + "false"
	if err := putKV(ctx, "channels", key, key, row, channelID); err != nil {
		return nil, err
	}

	hi, lo := a, b
	if hi < lo {
		hi, lo = lo, hi
	}
	stateKey := bigintSeg(hi) + kvSep + bigintSeg(lo) + kvSep + bigintSeg(channelID)
	state := map[string]any{
		"channel_id":  bigintValue(channelID),
		"hi_user_id":  bigintValue(hi),
		"lo_user_id":  bigintValue(lo),
	}
	if err := putKV(ctx, "dm_states", stateKey, stateKey, state, channelID); err != nil {
		return nil, err
	}

	// private_channels carries a denormalised copy of the channel; the api patches it for
	// each participant so the client can render the conversation without fetching the channel.
	for _, uid := range []string{a, b} {
		pk := bigintSeg(uid) + kvSep + bigintSeg(channelID)
		snapshot := map[string]any{
			"user_id":                       bigintValue(uid),
			"channel_id":                    bigintValue(channelID),
			"is_gdm":                        false,
			"snapshot_at":                   dateValue(now),
			"channel_name":                  nil,
			"channel_nsfw":                  nil,
			"channel_type":                  dmChannelType,
			"channel_nicks":                 nil,
			"channel_version":               1,
			"channel_owner_id":              nil,
			"channel_icon_hash":             nil,
			"channel_recipient_ids":         map[string]any{"__fluxer_type": "set", "value": []any{bigintValue(a), bigintValue(b)}},
			"channel_last_message_id":       nil,
			"channel_last_pin_timestamp":    nil,
			"channel_rate_limit_per_user":   0,
		}
		if err := putKV(ctx, "private_channels", pk, pk, snapshot, channelID); err != nil {
			return nil, err
		}
	}
	stored, err := readKV(ctx, "channels", key)
	if err != nil {
		return nil, err
	}
	if stored != nil {
		stored["_fresh"] = true
	}
	return stored, nil
}

// insertMessage writes the message row, its author index entry and the channel state row.
func insertMessage(ctx context.Context, channelID, authorID, content string) (map[string]any, error) {
	nc := workerNATS()
	ids, err := snowflakeBatch(nc, 1)
	if err != nil {
		return nil, err
	}
	messageID := ids[0]
	bucket, err := bucketFor(messageID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	row := map[string]any{
		"message_id":       bigintValue(messageID),
		"channel_id":       bigintValue(channelID),
		"author_id":        bigintValue(authorID),
		"content":          content,
		"type":             messageTypeUser,
		"flags":            0,
		"version":          1,
		"bucket":           bucket,
		"has_reaction":     false,
		"mention_everyone": false,
	}
	// partition_key is channel + bucket only; the message id belongs to row_key alone.
	// Getting this wrong keeps the message readable by id but invisible in history.
	messagePartition := bigintSeg(channelID) + kvSep + strconv.FormatInt(bucket, 10)
	messageKey := messagePartition + kvSep + bigintSeg(messageID)
	if err := putKV(ctx, "messages", messagePartition, messageKey, row, messageID); err != nil {
		return nil, err
	}
	// channel_message_buckets is how the api enumerates history: without the
	// (channel, bucket) row the message exists in messages but is never listed.
	bucketKey := bigintSeg(channelID) + kvSep + strconv.FormatInt(bucket, 10)
	bucketRow := map[string]any{
		"channel_id": bigintValue(channelID),
		"bucket":     bucket,
		"updated_at": dateValue(now),
	}
	if err := putKV(ctx, "channel_message_buckets", bucketKey, bucketKey, bucketRow, strconv.FormatInt(bucket, 10)); err != nil {
		return nil, err
	}

	authorKey := bigintSeg(authorID) + kvSep + bigintSeg(messageID)
	authorRow := map[string]any{
		"author_id":  bigintValue(authorID),
		"channel_id": bigintValue(channelID),
		"message_id": bigintValue(messageID),
	}
	if err := putKV(ctx, "messages_by_author_id_v2", authorKey, authorKey, authorRow, messageID); err != nil {
		return nil, err
	}

	stateKey := bigintSeg(channelID)
	existing, err := readKV(ctx, "channel_state", stateKey)
	if err != nil {
		return nil, err
	}
	state := map[string]any{
		"channel_id":          bigintValue(channelID),
		"updated_at":          dateValue(now),
		"has_messages":        true,
		"last_message_id":     bigintValue(messageID),
		"last_message_bucket": bucket,
	}
	if existing == nil {
		state["created_bucket"] = bucket
	}
	if err := putKV(ctx, "channel_state", stateKey, stateKey, state, messageID); err != nil {
		return nil, err
	}
	return row, nil
}

// dispatchToUser pushes one event to every session of a user. presence.dispatch is the
// same RPC the api uses for DM fanout.
func dispatchToUser(nc *nats.Conn, userID, event string, data map[string]any) error {
	body, err := json.Marshal(map[string]any{
		"user_id": userID,
		"event":   event,
		"data":    data,
	})
	if err != nil {
		return err
	}
	msg, err := nc.Request("rpc.gateway.presence.dispatch", body, 5*time.Second)
	if err != nil {
		return fmt.Errorf("presence.dispatch: %w", err)
	}
	var resp struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return fmt.Errorf("presence.dispatch decode: %w", err)
	}
	if !resp.OK {
		return fmt.Errorf("presence.dispatch refused: %s", resp.Error)
	}
	return nil
}

func sendOneSystemDm(ctx context.Context, recipientID, content string) error {
	nc := workerNATS()
	channel, err := ensureDMChannel(ctx, nc, systemUserID, recipientID)
	if err != nil {
		return err
	}
	if channel == nil {
		return errors.New("dm channel could not be created")
	}
	channelID := typedString(channel["channel_id"])
	if fresh, _ := channel["_fresh"].(bool); fresh {
		if err := dispatchChannelCreate(ctx, nc, channel, []string{systemUserID, recipientID}, recipientID); err != nil {
			log.Printf("sendSystemDm: CHANNEL_CREATE for %s failed: %v", channelID, err)
		}
	}
	row, err := insertMessage(ctx, channelID, systemUserID, content)
	if err != nil {
		return err
	}
	payload, err := buildMessageResponse(nc, row, recipientID, "")
	if err != nil {
		return fmt.Errorf("build message payload: %w", err)
	}
	payload["channel_type"] = dmChannelType
	return dispatchToUser(nc, recipientID, "MESSAGE_CREATE", payload)
}

func taskSendSystemDm(ctx context.Context, payload map[string]any) error {
	if err := openDB(); err != nil {
		return fmt.Errorf("sendSystemDm: %w", err)
	}
	content, _ := payload["content"].(string)
	if content == "" {
		return errors.New("sendSystemDm: empty content")
	}
	var recipients []string
	if raw, ok := payload["user_ids"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok && s != "" {
				recipients = append(recipients, s)
			}
		}
	}
	if all, _ := payload["all_users"].(bool); all {
		rows, err := db.QueryContext(ctx, `
SELECT row_data->'user_id'->>'value' FROM fluxer_kv
WHERE table_name = 'users' AND row_data->>'is_bot' <> 'true'`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			recipients = append(recipients, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	if len(recipients) == 0 {
		log.Printf("sendSystemDm: no recipients in payload")
		return nil
	}
	sent, failed := 0, 0
	for _, id := range recipients {
		if id == systemUserID {
			continue
		}
		if err := sendOneSystemDm(ctx, id, content); err != nil {
			failed++
			log.Printf("sendSystemDm: %s failed: %v", id, err)
			continue
		}
		sent++
	}
	log.Printf("sendSystemDm: sent=%d failed=%d", sent, failed)
	if failed > 0 && sent == 0 {
		return fmt.Errorf("sendSystemDm: all %d recipients failed", failed)
	}
	return nil
}

// userPartial builds the small user object the client expects inside a channel.
func userPartial(ctx context.Context, userID string) map[string]any {
	out := map[string]any{
		"id":            userID,
		"username":      "",
		"discriminator": "0000",
		"global_name":   nil,
		"avatar":        nil,
		"avatar_color":  nil,
		"flags":         0,
	}
	var name sql.NullString
	var global sql.NullString
	var bot sql.NullString
	err := db.QueryRowContext(ctx, `
SELECT row_data->>'username',
       row_data->>'global_name',
       row_data->>'bot'
FROM fluxer_kv WHERE table_name = 'users' AND row_data->'user_id'->>'value' = $1
LIMIT 1`, userID).Scan(&name, &global, &bot)
	if err != nil {
		return out
	}
	if name.Valid {
		out["username"] = name.String
		if !global.Valid || global.String == "" {
			out["global_name"] = name.String
		}
	}
	if global.Valid && global.String != "" {
		out["global_name"] = global.String
	}
	if bot.Valid && bot.String == "true" {
		out["bot"] = true
	}
	return out
}

// dispatchChannelCreate tells a user about a channel they were just added to. Without it
// the client only learns about the conversation from the first MESSAGE_CREATE, which is
// why a brand new system DM used to appear only after a page reload.
func dispatchChannelCreate(ctx context.Context, nc *nats.Conn, channelRow map[string]any, participants []string, viewerID string) error {
	channelID := typedString(channelRow["channel_id"])
	recipients := make([]any, 0, len(participants))
	for _, id := range participants {
		if id == viewerID {
			continue
		}
		recipients = append(recipients, userPartial(ctx, id))
	}
	data := map[string]any{
		"id":                     channelID,
		"type":                   dmChannelType,
		"name":                   nil,
		"topic":                  nil,
		"url":                    nil,
		"icon":                   nil,
		"owner_id":               nil,
		"parent_id":              nil,
		"bitrate":                nil,
		"user_limit":             nil,
		"voice_connection_limit": nil,
		"rtc_region":             nil,
		"last_message_id":        nil,
		"last_pin_timestamp":     nil,
		"nsfw":                   false,
		"recipients":             recipients,
	}
	return dispatchToUser(nc, viewerID, "CHANNEL_CREATE", data)
}
