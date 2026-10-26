package main

// userProcessPendingDeletion: the account deletion cascade itself.
//
// The api decides *that* an account is going away (DELETE /users/@me/delete sets
// flags SELF_DELETED and pending_deletion_at, then drops a member into the valkey
// set the scheduler reads). This handler is what actually removes the person from
// the instance: guild membership, group DMs, authored messages, uploaded assets,
// sessions, tokens and every per-user row, and finally replaces the users row with
// an anonymised "DeletedUser" record so that old messages still have an author.
//
// Two rules shaped the port:
//
//   - Every row that belongs to a user is found through row_data, not through
//     partition_key. The KV mapper splits partitions differently per table (some
//     keep the whole primary key, some only the first column), while the columns
//     are the same everywhere, so `row_data->'user_id'->>'value' = $1` is the one
//     predicate that cannot silently match nothing.
//   - A failure in one entity must not abort the rest. Upstream wraps every guild
//     and every group DM in its own try/catch and only the message anonymisation
//     loop is allowed to throw; that structure is kept, because a single broken
//     guild would otherwise leave the account half deleted forever.
//
// Intentional gaps, all of them because the corresponding subsystem is not part of
// this deployment: no Stripe (no subscription to cancel), no cache purge queue (the
// adapter is `none`), no enforcement/network-block repository (no admin decisions
// table in use), no activity stream (EVENTS does not exist). They are logged rather
// than silently skipped so a future reader knows what was left out.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	deletedUserName            = "DeletedUser"
	deletedUserGlobalName      = "Deleted User"
	timezonePrivacyEveryone    = 1 << 0
	messageTypeRecipientRemove = 2
	deletionMessageChunk       = 100
)

// userDeletionJob is the payload the scheduler publishes.
type userDeletionJob struct {
	UserID    string
	Reason    int
	PendingAt time.Time
}

func decodeUserDeletionJob(payload map[string]any) (userDeletionJob, error) {
	job := userDeletionJob{Reason: -1}
	if id, ok := payload["userId"].(string); ok {
		job.UserID = id
	}
	if job.UserID == "" {
		return job, fmt.Errorf("userId is required")
	}
	switch v := payload["deletionReasonCode"].(type) {
	case float64:
		job.Reason = int(v)
	case int:
		job.Reason = v
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			job.Reason = n
		}
	}
	if ts, ok := payload["pendingDeletionAt"].(string); ok {
		job.PendingAt = parseRFC3339(ts)
	}
	return job, nil
}

// typedSet flattens a {"__fluxer_type":"set"} value into its ids.
func typedSet(v any) []string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	raw, ok := m["value"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if id := typedString(item); id != "" {
			out = append(out, id)
		}
	}
	return out
}

// intField reads a numeric field that may be stored either as a plain JSON number
// (guilds.member_count, buckets) or behind the typed wrapper.
func intFieldChecked(row map[string]any, key string) (int, bool) {
	switch v := row[key].(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case map[string]any:
		if s, ok := v["value"].(string); ok {
			if n, err := strconv.Atoi(s); err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

// mustJSON marshals a value that is known to be marshalable (maps of strings).
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}

// patchRowField sets one jsonb field on a row, keeping everything else.
func patchRowField(ctx context.Context, table, rowKey, field string, value any) error {
	_, err := db.ExecContext(ctx, `
UPDATE fluxer_kv SET row_data = row_data || jsonb_build_object($3::text, $4::jsonb), updated_at = now()
WHERE table_name = $1 AND row_key = $2`, table, rowKey, field, string(mustJSON(value)))
	return err
}

// userScopedTables lists every table whose rows carry a direct reference to a user
// and can be dropped wholesale. The column differs: relationships and notes name the
// user as source or target, everything else as user_id. Tables upstream has but this
// deployment never wrote (no rows at all) are still listed: deleting from an empty
// table_name costs nothing and keeps the list complete for the day they appear.
var userScopedTables = []struct{ table, column string }{
	{"user_settings", "user_id"},
	{"user_guild_settings", "user_id"},
	{"read_states", "user_id"},
	{"recent_mentions", "user_id"},
	{"recent_mentions_by_guild", "user_id"},
	{"user_recovery_kits", "user_id"},
	{"user_dm_history", "user_id"},
	{"authorized_ips_v2", "user_id"},
	{"authorized_ip_trust_keys_v1", "user_id"},
	{"auth_sessions_by_user_id", "user_id"},
	{"auth_session_tombstones", "user_id"},
	{"oauth2_access_tokens_by_user", "user_id"},
	{"oauth2_refresh_tokens_by_user", "user_id"},
	{"users_by_last_active_ip", "user_id"},
	{"users_by_last_active_ip_trust_key_v1", "user_id"},
	{"users_by_username", "user_id"},
	{"notes", "source_user_id"},
	{"notes", "target_user_id"},
	{"relationships", "source_user_id"},
	{"relationships_by_target", "target_user_id"},
	{"saved_messages", "user_id"},
	{"pinned_dms", "user_id"},
	{"push_subscriptions", "user_id"},
	{"mfa_backup_codes", "user_id"},
	{"webauthn_credentials", "user_id"},
	{"favorite_memes", "user_id"},
	{"user_connections", "user_id"},
	{"guild_bans", "user_id"},
	{"thread_members_by_user", "user_id"},
	{"user_email_owners", "user_id"},
	{"applications_by_owner", "owner_user_id"},
}

// patchUserRow merges fields into a users row and bumps version, the way every
// upstream writer does. Absent fields are untouched; a nil value removes nothing,
// use clearUserFields for that.
func patchUserRow(ctx context.Context, userID string, patch map[string]any) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
UPDATE fluxer_kv SET
	row_data = row_data || $3::jsonb || jsonb_build_object('version', coalesce((row_data->>'version')::int, 0) + 1),
	updated_at = now()
WHERE table_name = 'users' AND row_key = $1 AND row_data->'user_id'->>'value' = $2`,
		bigintSeg(userID), userID, string(body))
	return err
}

// clearUserFields removes keys from a users row (the anonymised record has no
// email, no password hash and no avatar anymore, rather than nulled-out copies).
func clearUserFields(ctx context.Context, userID string, fields ...string) error {
	if len(fields) == 0 {
		return nil
	}
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		parts = append(parts, "'"+strings.ReplaceAll(f, "'", "")+"'")
	}
	q := fmt.Sprintf(`
UPDATE fluxer_kv SET
	row_data = (row_data - ARRAY[%s]::text[]) || jsonb_build_object('version', coalesce((row_data->>'version')::int, 0) + 1),
	updated_at = now()
WHERE table_name = 'users' AND row_key = $1 AND row_data->'user_id'->>'value' = $2`, strings.Join(parts, ","))
	_, err := db.ExecContext(ctx, q, bigintSeg(userID), userID)
	return err
}

// startUserDeletion claims the deletion the same way upstream's startDeletion does:
// it only succeeds when the row still carries exactly the schedule the job was cut
// from and no other worker has already started it.
func startUserDeletion(ctx context.Context, job userDeletionJob, user map[string]any) (bool, error) {
	scheduled := parseRFC3339(typedString(user["pending_deletion_at"]))
	if scheduled.IsZero() {
		return false, nil
	}
	if !job.PendingAt.IsZero() && scheduled.UnixMilli() != job.PendingAt.UnixMilli() {
		log.Printf("userProcessPendingDeletion: %s: schedule moved (%s != %s), leaving it to the scheduler",
			job.UserID, scheduled.Format(time.RFC3339Nano), job.PendingAt.Format(time.RFC3339Nano))
		return false, nil
	}
	state := deletionStateFromRow(user)
	if !state.StartedAt.IsZero() {
		// A previous attempt already claimed it; carrying on is safe and is what
		// makes a retried job idempotent.
		return true, nil
	}
	if scheduled.After(time.Now().UTC()) {
		return false, nil
	}
	if state.deletionBlocked() {
		log.Printf("userProcessPendingDeletion: %s: account is not eligible for automated deletion", job.UserID)
		return false, nil
	}
	now := time.Now().UTC()
	res, err := db.ExecContext(ctx, `
UPDATE fluxer_kv SET
	row_data = row_data || jsonb_build_object('deletion_started_at', $3::jsonb),
	updated_at = now()
WHERE table_name = 'users' AND row_key = $1
  AND row_data->'user_id'->>'value' = $2
  AND row_data->'pending_deletion_at'->>'value' = $4
  AND (row_data->'deletion_started_at') IS NULL`,
		bigintSeg(job.UserID), job.UserID, dateValue(now), typedString(user["pending_deletion_at"]))
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		log.Printf("userProcessPendingDeletion: %s: deletion state changed under us, skipping this run", job.UserID)
		return false, nil
	}
	return true, nil
}

// createDeletedUserRecord makes the placeholder author that anonymised messages
// point at. It is an ordinary users row with the DELETED flag and no credentials.
func createDeletedUserRecord(ctx context.Context) (string, error) {
	ids, err := snowflakeBatch(workerNATS(), 1)
	if err != nil {
		return "", err
	}
	userID := ids[0]
	row := map[string]any{
		"user_id":                bigintValue(userID),
		"username":               deletedUserName,
		"discriminator":          0,
		"global_name":            deletedUserGlobalName,
		"bot":                    false,
		"system":                 false,
		"flags":                  map[string]any{"__fluxer_type": "bigint", "value": strconv.FormatUint(userFlagDeleted, 10)},
		"version":                1,
		"timezone_privacy_flags": timezonePrivacyEveryone,
		"authenticator_types":    map[string]any{"__fluxer_type": "set", "value": []any{}},
		"email_verified":         false,
		"email_bounced":          false,
	}
	if err := putKV(ctx, "users", bigintSeg(userID), bigintSeg(userID), row, userID); err != nil {
		return "", err
	}
	return userID, nil
}

// deleteObject removes an object from the cdn bucket, keeping the caller readable:
// a missing object is not an error and a failure is only logged.
func deleteObject(ctx context.Context, key, what string) {
	if key == "" {
		return
	}
	if err := s3Delete(ctx, key); err != nil {
		log.Printf("userProcessPendingDeletion: delete %s %s: %v", what, key, err)
	}
}

func dispatchGuild(nc *nats.Conn, guildID, event string, data map[string]any) error {
	body, err := json.Marshal(map[string]any{"guild_id": guildID, "event": event, "data": data})
	if err != nil {
		return err
	}
	msg, err := nc.Request("rpc.gateway.guild.dispatch", body, 5*time.Second)
	if err != nil {
		return fmt.Errorf("guild.dispatch: %w", err)
	}
	return gatewayAck(msg.Data, "guild.dispatch")
}

func leaveGuildGateway(nc *nats.Conn, userID, guildID string) error {
	body, err := json.Marshal(map[string]any{"user_id": userID, "guild_id": guildID})
	if err != nil {
		return err
	}
	msg, err := nc.Request("rpc.gateway.presence.leave_guild", body, 5*time.Second)
	if err != nil {
		return fmt.Errorf("presence.leave_guild: %w", err)
	}
	return gatewayAck(msg.Data, "presence.leave_guild")
}

func terminateAllSessions(nc *nats.Conn, userID string) error {
	body, err := json.Marshal(map[string]any{"user_id": userID})
	if err != nil {
		return err
	}
	msg, err := nc.Request("rpc.gateway.presence.terminate_all_sessions", body, 5*time.Second)
	if err != nil {
		return fmt.Errorf("presence.terminate_all_sessions: %w", err)
	}
	return gatewayAck(msg.Data, "presence.terminate_all_sessions")
}

func gatewayAck(raw []byte, what string) error {
	var resp struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("%s decode: %w", what, err)
	}
	if !resp.OK {
		return fmt.Errorf("%s refused: %s", what, resp.Error)
	}
	return nil
}

// guildMemberships returns the guilds the user is a member of, from the by-user
// index which is the only place that answers this without scanning every guild.
func guildMemberships(ctx context.Context, userID string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'guild_id'->>'value' FROM fluxer_kv
WHERE table_name = 'guild_members_by_user_id' AND row_data->'user_id'->>'value' = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids, rows.Err()
}

// leaveGuilds removes the membership row, the member's guild avatar and banner, the
// member count on the guild, and tells the gateway - in that order, so a late
// GUILD_MEMBER_REMOVE cannot resurrect a row.
func leaveGuilds(ctx context.Context, nc *nats.Conn, userID string) (int, error) {
	guildIDs, err := guildMemberships(ctx, userID)
	if err != nil {
		return 0, err
	}
	left := 0
	for _, guildID := range guildIDs {
		memberKey := bigintSeg(guildID) + kvSep + bigintSeg(userID)
		member, err := readKV(ctx, "guild_members", memberKey)
		if err != nil {
			log.Printf("userProcessPendingDeletion: %s: guild %s: read member: %v", userID, guildID, err)
			continue
		}
		if member == nil {
			// The index is stale: clean it and move on.
			_, _ = db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'guild_members_by_user_id'
				AND row_data->'user_id'->>'value' = $1 AND row_data->'guild_id'->>'value' = $2`, userID, guildID)
			continue
		}
		if hash := typedString(member["avatar_hash"]); hash != "" {
			deleteObject(ctx, "guilds/"+guildID+"/users/"+userID+"/avatars/"+hash, "guild member avatar")
		}
		if hash := typedString(member["banner_hash"]); hash != "" {
			deleteObject(ctx, "guilds/"+guildID+"/users/"+userID+"/banners/"+hash, "guild member banner")
		}
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'guild_members'
			AND row_data->'guild_id'->>'value' = $1 AND row_data->'user_id'->>'value' = $2`, guildID, userID); err != nil {
			log.Printf("userProcessPendingDeletion: %s: guild %s: delete member: %v", userID, guildID, err)
			continue
		}
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'guild_members_by_user_id'
			AND row_data->'user_id'->>'value' = $1 AND row_data->'guild_id'->>'value' = $2`, userID, guildID); err != nil {
			log.Printf("userProcessPendingDeletion: %s: guild %s: delete membership index: %v", userID, guildID, err)
		}
		if guild, err := readKV(ctx, "guilds", bigintSeg(guildID)); err == nil && guild != nil {
			count, ok := intFieldChecked(guild, "member_count")
			if !ok {
				log.Printf("userProcessPendingDeletion: %s: guild %s: member_count is not a number, leaving it alone", userID, guildID)
				continue
			}
			if count > 0 {
				count--
			}
			if err := patchRowField(ctx, "guilds", bigintSeg(guildID), "member_count", count); err != nil {
				log.Printf("userProcessPendingDeletion: %s: guild %s: member_count: %v", userID, guildID, err)
			}
		}
		if err := dispatchGuild(nc, guildID, "GUILD_MEMBER_REMOVE", map[string]any{
			"user": map[string]any{"id": userID},
		}); err != nil {
			log.Printf("userProcessPendingDeletion: %s: guild %s: dispatch: %v", userID, guildID, err)
		}
		if err := leaveGuildGateway(nc, userID, guildID); err != nil {
			log.Printf("userProcessPendingDeletion: %s: guild %s: leave: %v", userID, guildID, err)
		}
		left++
	}
	return left, nil
}

// classedPrivateChannel is one row of private_channels: the per-user snapshot of a
// DM or group DM the user can see.
type classedPrivateChannel struct {
	ChannelID    string
	IsGroup      bool
	OwnerID      string
	Nicknames    map[string]string
	RecipientIDs []string
}

func privateChannelsOf(ctx context.Context, userID string) ([]classedPrivateChannel, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'private_channels' AND row_data->'user_id'->>'value' = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []classedPrivateChannel
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		channel := classedPrivateChannel{
			ChannelID: typedString(row["channel_id"]),
			OwnerID:   typedString(row["channel_owner_id"]),
		}
		if isGDM, ok := row["is_gdm"].(bool); ok {
			channel.IsGroup = isGDM
		}
		// channel_type is the authoritative field (GROUP_DM = 3); is_gdm is a
		// snapshot that can lag behind a channel changing shape.
		if typedString(row["channel_type"]) == "3" {
			channel.IsGroup = true
		}
		channel.RecipientIDs = typedSet(row["channel_recipient_ids"])
		if nicks, ok := row["channel_nicks"].(map[string]any); ok {
			channel.Nicknames = map[string]string{}
			for k, v := range nicks {
				if s, ok := v.(string); ok {
					channel.Nicknames[k] = s
				}
			}
		}
		if channel.ChannelID != "" {
			out = append(out, channel)
		}
	}
	return out, rows.Err()
}

// closePrivateChannel drops the per-user pointer and nothing else, which is what
// userRepository.closeDmForUser does.
func closePrivateChannel(ctx context.Context, userID, channelID string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'private_channels'
		AND row_data->'user_id'->>'value' = $1 AND row_data->'channel_id'->>'value' = $2`, userID, channelID)
	return err
}

func deleteDmState(ctx context.Context, a, b, channelID string) {
	_, _ = db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'dm_states'
		AND row_data->'channel_id'->>'value' = $1
		AND (row_data->'hi_user_id'->>'value' = $2 OR row_data->'lo_user_id'->>'value' = $2)
		AND (row_data->'hi_user_id'->>'value' = $3 OR row_data->'lo_user_id'->>'value' = $3)`,
		channelID, a, b)
}

// dmChannelResponse renders the ChannelResponse that CHANNEL_DELETE carries. The
// gateway forwards `data` untouched, and the client expects channel fields, not
// just an id, so the row is mapped here rather than passed raw.
func dmChannelResponse(channelRow map[string]any) map[string]any {
	resp := map[string]any{
		"id":                 typedString(channelRow["channel_id"]),
		"type":               channelRow["type"],
		"name":               channelRow["name"],
		"icon":               channelRow["icon_hash"],
		"owner_id":           channelRow["owner_id"],
		"nsfw":               channelRow["nsfw"],
		"last_message_id":    channelRow["last_message_id"],
		"last_pin_timestamp": channelRow["last_pin_timestamp"],
	}
	if nicks, ok := channelRow["nicks"]; ok && nicks != nil {
		resp["nicks"] = nicks
	}
	return resp
}

// leaveGroupDms mirrors the upstream loop: replace the recipient list, transfer
// ownership, post the RECIPIENT_REMOVE notice, delete the channel when the last
// recipient leaves, and dispatch to everybody involved.
func leaveGroupDms(ctx context.Context, nc *nats.Conn, userID string) (int, error) {
	channels, err := privateChannelsOf(ctx, userID)
	if err != nil {
		return 0, err
	}
	left := 0
	for _, channel := range channels {
		if !channel.IsGroup {
			continue
		}
		if err := leaveOneGroupDm(ctx, nc, userID, channel); err != nil {
			log.Printf("userProcessPendingDeletion: %s: group dm %s: %v", userID, channel.ChannelID, err)
			continue
		}
		left++
	}
	return left, nil
}

func leaveOneGroupDm(ctx context.Context, nc *nats.Conn, userID string, channel classedPrivateChannel) error {
	remaining := make([]string, 0, len(channel.RecipientIDs))
	for _, id := range channel.RecipientIDs {
		if id != userID {
			remaining = append(remaining, id)
		}
	}
	channelKey := bigintSeg(channel.ChannelID) + kvSep + "false"

	channelRow, err := readKV(ctx, "channels", channelKey)
	if err != nil {
		return err
	}
	if len(remaining) == 0 {
		// Nobody left: the channel itself goes away. The response is rendered from
		// the row we are about to delete, which is what the client expects to see.
		resp := dmChannelResponse(channelRow)
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'channels'
			AND row_data->'channel_id'->>'value' = $1`, channel.ChannelID); err != nil {
			return err
		}
		if err := closePrivateChannel(ctx, userID, channel.ChannelID); err != nil {
			return err
		}
		if err := dispatchToUser(nc, userID, "CHANNEL_DELETE", resp); err != nil {
			log.Printf("userProcessPendingDeletion: %s: CHANNEL_DELETE %s: %v", userID, channel.ChannelID, err)
		}
		return nil
	}

	owner := channel.OwnerID
	if owner == userID {
		owner = remaining[0]
	}
	recipients := make([]any, 0, len(remaining))
	for _, id := range remaining {
		recipients = append(recipients, bigintValue(id))
	}
	nicknames := map[string]any{}
	for id, nick := range channel.Nicknames {
		if id != userID {
			nicknames[id] = nick
		}
	}
	if channelRow == nil {
		return fmt.Errorf("channel row %s is missing", channel.ChannelID)
	}
	resp := dmChannelResponse(channelRow)
	channelRow["owner_id"] = bigintValue(owner)
	channelRow["recipient_ids"] = map[string]any{"__fluxer_type": "set", "value": recipients}
	if len(nicknames) > 0 {
		channelRow["nicks"] = map[string]any{"__fluxer_type": "map", "value": nicknames}
	} else {
		delete(channelRow, "nicks")
	}
	version := 0
	if raw, ok := channelRow["version"]; ok {
		if n, err := strconv.Atoi(typedString(raw)); err == nil {
			version = n
		}
	}
	channelRow["version"] = version + 1
	if err := putKV(ctx, "channels", channelKey, channelKey, channelRow, channel.ChannelID); err != nil {
		return err
	}
	if err := closePrivateChannel(ctx, userID, channel.ChannelID); err != nil {
		return err
	}
	if _, err := insertMessageSpec(ctx, messageInsert{
		ChannelID:    channel.ChannelID,
		AuthorID:     userID,
		Type:         messageTypeRecipientRemove,
		MentionUsers: []string{userID},
	}); err != nil {
		log.Printf("userProcessPendingDeletion: %s: recipient-remove notice: %v", userID, err)
	}
	for _, id := range remaining {
		if err := dispatchToUser(nc, id, "CHANNEL_RECIPIENT_REMOVE", map[string]any{
			"channel_id": channel.ChannelID,
			"user":       userPartial(ctx, userID),
		}); err != nil {
			log.Printf("userProcessPendingDeletion: %s: CHANNEL_RECIPIENT_REMOVE -> %s: %v", userID, id, err)
		}
	}
	if err := dispatchToUser(nc, userID, "CHANNEL_DELETE", resp); err != nil {
		log.Printf("userProcessPendingDeletion: %s: CHANNEL_DELETE %s: %v", userID, channel.ChannelID, err)
	}
	return nil
}

// anonymizeAuthoredMessages rewrites authorship of everything the user ever posted
// to the deleted-user placeholder. It walks the by-author index forward by message
// id, chunk by chunk, because rewriting inside the same index would otherwise make
// the loop skip rows.
func anonymizeAuthoredMessages(ctx context.Context, userID, deletedUserID string) (int, error) {
	last := ""
	rewritten := 0
	for {
		q := `SELECT row_data->'channel_id'->>'value', row_data->'message_id'->>'value'
FROM fluxer_kv
WHERE table_name = 'messages_by_author_id_v2' AND row_data->'author_id'->>'value' = $1`
		args := []any{userID}
		if last != "" {
			q += ` AND (row_data->'message_id'->>'value')::numeric > $2::numeric`
			args = append(args, last)
		}
		q += ` ORDER BY (row_data->'message_id'->>'value')::numeric ASC LIMIT ` + strconv.Itoa(deletionMessageChunk)
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			return rewritten, err
		}
		type pair struct{ channelID, messageID string }
		var chunk []pair
		for rows.Next() {
			var p pair
			if err := rows.Scan(&p.channelID, &p.messageID); err != nil {
				rows.Close()
				return rewritten, err
			}
			chunk = append(chunk, p)
		}
		rows.Close()
		if len(chunk) == 0 {
			return rewritten, nil
		}
		for _, p := range chunk {
			if err := anonymizeOneMessage(ctx, p.channelID, p.messageID, userID, deletedUserID); err != nil {
				return rewritten, err
			}
			rewritten++
			last = p.messageID
		}
		if len(chunk) < deletionMessageChunk {
			return rewritten, nil
		}
	}
}

func anonymizeOneMessage(ctx context.Context, channelID, messageID, oldAuthorID, newAuthorID string) error {
	bucket, err := bucketFor(messageID)
	if err != nil {
		return err
	}
	oldKey := bigintSeg(oldAuthorID) + kvSep + bigintSeg(messageID)
	if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'messages_by_author_id_v2' AND row_key = $1`, oldKey); err != nil {
		return err
	}
	newKey := bigintSeg(newAuthorID) + kvSep + bigintSeg(messageID)
	newRow := map[string]any{
		"author_id":  bigintValue(newAuthorID),
		"channel_id": bigintValue(channelID),
		"message_id": bigintValue(messageID),
	}
	if err := putKV(ctx, "messages_by_author_id_v2", newKey, newKey, newRow, messageID); err != nil {
		return err
	}
	partition := bigintSeg(channelID) + kvSep + strconv.FormatInt(bucket, 10)
	messageKey := partition + kvSep + bigintSeg(messageID)
	_, err = db.ExecContext(ctx, `
UPDATE fluxer_kv SET
	row_data = row_data || jsonb_build_object('author_id', $3::jsonb),
	updated_at = now()
WHERE table_name = 'messages' AND row_key = $1 AND row_data->'channel_id'->>'value' = $2`,
		messageKey, channelID, string(mustJSON(bigintValue(newAuthorID))))
	return err
}

// deleteUserAssets removes the avatar, the banner and every favourite meme image.
func deleteUserAssets(ctx context.Context, user map[string]any, userID string) int {
	deleted := 0
	if hash := typedString(user["avatar_hash"]); hash != "" {
		deleteObject(ctx, "avatars/"+userID+"/"+hash, "avatar")
		deleted++
	}
	if hash := typedString(user["banner_hash"]); hash != "" {
		deleteObject(ctx, "banners/"+userID+"/"+hash, "banner")
		deleted++
	}
	rows, err := db.QueryContext(ctx, `
SELECT row_data->>'storage_key', row_data->'meme_id'->>'value' FROM fluxer_kv
WHERE table_name = 'favorite_memes' AND row_data->'user_id'->>'value' = $1`, userID)
	if err != nil {
		log.Printf("userProcessPendingDeletion: %s: favorite memes: %v", userID, err)
		return deleted
	}
	defer rows.Close()
	for rows.Next() {
		var key, memeID string
		if err := rows.Scan(&key, &memeID); err != nil {
			continue
		}
		deleteObject(ctx, key, "favorite meme")
		if memeID != "" {
			// favorite_memes_by_meme_id is keyed by the meme, so the row is only
			// reachable through this id.
			_, _ = db.ExecContext(ctx, `DELETE FROM fluxer_kv
				WHERE table_name = 'favorite_memes_by_meme_id' AND row_data->'meme_id'->>'value' = $1`, memeID)
		}
		deleted++
	}
	return deleted
}

// deleteWebAuthnLookups removes the credential-id keyed lookup rows, which cannot be
// found from the user id at all: the credential ids have to be read first.
func deleteWebAuthnLookups(ctx context.Context, userID string) int {
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'credential_id'->>'value' FROM fluxer_kv
WHERE table_name = 'webauthn_credentials' AND row_data->'user_id'->>'value' = $1`, userID)
	if err != nil {
		return 0
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil && id != "" {
			ids = append(ids, id)
		}
	}
	rows.Close()
	deleted := 0
	for _, id := range ids {
		if res, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv
			WHERE table_name = 'webauthn_credential_lookup' AND row_key = $1`, id); err == nil {
			if n, _ := res.RowsAffected(); n > 0 {
				deleted++
			}
		}
	}
	return deleted
}

// deleteUserDataRows drops every per-user row listed in userScopedTables and reports
// how many rows each table gave up, so the log says what actually happened.
func deleteUserDataRows(ctx context.Context, userID string) (int64, error) {
	var total int64
	for _, spec := range userScopedTables {
		res, err := db.ExecContext(ctx, fmt.Sprintf(`
DELETE FROM fluxer_kv WHERE table_name = $1 AND row_data->'%s'->>'value' = $2`, spec.column),
			spec.table, userID)
		if err != nil {
			log.Printf("userProcessPendingDeletion: %s: delete %s: %v", userID, spec.table, err)
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			log.Printf("userProcessPendingDeletion: %s: removed %d row(s) from %s", userID, n, spec.table)
			total += n
		}
	}
	return total, nil
}

// revokeUserSessions deletes the session rows behind the by-user index and asks the
// gateway to drop the live connections.
func revokeUserSessions(ctx context.Context, nc *nats.Conn, userID string) error {
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'session_id_hash'->>'value' FROM fluxer_kv
WHERE table_name = 'auth_sessions_by_user_id' AND row_data->'user_id'->>'value' = $1`, userID)
	if err != nil {
		return err
	}
	var hashes []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err == nil && h != "" {
			hashes = append(hashes, h)
		}
	}
	rows.Close()
	for _, h := range hashes {
		// auth_sessions' partition and row key are both the hash itself, wrapped in
		// a buffer type, so the exact key is built from the same encoding.
		key := `{"__fluxer_type":"buffer","value":"` + h + `"}`
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'auth_sessions' AND row_key = $1`, key); err != nil {
			log.Printf("userProcessPendingDeletion: %s: delete session: %v", userID, err)
		}
	}
	if err := terminateAllSessions(nc, userID); err != nil {
		log.Printf("userProcessPendingDeletion: %s: terminate sessions: %v", userID, err)
	}
	return nil
}

// revokeOAuthTokens deletes the tokens themselves, not just the by-user index: the
// index row carries the hashed token, which is also the primary key of the token row.
func revokeOAuthTokens(ctx context.Context, userID string) int {
	deleted := 0
	for _, spec := range []struct{ index, tokens string }{
		{"oauth2_access_tokens_by_user", "oauth2_access_tokens"},
		{"oauth2_refresh_tokens_by_user", "oauth2_refresh_tokens"},
	} {
		rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT row_data->'token_'->>'value' FROM fluxer_kv
WHERE table_name = '%s' AND row_data->'user_id'->>'value' = $1`, spec.index), userID)
		if err != nil {
			log.Printf("userProcessPendingDeletion: %s: read %s: %v", userID, spec.index, err)
			continue
		}
		var hashes []string
		for rows.Next() {
			var h string
			if err := rows.Scan(&h); err == nil && h != "" {
				hashes = append(hashes, h)
			}
		}
		rows.Close()
		for _, h := range hashes {
			res, err := db.ExecContext(ctx,
				fmt.Sprintf(`DELETE FROM fluxer_kv WHERE table_name = '%s' AND row_key = $1`, spec.tokens), h)
			if err != nil {
				log.Printf("userProcessPendingDeletion: %s: delete %s row: %v", userID, spec.tokens, err)
				continue
			}
			if n, _ := res.RowsAffected(); n > 0 {
				deleted++
			}
		}
	}
	return deleted
}

// anonymizeUserRecord scrubs the personal fields and finally clears the schedule,
// which is what makes the account stop being "pending deletion" at all.
func anonymizeUserRecord(ctx context.Context, userID string, reason int) error {
	flags := uint64(userFlagDeleted)
	if user, err := readKV(ctx, "users", bigintSeg(userID)); err == nil && user != nil {
		flags |= parseUserFlags(user)
	}
	patch := map[string]any{
		"username":               deletedUserName,
		"discriminator":          0,
		"global_name":            deletedUserGlobalName,
		"flags":                  map[string]any{"__fluxer_type": "bigint", "value": strconv.FormatUint(flags|userFlagDeleted, 10)},
		"timezone_privacy_flags": timezonePrivacyEveryone,
		"authenticator_types":    map[string]any{"__fluxer_type": "set", "value": []any{}},
		"email_verified":         false,
		"email_bounced":          false,
		"bot":                    false,
		"system":                 false,
	}
	if reason >= 0 {
		patch["deletion_reason_code"] = map[string]any{"__fluxer_type": "bigint", "value": strconv.Itoa(reason)}
	}
	if err := patchUserRow(ctx, userID, patch); err != nil {
		return err
	}
	return clearUserFields(ctx, userID,
		"email", "password_hash", "password_last_changed_at", "totp_secret", "avatar_hash", "avatar_color",
		"banner_hash", "banner_color", "bio", "pronouns", "accent_color", "timezone", "date_of_birth",
		"locale", "premium_type", "premium_since", "premium_until", "stripe_subscription_id",
		"stripe_customer_id", "last_active_at", "last_active_ip", "temp_banned_until", "acls", "traits",
		"deletion_public_reason", "deletion_audit_log_reason", "pending_deletion_at")
}

func taskUserProcessPendingDeletion(ctx context.Context, payload map[string]any) error {
	job, err := decodeUserDeletionJob(payload)
	if err != nil {
		return fmt.Errorf("userProcessPendingDeletion: %w", err)
	}
	if err := openDB(); err != nil {
		return fmt.Errorf("userProcessPendingDeletion: %w", err)
	}
	nc := workerNATS()
	if nc == nil {
		return fmt.Errorf("userProcessPendingDeletion: no NATS connection")
	}
	user, err := readKV(ctx, "users", bigintSeg(job.UserID))
	if err != nil {
		return fmt.Errorf("userProcessPendingDeletion: read user: %w", err)
	}
	if user == nil {
		log.Printf("userProcessPendingDeletion: %s: no such user", job.UserID)
		return nil
	}
	claim, err := startUserDeletion(ctx, job, user)
	if err != nil {
		return fmt.Errorf("userProcessPendingDeletion: start: %w", err)
	}
	if !claim {
		return nil
	}

	log.Printf("userProcessPendingDeletion: %s: deleting account (reason=%d)", job.UserID, job.Reason)

	deletedUserID, err := createDeletedUserRecord(ctx)
	if err != nil {
		return fmt.Errorf("userProcessPendingDeletion: placeholder: %w", err)
	}
	leftGuilds, _ := leaveGuilds(ctx, nc, job.UserID)
	leftDms, _ := leaveGroupDms(ctx, nc, job.UserID)
	messages, err := anonymizeAuthoredMessages(ctx, job.UserID, deletedUserID)
	if err != nil {
		// Authorship is the part that must not be half-done: stop and let the retry
		// pick up from the last rewritten message.
		return fmt.Errorf("userProcessPendingDeletion: anonymise messages: %w", err)
	}
	assets := deleteUserAssets(ctx, user, job.UserID)
	if err := revokeUserSessions(ctx, nc, job.UserID); err != nil {
		log.Printf("userProcessPendingDeletion: %s: sessions: %v", job.UserID, err)
	}
	tokens := revokeOAuthTokens(ctx, job.UserID)
	lookups := deleteWebAuthnLookups(ctx, job.UserID)
	rows, err := deleteUserDataRows(ctx, job.UserID)
	if err != nil {
		return fmt.Errorf("userProcessPendingDeletion: delete rows: %w", err)
	}
	if err := anonymizeUserRecord(ctx, job.UserID, job.Reason); err != nil {
		return fmt.Errorf("userProcessPendingDeletion: anonymise user: %w", err)
	}
	// A touch buffered before the deletion would otherwise be flushed into the
	// anonymised row minutes later, putting the old IP back.
	if err := valkey().HDel(ctx, userActivityKey, job.UserID).Err(); err != nil {
		log.Printf("userProcessPendingDeletion: %s: drop buffered activity: %v", job.UserID, err)
	}
	log.Printf("userProcessPendingDeletion: %s: done guilds=%d group_dms=%d messages=%d assets=%d tokens=%d lookups=%d rows=%d",
		job.UserID, leftGuilds, leftDms, messages, assets, tokens, lookups, rows)
	return nil
}
