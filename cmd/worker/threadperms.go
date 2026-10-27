package main

// taskSeedThreadPermissions ports SeedThreadPermissions.ts.
//
// Threads arrived after the guild permission model, and the thread-only bits
// (create public/private threads, send in threads) had to be granted to channels
// and roles that already existed. This one-shot task rewrites what exists:
//
//   - every channel permission overwrite loses the thread bits and, when it grants
//     SEND_MESSAGES, gains all three;
//   - the @everyone role gets the three bits when it can send messages;
//   - the guild marker records that the seed ran, so it is not repeated.
//
// The two bulk dispatches the api sends afterwards (GUILD_ROLE_UPDATE_BULK and
// CHANNEL_UPDATE_BULK) are not ported: they need the channel response mapper.
// Clients see the new bits on their next fetch.
//
// Encoding note: channel overwrites are written by the api as a map of target id →
// {type, allow_, deny_}; on a guild without overwrites the column is null. The
// parser here accepts that map shape and says so in the log when it meets
// something else, rather than guessing at bits it cannot see.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
)

// Thread-only permission bits, from @fluxer/constants ThreadPermissionFlags.
const (
	permSendMessages        uint64 = 1 << 11
	permCreatePublicThreads uint64 = 1 << 35
	permCreatePrivateThread uint64 = 1 << 36
	permSendInThreads       uint64 = 1 << 38
	threadPermissionBits           = permCreatePublicThreads | permCreatePrivateThread | permSendInThreads
)

func taskSeedThreadPermissions(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("seedThreadPermissions: %w", err)
		}
	}
	guildID := stringField(payload, "guildId")
	if !isDigits(guildID) {
		return fmt.Errorf("seedThreadPermissions: guildId is required")
	}
	if row, err := readKV(ctx, guildsTable, bigintSeg(guildID)); err != nil {
		return fmt.Errorf("seedThreadPermissions: read guild: %w", err)
	} else if row == nil {
		log.Printf("seedThreadPermissions: guild %s does not exist", guildID)
		return nil
	}

	channels, err := seedGuildChannels(ctx, guildID)
	if err != nil {
		return fmt.Errorf("seedThreadPermissions: channels: %w", err)
	}
	roleChanged, err := seedEveryoneRole(ctx, guildID)
	if err != nil {
		return fmt.Errorf("seedThreadPermissions: everyone role: %w", err)
	}
	if err := markThreadPermsSeeded(ctx, guildID); err != nil {
		return fmt.Errorf("seedThreadPermissions: marker: %w", err)
	}
	log.Printf("seedThreadPermissions: guild=%s channels=%d role_updated=%v", guildID, channels, roleChanged)
	return nil
}

// seedGuildChannels rewrites the overwrites of every channel that has them and
// returns how many channels it looked at.
func seedGuildChannels(ctx context.Context, guildID string) (int, error) {
	prefix := bigintSeg(guildID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_key, row_data::text FROM fluxer_kv
WHERE table_name = 'channels_by_guild_id' AND left(row_key, length($1)) = $1`, prefix)
	if err != nil {
		return 0, err
	}
	var channelIDs []string
	for rows.Next() {
		var rowKey, raw string
		if err := rows.Scan(&rowKey, &raw); err != nil {
			rows.Close()
			return 0, err
		}
		if id := parseChannelIDFromRow(raw); id != "" {
			channelIDs = append(channelIDs, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, channelID := range channelIDs {
		channel, err := readKV(ctx, "channels", bigintSeg(channelID)+kvSep+boolSeg(false))
		if err != nil {
			return 0, err
		}
		if channel == nil {
			continue
		}
		overwrites, ok := channel["permission_overwrites"].(map[string]any)
		if !ok || overwrites == nil {
			continue // null column: nothing to seed on this channel
		}
		updated, changed := seedOverwriteMap(overwrites)
		if !changed {
			continue
		}
		if err := patchRowField(ctx, "channels", bigintSeg(channelID)+kvSep+boolSeg(false),
			"permission_overwrites", updated); err != nil {
			return 0, fmt.Errorf("channel %s overwrites: %w", channelID, err)
		}
	}
	return len(channelIDs), nil
}

// seedOverwriteMap applies the thread-bit rule to every entry of the api's map.
func seedOverwriteMap(overwrites map[string]any) (map[string]any, bool) {
	list, ok := overwrites["value"].([]any)
	if !ok {
		log.Printf("seedThreadPermissions: permission_overwrites is not a list of pairs, left untouched")
		return nil, false
	}
	changed := false
	out := make([]any, 0, len(list))
	for _, item := range list {
		pair, ok := item.([]any)
		if !ok || len(pair) != 2 {
			out = append(out, item)
			continue
		}
		target, _ := pair[0].(string)
		body, ok := pair[1].(map[string]any)
		if !ok {
			out = append(out, item)
			continue
		}
		allow, _ := parseBigintString(typedString(body["allow_"]))
		deny, _ := parseBigintString(typedString(body["deny_"]))
		newAllow := seedThreadBits(allow)
		newDeny := seedThreadBits(deny)
		if newAllow != allow || newDeny != deny {
			changed = true
			body["allow_"] = strconv.FormatUint(newAllow, 10)
			body["deny_"] = strconv.FormatUint(newDeny, 10)
		}
		out = append(out, []any{target, body})
	}
	if !changed {
		return nil, false
	}
	overwrites["value"] = out
	return overwrites, true
}

// seedThreadBits clears the three thread bits and, when SEND_MESSAGES is granted,
// grants them back. A deny or allow without SEND_MESSAGES simply loses them.
func seedThreadBits(value uint64) uint64 {
	cleared := value &^ threadPermissionBits
	if value&permSendMessages != 0 {
		return cleared | threadPermissionBits
	}
	return cleared
}

// seedEveryoneRole grants the thread bits to the @everyone role when it can send
// messages and does not have them yet. Returns whether it changed the role.
func seedEveryoneRole(ctx context.Context, guildID string) (bool, error) {
	// The everyone role's id is the guild's id.
	roleKey := bigintSeg(guildID) + kvSep + bigintSeg(guildID)
	role, err := readKV(ctx, "guild_roles", roleKey)
	if err != nil {
		return false, err
	}
	if role == nil {
		return false, nil
	}
	permissions, ok := parseBigintString(typedString(role["permissions"]))
	if !ok {
		return false, fmt.Errorf("unreadable permissions on the everyone role")
	}
	if permissions&permSendMessages == 0 {
		return false, nil
	}
	seeded := permissions | threadPermissionBits
	if seeded == permissions {
		return false, nil
	}
	if err := patchRowField(ctx, "guild_roles", roleKey, "permissions", map[string]any{
		"__fluxer_type": "bigint", "value": strconv.FormatUint(seeded, 10),
	}); err != nil {
		return false, err
	}
	return true, nil
}

// markThreadPermsSeeded writes the marker the api checks before queueing this task.
func markThreadPermsSeeded(ctx context.Context, guildID string) error {
	key := bigintSeg(guildID)
	row, err := readKV(ctx, "guild_thread_state", key)
	if err != nil {
		return err
	}
	if row == nil {
		return putKV(ctx, "guild_thread_state", key, key, map[string]any{
			"guild_id":        bigintValue(guildID),
			"perms_seeded_at": kvDate(time.Now()),
		}, "")
	}
	return patchRowField(ctx, "guild_thread_state", key, "perms_seeded_at", kvDate(time.Now()))
}

// parseChannelIDFromRow reads the channel id out of a channels_by_guild_id row.
func parseChannelIDFromRow(raw string) string {
	var row map[string]any
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		return ""
	}
	return typedString(row["channel_id"])
}

// parseBigintString reads a typed bigint or a plain decimal string.
func parseBigintString(v string) (uint64, bool) {
	if v == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
