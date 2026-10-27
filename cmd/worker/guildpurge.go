package main

// taskGuildProcessDeletion ports GuildProcessDeletion.ts (purgeGuild).
//
// Deleting a guild is a cascade across two dozen tables, and the api runs it in
// this worker because it must not hold a request open for minutes. The order is
// the same as upstream's: tell the guild first, then take the content and the
// memberships away, then the guild row itself.
//
// Every deletion here is either "all rows whose row key starts with the guild id"
// or "all rows whose guild_id column equals it", which is exactly how the api's
// per-table repositories express the same statements. The second form is used for
// the reverse indexes (bans by user, emojis by id, invites by code, …), where the
// guild id lives inside the document rather than at the front of the key.
//
// Not ported, all because the machinery is absent:
//   - crosspost follower removal scheduling (no crosspost consumers);
//   - search index deletion (no search backend);
//   - removing the guild from users' folders and the guild-visits cleanup job
//     (needs the folder and visit services);
//   - the thread-only channel and guild-thread marker cleanup beyond the plain
//     row deletes below.

import (
	"context"
	"fmt"
	"log"
)

// Tables whose row keys start with the guild id.
var guildKeyedTables = []string{
	"guild_roles",
	"guild_emojis",
	"guild_stickers",
	"guild_bans",
	"guild_members",
	"guild_thread_state",
	"guild_positions",
	"guild_visits_by_guild",
	"guild_discovery",
	"invites_by_guild_id",
	"webhooks_by_guild_id",
	"channels_by_guild_id",
	"active_threads_by_guild",
	"thread_parent_config",
	"thread_only_channels_by_guild",
	"forum_pinned_thread",
}

// Tables whose guild id lives in the document, not in the key prefix.
var guildDocumentTables = []string{
	"guild_emojis_by_emoji_id",
	"guild_stickers_by_sticker_id",
	"guild_bans_by_user_id",
	"guild_bans_by_email",
	"guild_members_by_user_id",
	"invites",
	"webhooks",
	"webhooks_by_channel_id",
	"webhooks_by_source_channel_id",
	"guild_discovery_by_status",
	"threads_by_parent",
	"archived_threads_by_parent",
	"thread_members",
	"thread_members_by_user",
	"guild_visits_by_user",
}

func taskGuildProcessDeletion(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("guildProcessDeletion: %w", err)
		}
	}
	guildID := stringField(payload, "guildId")
	if !isDigits(guildID) {
		return fmt.Errorf("guildProcessDeletion: guildId is required")
	}
	guild, err := readKV(ctx, guildsTable, bigintSeg(guildID))
	if err != nil {
		return fmt.Errorf("guildProcessDeletion: read guild: %w", err)
	}
	if guild == nil {
		log.Printf("guildProcessDeletion: guild %s does not exist, nothing to do", guildID)
		return nil
	}

	// The guild goes away for its clients before anything is deleted, so nobody
	// keeps sending into a guild that is being torn down.
	if nc := workerNATS(); nc != nil {
		if err := dispatchGuild(nc, guildID, "GUILD_DELETE", map[string]any{"id": guildID}); err != nil {
			log.Printf("guildProcessDeletion: guild %s: GUILD_DELETE dispatch: %v (continuing)", guildID, err)
		}
	}

	channels, err := guildChannelIDs(ctx, guildID)
	if err != nil {
		return fmt.Errorf("guildProcessDeletion: list channels: %w", err)
	}
	for _, channelID := range channels {
		if err := purgeThreadsOfChannel(ctx, guildID, channelID); err != nil {
			log.Printf("guildProcessDeletion: channel %s threads: %v (continuing)", channelID, err)
		}
		if err := deleteAllMessagesInChannel(ctx, channelID); err != nil {
			log.Printf("guildProcessDeletion: channel %s messages: %v (continuing)", channelID, err)
		}
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'channels' AND row_key = $1`,
			bigintSeg(channelID)+kvSep+boolSeg(false)); err != nil {
			return fmt.Errorf("guildProcessDeletion: delete channel %s: %w", channelID, err)
		}
	}

	members, err := guildMemberIDs(ctx, guildID)
	if err != nil {
		return fmt.Errorf("guildProcessDeletion: list members: %w", err)
	}
	nc := workerNATS()
	for _, userID := range members {
		if nc != nil {
			if err := leaveGuildGateway(nc, userID, guildID); err != nil {
				log.Printf("guildProcessDeletion: user %s leave: %v (continuing)", userID, err)
			}
		}
		// Per-user guild settings are keyed by the member, so they are not covered
		// by the prefix deletes below.
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'user_guild_settings' AND row_key = $1`,
			bigintSeg(userID)+kvSep+bigintSeg(guildID)); err != nil {
			log.Printf("guildProcessDeletion: user %s settings: %v (continuing)", userID, err)
		}
	}

	if err := deleteGuildRows(ctx, guildID); err != nil {
		return fmt.Errorf("guildProcessDeletion: %w", err)
	}
	if err := valkey().Del(ctx, threadArchiveQueuePrefix+guildID).Err(); err != nil {
		log.Printf("guildProcessDeletion: archive queue: %v (rows removed)", err)
	}
	_ = valkey().SRem(ctx, threadArchiveGuildsKey, guildID).Err()

	log.Printf("guildProcessDeletion: guild=%s channels=%d members=%d purged", guildID, len(channels), len(members))
	return nil
}

// purgeThreadsOfChannel removes the threads that hang off a channel.
func purgeThreadsOfChannel(ctx context.Context, guildID, parentID string) error {
	threadIDs, err := threadIDsByParent(ctx, parentID)
	if err != nil {
		return err
	}
	for _, threadID := range threadIDs {
		if err := purgeThread(ctx, guildID, parentID, threadID); err != nil {
			return fmt.Errorf("thread %s: %w", threadID, err)
		}
	}
	return nil
}

func guildMemberIDs(ctx context.Context, guildID string) ([]string, error) {
	prefix := bigintSeg(guildID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'user_id'->>'value' FROM fluxer_kv
WHERE table_name = 'guild_members' AND left(row_key, length($1)) = $1`, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var userID *string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		if userID != nil && isDigits(*userID) {
			out = append(out, *userID)
		}
	}
	return out, rows.Err()
}

// deleteGuildRows removes the guild-keyed tables, the document-keyed indexes and
// finally the guild row itself.
func deleteGuildRows(ctx context.Context, guildID string) error {
	prefix := bigintSeg(guildID) + kvSep
	for _, table := range guildKeyedTables {
		if _, err := db.ExecContext(ctx, `
DELETE FROM fluxer_kv WHERE table_name = $1 AND left(row_key, length($2)) = $2`, table, prefix); err != nil {
			return fmt.Errorf("delete %s: %w", table, err)
		}
	}
	for _, table := range guildDocumentTables {
		if _, err := db.ExecContext(ctx, `
DELETE FROM fluxer_kv WHERE table_name = $1 AND row_data->'guild_id'->>'value' = $2`, table, guildID); err != nil {
			return fmt.Errorf("delete %s: %w", table, err)
		}
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		guildsTable, bigintSeg(guildID)); err != nil {
		return fmt.Errorf("delete guild row: %w", err)
	}
	return nil
}
