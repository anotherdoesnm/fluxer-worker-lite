package main

// taskHarvestGuildData ports HarvestGuildData.ts.
//
// The admin archive of a guild: one `guild.json` with the guild, its roles, members,
// channels, emojis and stickers, and one `channels/<id>/messages.json` per text-based
// channel with the most recent messages. The archive lands in the harvests bucket and
// the `admin_archives_by_subject` row carries its progress, size and link expiry —
// the same contract the user export has, because it feeds the same admin screen.
//
// The row is keyed (subject_type, subject_id, archive_id), which is why the task
// refuses to run when it is missing: the request that queued the job wrote that row,
// and inventing it here would orphan the job from the screen that shows it.
//
// Ported limits: 1000 messages per channel, newest first, text/voice/announcement
// channels. Not ported, in OMISSIONS.md: attachment download and the asset bundle
// (guild icon, banners, emoji files), thread metadata beyond their channels, and the
// completions notice.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"
)

const (
	adminArchivesTable   = "admin_archives_by_subject"
	guildArchiveMessages = 1000
	archiveLinkTTL       = 7 * 24 * time.Hour
)

func taskHarvestGuildData(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("harvestGuildData: %w", err)
		}
	}
	guildID := stringField(payload, "guildId")
	archiveID := stringField(payload, "archiveId")
	if !isDigits(guildID) || !isDigits(archiveID) {
		return fmt.Errorf("harvestGuildData: guildId and archiveId are required")
	}
	rowKey := guildArchiveRowKey(guildID, archiveID)
	existing, err := readKV(ctx, adminArchivesTable, rowKey)
	if err != nil {
		return fmt.Errorf("harvestGuildData: read record: %w", err)
	}
	if existing == nil {
		return fmt.Errorf("harvestGuildData: admin archive record not found for guild %s", guildID)
	}
	if typedString(existing["completed_at"]) != "" {
		log.Printf("harvestGuildData: guild=%s archive=%s already completed, skipping", guildID, archiveID)
		return nil
	}
	if typedString(existing["terminal_failed_at"]) != "" {
		return fmt.Errorf("harvestGuildData: archive failed terminally: %s", typedString(existing["error_message"]))
	}
	attemptID, err := randomToken()
	if err != nil {
		return fmt.Errorf("harvestGuildData: attempt id: %w", err)
	}
	expiresAt := time.Now().UTC().Add(archiveLinkTTL)
	if err := writeGuildArchiveProgress(ctx, rowKey, map[string]any{
		"attempt_id":       attemptID,
		"started_at":       kvDate(time.Now().UTC()),
		"expires_at":       kvDate(expiresAt),
		"progress_percent": 5,
		"progress_step":    "collecting",
		"error_message":    nil,
	}); err != nil {
		return fmt.Errorf("harvestGuildData: start: %w", err)
	}

	data, err := collectGuildArchive(ctx, guildID)
	if err != nil {
		writeGuildArchiveFailure(ctx, rowKey, err)
		return fmt.Errorf("harvestGuildData: collect: %w", err)
	}
	entries, err := guildArchiveEntries(data)
	if err != nil {
		writeGuildArchiveFailure(ctx, rowKey, err)
		return fmt.Errorf("harvestGuildData: entries: %w", err)
	}
	archive, err := buildZip(entries)
	if err != nil {
		writeGuildArchiveFailure(ctx, rowKey, err)
		return fmt.Errorf("harvestGuildData: zip: %w", err)
	}
	storageKey := fmt.Sprintf("archives/guilds/%s/%s/%s/guild-archive.zip", guildID, archiveID, attemptID)
	endpoint, _, accessKey, secretKey, region := s3Config()
	if err := s3PutTo(ctx, endpoint, harvestBucket(), accessKey, secretKey, region, storageKey, archive, "application/zip"); err != nil {
		writeGuildArchiveFailure(ctx, rowKey, err)
		return fmt.Errorf("harvestGuildData: upload: %w", err)
	}
	if err := writeGuildArchiveProgress(ctx, rowKey, map[string]any{
		"completed_at":            kvDate(time.Now().UTC()),
		"storage_key":             storageKey,
		"file_size":               bigintValue(strconv.Itoa(len(archive))),
		"progress_percent":        100,
		"progress_step":           "completed",
		"download_url_expires_at": kvDate(expiresAt),
	}); err != nil {
		return fmt.Errorf("harvestGuildData: complete: %w", err)
	}
	log.Printf("harvestGuildData: guild=%s archive=%s bytes=%d key=%s", guildID, archiveID, len(archive), storageKey)
	return nil
}

// guildArchiveRowKey is (subject_type, subject_id, archive_id) for a guild.
func guildArchiveRowKey(guildID, archiveID string) string {
	return stringSeg("guild") + kvSep + bigintSeg(guildID) + kvSep + bigintSeg(archiveID)
}

// collectGuildArchive reads the guild, its satellites and its messages.
func collectGuildArchive(ctx context.Context, guildID string) (map[string]any, error) {
	guild, err := readKV(ctx, guildsTable, bigintSeg(guildID))
	if err != nil {
		return nil, err
	}
	if guild == nil {
		return nil, fmt.Errorf("guild %s does not exist", guildID)
	}
	prefix := bigintSeg(guildID) + kvSep
	roles, err := rowsByPrefix(ctx, "guild_roles", prefix)
	if err != nil {
		return nil, err
	}
	members, err := rowsByPrefix(ctx, "guild_members", prefix)
	if err != nil {
		return nil, err
	}
	channels, err := rowsByPrefix(ctx, "channels_by_guild_id", prefix)
	if err != nil {
		return nil, err
	}
	emojis, err := rowsByPrefix(ctx, "guild_emojis", prefix)
	if err != nil {
		return nil, err
	}
	stickers, err := rowsByPrefix(ctx, "guild_stickers", prefix)
	if err != nil {
		return nil, err
	}

	channelBlocks := make([]map[string]any, 0, len(channels))
	messagesByChannel := make(map[string][]map[string]any)
	for _, link := range channels {
		channelID := typedString(link["channel_id"])
		channel, err := readChannel(ctx, channelID)
		if err != nil || channel == nil {
			continue
		}
		channelBlocks = append(channelBlocks, map[string]any{
			"id":        channelID,
			"name":      typedString(channel["name"]),
			"type":      intOrZero(channel["type"]),
			"parent_id": typedString(channel["parent_id"]),
			"topic":     typedString(channel["topic"]),
			"nsfw":      boolField(channel, "nsfw"),
			"position":  intOrZero(channel["position"]),
		})
		if !guildTextBasedChannel(intOrZero(channel["type"])) {
			continue
		}
		messages, err := guildChannelMessages(ctx, channelID, guildArchiveMessages)
		if err != nil {
			return nil, fmt.Errorf("channel %s messages: %w", channelID, err)
		}
		messagesByChannel[channelID] = messages
	}
	sort.Slice(channelBlocks, func(i, j int) bool {
		return typedString(channelBlocks[i]["id"]) < typedString(channelBlocks[j]["id"])
	})

	guildJSON := map[string]any{
		"schema_version": harvestSchemaVersion,
		"exported_at":    time.Now().UTC().Format(time.RFC3339),
		"guild": map[string]any{
			"id":          guildID,
			"name":        typedString(guild["name"]),
			"owner_id":    typedString(guild["owner_id"]),
			"icon_hash":   typedString(guild["icon_hash"]),
			"banner_hash": typedString(guild["banner_hash"]),
			"description": typedString(guild["description"]),
			"features":    stringSet(guild["features"]),
			"created_at":  snowflakeTimeFromID(guildID),
		},
		"roles":    normalizeRows(roles),
		"members":  normalizeRows(members),
		"channels": channelBlocks,
		"emojis":   normalizeRows(emojis),
		"stickers": normalizeRows(stickers),
	}
	return map[string]any{"guild.json": guildJSON, "messages": messagesByChannel}, nil
}

// guildArchiveEntries lays the archive out the way the api does.
func guildArchiveEntries(data map[string]any) ([]zipEntry, error) {
	guildJSON, _ := data["guild.json"].(map[string]any)
	body, err := json.MarshalIndent(guildJSON, "", "  ")
	if err != nil {
		return nil, err
	}
	entries := []zipEntry{{Name: "guild.json", Data: body}}
	byChannel, _ := data["messages"].(map[string][]map[string]any)
	channelIDs := make([]string, 0, len(byChannel))
	for id := range byChannel {
		channelIDs = append(channelIDs, id)
	}
	sort.Strings(channelIDs)
	for _, channelID := range channelIDs {
		list, err := json.MarshalIndent(byChannel[channelID], "", "  ")
		if err != nil {
			return nil, err
		}
		entries = append(entries, zipEntry{
			Name: fmt.Sprintf("channels/%s/messages.json", channelID),
			Data: list,
		})
	}
	return entries, nil
}

// guildChannelMessages reads up to limit messages, newest first, and reverses them so
// the file reads chronologically.
func guildChannelMessages(ctx context.Context, channelID string, limit int) ([]map[string]any, error) {
	prefix := bigintSeg(channelID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'messages' AND left(row_key, length($1)) = $1
ORDER BY row_key DESC
LIMIT $2`, prefix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		messageID := typedString(row["message_id"])
		entry := map[string]any{
			"id":        messageID,
			"author_id": typedString(row["author_id"]),
			"timestamp": snowflakeTimeFromID(messageID),
			"content":   typedString(row["content"]),
		}
		if attachments := messageAttachments(row); len(attachments) > 0 {
			list := make([]map[string]any, 0, len(attachments))
			for _, a := range attachments {
				list = append(list, map[string]any{
					"attachment_id": a.AttachmentID,
					"filename":      a.Filename,
					"archive_path":  nil, // attachments are not bundled (see OMISSIONS)
				})
			}
			entry["attachments"] = list
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}
	return out, nil
}

// guildTextBasedChannel is the levant set the api harvests: text, voice, announcement.
func guildTextBasedChannel(channelType int) bool {
	return channelType == 0 || channelType == 2 || channelType == 5
}

// rowsByPrefix reads rows whose key starts with a guild prefix.
func rowsByPrefix(ctx context.Context, table, prefix string) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = $1 AND left(row_key, length($2)) = $2
ORDER BY row_key`, table, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// normalizeRows drops the storage-level fields (partition keys, versions) from a row
// so the archive carries data, not the database's own bookkeeping.
func normalizeRows(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		clone := make(map[string]any, len(row))
		for key, value := range row {
			switch key {
			case "version", "partition_key", "row_key", "expires_at":
				continue
			}
			clone[key] = value
		}
		out = append(out, clone)
	}
	return out
}

// writeGuildArchiveProgress patches the admin archive row.
func writeGuildArchiveProgress(ctx context.Context, rowKey string, fields map[string]any) error {
	for field, value := range fields {
		if err := patchRowField(ctx, adminArchivesTable, rowKey, field, value); err != nil {
			return err
		}
	}
	return nil
}

func writeGuildArchiveFailure(ctx context.Context, rowKey string, cause error) {
	if err := writeGuildArchiveProgress(ctx, rowKey, map[string]any{
		"failed_at":     kvDate(time.Now().UTC()),
		"error_message": cause.Error(),
		"progress_step": "failed",
	}); err != nil {
		log.Printf("harvestGuildData: record failure: %v", err)
	}
}
