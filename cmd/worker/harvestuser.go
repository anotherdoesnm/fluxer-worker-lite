package main

// taskHarvestUserData ports HarvestUserData.ts.
//
// A person asks for a copy of their data; the api answers with a zip and a
// time-boxed link. The archive this worker builds carries the same files the api
// writes — `user.json`, `channels/<channel>/messages.json`, `account/security.json`,
// `integrations/oauth.json` — so a reader gets the same shape either way.
//
// The row in `user_harvests` is the contract with the ui: started, then completed
// with the storage key, the size and the link's expiry, or failed with a reason. A
// task that uploaded an archive but never wrote the row would leave a file nobody
// can find; a task that wrote completed without an upload would hand out a dead link.
//
// Not ported, named in OMISSIONS.md: the optional attachment walk (it needs the
// storage read path and a content-addressed manifest), the payments and operator-pass
// sections (nothing to export on this instance), and the completion notice (no email
// path; the ui reads the row instead).

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
	userHarvestsTable    = "user_harvests"
	harvestLinkTTL       = 7 * 24 * time.Hour
	harvestSchemaVersion = 1
)

func taskHarvestUserData(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("harvestUserData: %w", err)
		}
	}
	userID := stringField(payload, "userId")
	harvestID := stringField(payload, "harvestId")
	if !isDigits(userID) || !isDigits(harvestID) {
		return fmt.Errorf("harvestUserData: userId and harvestId are required")
	}
	rowKey := bigintSeg(userID) + kvSep + bigintSeg(harvestID)
	started := time.Now().UTC()
	if err := writeHarvestProgress(ctx, rowKey, userID, harvestID, map[string]any{
		"started_at":         kvDate(started),
		"progress_percent":   5,
		"progress_step":      "collecting",
		"failed_at":          nil,
		"terminal_failed_at": nil,
		"error_message":      nil,
	}); err != nil {
		return fmt.Errorf("harvestUserData: start: %w", err)
	}

	data, err := collectUserHarvest(ctx, userID)
	if err != nil {
		writeHarvestFailure(ctx, rowKey, err)
		return fmt.Errorf("harvestUserData: collect: %w", err)
	}
	if err := writeHarvestProgress(ctx, rowKey, userID, harvestID, map[string]any{"progress_percent": 60, "progress_step": "packing"}); err != nil {
		return fmt.Errorf("harvestUserData: progress: %w", err)
	}

	entries, err := userHarvestEntries(data)
	if err != nil {
		writeHarvestFailure(ctx, rowKey, err)
		return fmt.Errorf("harvestUserData: entries: %w", err)
	}
	archive, err := buildZip(entries)
	if err != nil {
		writeHarvestFailure(ctx, rowKey, err)
		return fmt.Errorf("harvestUserData: zip: %w", err)
	}

	attemptID, err := randomToken()
	if err != nil {
		return fmt.Errorf("harvestUserData: attempt id: %w", err)
	}
	storageKey := fmt.Sprintf("exports/%s/%s/%s/user-data.zip", userID, harvestID, attemptID)
	endpoint, _, accessKey, secretKey, region := s3Config()
	bucket := harvestBucket()
	if err := s3PutTo(ctx, endpoint, bucket, accessKey, secretKey, region, storageKey, archive, "application/zip"); err != nil {
		writeHarvestFailure(ctx, rowKey, err)
		return fmt.Errorf("harvestUserData: upload: %w", err)
	}

	expiresAt := time.Now().UTC().Add(harvestLinkTTL)
	if err := writeHarvestProgress(ctx, rowKey, userID, harvestID, map[string]any{
		"completed_at":            kvDate(time.Now().UTC()),
		"storage_key":             storageKey,
		"file_size":               bigintValue(strconv.Itoa(len(archive))),
		"progress_percent":        100,
		"progress_step":           "completed",
		"download_url_expires_at": kvDate(expiresAt),
		"attempt_id":              attemptID,
	}); err != nil {
		return fmt.Errorf("harvestUserData: complete: %w", err)
	}
	log.Printf("harvestUserData: user=%s harvest=%s bytes=%d key=%s", userID, harvestID, len(archive), storageKey)
	return nil
}

// collectUserHarvest reads the sections the archive carries.
func collectUserHarvest(ctx context.Context, userID string) (map[string]any, error) {
	user, err := readKV(ctx, "users", bigintSeg(userID))
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("user %s does not exist", userID)
	}
	out := map[string]any{
		"schema_version": harvestSchemaVersion,
		"exported_at":    time.Now().UTC().Format(time.RFC3339),
		"user":           harvestUserSection(userID, user),
	}
	sections := map[string]string{
		"auth_sessions":    "auth_sessions",
		"relationships":    "relationships",
		"notes":            "notes",
		"private_channels": "private_channels",
		"pinned_dms":       "pinned_dms",
		"saved_messages":   "saved_messages",
	}
	for field, table := range sections {
		rows, err := rowsOfUser(ctx, table, userID)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
		out[field] = rows
	}
	memberships, err := harvestGuildMemberships(ctx, userID)
	if err != nil {
		return nil, err
	}
	out["guild_memberships"] = memberships
	oauth, err := rowsOfOwner(ctx, "applications_by_owner", userID)
	if err != nil {
		return nil, err
	}
	out["oauth_applications"] = oauth
	// The security section is the profile of the account's own access: authorized IPs
	// and the activity stamps the api also exports.
	out["security"] = map[string]any{
		"last_active_at": typedString(user["last_active_at"]),
		"last_active_ip": typedString(user["last_active_ip"]),
	}
	messages, err := harvestAuthoredMessages(ctx, userID)
	if err != nil {
		return nil, err
	}
	out["messages_by_channel"] = messages
	return out, nil
}

// harvestUserSection is the `user` block of user.json.
func harvestUserSection(userID string, row map[string]any) map[string]any {
	created := ""
	if id, err := parseInt64(userID); err == nil {
		created = snowflakeTime(id).UTC().Format(time.RFC3339)
	}
	return map[string]any{
		"id":                  userID,
		"username":            typedString(row["username"]),
		"global_name":         typedString(row["global_name"]),
		"discriminator":       intOrZero(row["discriminator"]),
		"bot":                 boolField(row, "bot"),
		"system":              boolField(row, "system"),
		"email":               typedString(row["email"]),
		"email_verified":      boolField(row, "email_verified"),
		"email_bounced":       boolField(row, "email_bounced"),
		"avatar_hash":         typedString(row["avatar_hash"]),
		"banner_hash":         typedString(row["banner_hash"]),
		"bio":                 typedString(row["bio"]),
		"pronouns":            typedString(row["pronouns"]),
		"accent_color":        nullableInt(row["accent_color"]),
		"locale":              typedString(row["locale"]),
		"flags":               strconv.FormatUint(userFlags(row), 10),
		"premium_type":        intOrZero(row["premium_type"]),
		"premium_since":       typedString(row["premium_since"]),
		"premium_until":       typedString(row["premium_until"]),
		"terms_agreed_at":     typedString(row["terms_agreed_at"]),
		"privacy_agreed_at":   typedString(row["privacy_agreed_at"]),
		"last_active_at":      typedString(row["last_active_at"]),
		"created_at":          created,
		"mfa_enabled":         len(stringSet(row["authenticator_types"])) > 0,
		"authenticator_types": stringSet(row["authenticator_types"]),
		"traits":              stringSet(row["traits"]),
	}
}

// userHarvestEntries lays the sections out as the api's file names.
func userHarvestEntries(data map[string]any) ([]zipEntry, error) {
	userJSON := map[string]any{}
	for key, value := range data {
		if key == "schema_version" || key == "exported_at" {
			continue
		}
		userJSON[key] = value
	}
	body, err := json.MarshalIndent(userJSON, "", "  ")
	if err != nil {
		return nil, err
	}
	security, err := json.MarshalIndent(map[string]any{
		"security":         data["security"],
		"auth_sessions":    data["auth_sessions"],
		"relationships":    data["relationships"],
		"notes":            data["notes"],
		"private_channels": data["private_channels"],
	}, "", "  ")
	if err != nil {
		return nil, err
	}
	oauth, err := json.MarshalIndent(map[string]any{"applications": data["oauth_applications"]}, "", "  ")
	if err != nil {
		return nil, err
	}
	entries := []zipEntry{
		{Name: "user.json", Data: body},
		{Name: "account/security.json", Data: security},
		{Name: "integrations/oauth.json", Data: oauth},
	}
	// One file per channel, named as the api names it: people open these archives and
	// look for their messages first.
	if byChannel, ok := data["messages_by_channel"].(map[string][]map[string]any); ok {
		channels := make([]string, 0, len(byChannel))
		for channelID := range byChannel {
			channels = append(channels, channelID)
		}
		sort.Strings(channels)
		for _, channelID := range channels {
			list, err := json.MarshalIndent(byChannel[channelID], "", "  ")
			if err != nil {
				return nil, err
			}
			entries = append(entries, zipEntry{
				Name: fmt.Sprintf("channels/%s/messages.json", channelID),
				Data: list,
			})
		}
	}
	return entries, nil
}

// rowsOfUser reads rows whose document carries this user id.
func rowsOfUser(ctx context.Context, table, userID string) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = $1 AND row_data->'user_id'->>'value' = $2`, table, userID)
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

// rowsOfOwner reads rows whose document carries this user as owner/creator.
func rowsOfOwner(ctx context.Context, table, userID string) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = $1 AND row_data->'owner_user_id'->>'value' = $2`, table, userID)
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

// harvestAuthoredMessages groups the person's messages by channel, oldest first. The
// author index is the only place that knows all of them, which is why it is walked
// rather than every channel's history.
func harvestAuthoredMessages(ctx context.Context, userID string) (map[string][]map[string]any, error) {
	refs, err := listMessagesByAuthor(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]map[string]any)
	for _, ref := range refs {
		row, err := readMessage(ctx, ref.ChannelID, ref.MessageID)
		if err != nil {
			return nil, err
		}
		if row == nil {
			continue
		}
		entry := map[string]any{
			"id":          ref.MessageID,
			"channel_id":  ref.ChannelID,
			"content":     typedString(row["content"]),
			"type":        intOrZero(row["type"]),
			"flags":       intOrZero(row["flags"]),
			"created_at":  snowflakeTimeFromID(ref.MessageID),
			"attachments": len(messageAttachments(row)),
			"embeds":      countJSONList(row["embeds"]),
		}
		out[ref.ChannelID] = append(out[ref.ChannelID], entry)
	}
	for channelID := range out {
		list := out[channelID]
		sort.Slice(list, func(i, j int) bool { return typedStringFromAny(list[i]["id"]) < typedStringFromAny(list[j]["id"]) })
		out[channelID] = list
	}
	return out, nil
}

// snowflakeTimeFromID renders a message id as an ISO timestamp.
func snowflakeTimeFromID(messageID string) string {
	id, err := parseInt64(messageID)
	if err != nil {
		return ""
	}
	return snowflakeTime(id).UTC().Format(time.RFC3339)
}

func countJSONList(v any) int {
	list, ok := v.([]any)
	if !ok {
		return 0
	}
	return len(list)
}

func typedStringFromAny(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func harvestGuildMemberships(ctx context.Context, userID string) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'guild_members_by_user_id' AND row_data->'user_id'->>'value' = $1`, userID)
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
		guildID := typedString(row["guild_id"])
		entry := map[string]any{"guild_id": guildID}
		if guild, err := readKV(ctx, guildsTable, bigintSeg(guildID)); err == nil && guild != nil {
			entry["guild_name"] = typedString(guild["name"])
		}
		if member, err := readKV(ctx, "guild_members", bigintSeg(guildID)+kvSep+bigintSeg(userID)); err == nil && member != nil {
			entry["joined_at"] = typedString(member["joined_at"])
			entry["nick"] = typedString(member["nick"])
			entry["role_ids"] = stringSet(member["role_ids"])
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

// writeHarvestProgress patches fields on the harvest row, creating it when absent.
func writeHarvestProgress(ctx context.Context, rowKey, userID, harvestID string, fields map[string]any) error {
	existing, err := readKV(ctx, userHarvestsTable, rowKey)
	if err != nil {
		return err
	}
	if existing == nil {
		base := map[string]any{
			"user_id":      bigintValue(userID),
			"harvest_id":   bigintValue(harvestID),
			"requested_at": kvDate(time.Now().UTC()),
		}
		if err := putKV(ctx, userHarvestsTable, rowKey, rowKey, base, ""); err != nil {
			return err
		}
	}
	for field, value := range fields {
		if err := patchRowField(ctx, userHarvestsTable, rowKey, field, value); err != nil {
			return err
		}
	}
	return nil
}

func writeHarvestFailure(ctx context.Context, rowKey string, cause error) {
	fields := map[string]any{
		"failed_at":     kvDate(time.Now().UTC()),
		"error_message": cause.Error(),
		"progress_step": "failed",
	}
	if err := writeHarvestProgress(ctx, rowKey, "", "", fields); err != nil {
		log.Printf("harvestUserData: record failure: %v", err)
	}
}
