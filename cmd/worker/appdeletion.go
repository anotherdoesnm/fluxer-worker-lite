package main

// taskApplicationProcessDeletion ports ApplicationProcessDeletion.ts.
//
// Deleting an OAuth2 application also retires its bot: the bot account is
// anonymised in place (the same "Deleted User" the api writes), every guild that
// had it as a member is told with GUILD_MEMBER_UPDATE so the member list stops
// showing a name nobody owns any more, and only then are the application rows
// removed.
//
// The order matters. Removing the rows first would leave a bot account with no
// owner and no record of why it exists; anonymising first means a crash in the
// middle leaves a visible, explainable state.
//
// Not ported: the guild member *removal* that the api's user-facing delete path
// performs. This task is the worker half, where the bot keeps its member rows and
// only loses its identity — the api's own delete does the removing.

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	applicationsTable      = "applications"
	applicationsByOwnTable = "applications_by_owner"
)

func taskApplicationProcessDeletion(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("applicationProcessDeletion: %w", err)
		}
	}
	applicationID := stringField(payload, "applicationId")
	if !isDigits(applicationID) {
		return fmt.Errorf("applicationProcessDeletion: applicationId is required")
	}
	// rebrand() is the identity on this release, so the bot account carries the
	// application's id.
	botUserID := applicationID

	app, err := readKV(ctx, applicationsTable, bigintSeg(applicationID))
	if err != nil {
		return fmt.Errorf("applicationProcessDeletion: read application: %w", err)
	}
	if app == nil {
		log.Printf("applicationProcessDeletion: %s does not exist, nothing to do", applicationID)
		return nil
	}

	bot, err := readKV(ctx, "users", bigintSeg(botUserID))
	if err != nil {
		return fmt.Errorf("applicationProcessDeletion: read bot user: %w", err)
	}
	if bot == nil {
		// An application without its bot row is odd but not ours to repair: remove
		// the rows and say so.
		log.Printf("applicationProcessDeletion: %s has no bot user row", applicationID)
		return deleteApplicationRows(ctx, app, applicationID)
	}
	if userFlags(bot)&userFlagDeleted != 0 {
		log.Printf("applicationProcessDeletion: bot %s is already deleted, dropping the application", botUserID)
		return deleteApplicationRows(ctx, app, applicationID)
	}

	anonymised, err := anonymiseBotUser(ctx, botUserID)
	if err != nil {
		return fmt.Errorf("applicationProcessDeletion: %w", err)
	}
	if err := notifyBotGuilds(ctx, botUserID, anonymised); err != nil {
		return fmt.Errorf("applicationProcessDeletion: %w", err)
	}
	return deleteApplicationRows(ctx, app, applicationID)
}

// anonymiseBotUser applies the same identity the api's deleted-user placeholder
// carries, and marks the account DELETED so nothing re-creates a fresh name.
func anonymiseBotUser(ctx context.Context, botUserID string) (map[string]any, error) {
	rowKey := bigintSeg(botUserID)
	flags := userFlags(mustReadUser(ctx, botUserID)) | userFlagDeleted
	patch := map[string]any{
		"username":      map[string]any{"__fluxer_type": "string", "value": "DeletedUser"},
		"global_name":   map[string]any{"__fluxer_type": "string", "value": "Deleted User"},
		"discriminator": 0,
		"avatar_hash":   nil,
		"banner_hash":   nil,
		"bio":           nil,
		"pronouns":      nil,
		"accent_color":  nil,
		"flags":         map[string]any{"__fluxer_type": "bigint", "value": fmt.Sprintf("%d", flags)},
	}
	for field, value := range patch {
		if err := patchRowField(ctx, "users", rowKey, field, value); err != nil {
			return nil, fmt.Errorf("patch bot %s: %w", field, err)
		}
	}
	updated, err := readKV(ctx, "users", rowKey)
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// mustReadUser is a small helper: the caller already knows the row exists, and a
// second read failure is worth a panic-free zero value rather than a crash.
func mustReadUser(ctx context.Context, userID string) map[string]any {
	row, err := readKV(ctx, "users", bigintSeg(userID))
	if err != nil || row == nil {
		return map[string]any{}
	}
	return row
}

// notifyBotGuilds sends GUILD_MEMBER_UPDATE to every guild the bot is still in,
// so open clients replace the name and avatar without a reconnect.
func notifyBotGuilds(ctx context.Context, botUserID string, bot map[string]any) error {
	nc := workerNATS()
	if nc == nil {
		return nil
	}
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'guild_id'->>'value' FROM fluxer_kv
WHERE table_name = 'guild_members_by_user_id' AND left(row_key, length($1)) = $1`,
		bigintSeg(botUserID)+kvSep)
	if err != nil {
		return fmt.Errorf("list bot guilds: %w", err)
	}
	var guildIDs []string
	for rows.Next() {
		var guildID *string
		if err := rows.Scan(&guildID); err != nil {
			rows.Close()
			return err
		}
		if guildID != nil && isDigits(*guildID) {
			guildIDs = append(guildIDs, *guildID)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	now := time.Now().UTC()
	var failed int
	for _, guildID := range guildIDs {
		member, err := readKV(ctx, "guild_members", bigintSeg(guildID)+kvSep+bigintSeg(botUserID))
		if err != nil {
			failed++
			log.Printf("applicationProcessDeletion: guild %s: read member: %v", guildID, err)
			continue
		}
		if member == nil {
			continue
		}
		response, ok := guildMemberResponse(member, bot, now)
		if !ok {
			// The bot is anonymised, not hidden, so this is not expected; skip and
			// keep going rather than failing the whole deletion.
			log.Printf("applicationProcessDeletion: guild %s: member not renderable, skipped", guildID)
			continue
		}
		data := map[string]any{"guild_id": guildID}
		for k, v := range response {
			data[k] = v
		}
		if err := dispatchGuild(nc, guildID, "GUILD_MEMBER_UPDATE", data); err != nil {
			failed++
			log.Printf("applicationProcessDeletion: guild %s: dispatch: %v", guildID, err)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d guild updates failed", failed)
	}
	log.Printf("applicationProcessDeletion: bot %s anonymised in %d guilds", botUserID, len(guildIDs))
	return nil
}

// deleteApplicationRows removes the application and its owner index entry.
func deleteApplicationRows(ctx context.Context, app map[string]any, applicationID string) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		applicationsTable, bigintSeg(applicationID)); err != nil {
		return fmt.Errorf("delete application: %w", err)
	}
	ownerID := typedString(app["owner_id"])
	if ownerID != "" {
		if _, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
			applicationsByOwnTable, bigintSeg(ownerID)+kvSep+bigintSeg(applicationID)); err != nil {
			return fmt.Errorf("delete owner index: %w", err)
		}
	}
	return nil
}
