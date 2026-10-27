package main

// taskBulkDeleteSelfMessagesImmediate ports BulkDeleteSelfMessagesImmediate.ts.
//
// This is the "delete my messages now, with filters" action, as opposed to the
// scheduled one (bulkDeleteUserMessages) which deletes everything up to a moment.
// The filters are: a scope ("selected" or "inaccessible_only"), four include
// toggles (DMs, closed DMs, group DMs, guilds), a guild allow/deny list, and a
// timestamp window.
//
// The work is the same as everywhere else in this file's neighbours — walk the
// author index, decide per channel whether the message is in scope, delete through
// the shared bundle, tell the channel's clients. What is *not* ported is the
// completion notice: upstream sends a system DM (or an email when system DMs are
// hidden) built from a localised string. There is no localisation in this worker,
// and inventing English text where the api would send the person's language is
// worse than sending nothing, so the notice is a documented gap.

import (
	"context"
	"fmt"
	"log"
	"strconv"
)

const (
	channelTypeGuildText      = 0
	channelTypeDM             = 1
	channelTypeGroupDM        = 3
	channelTypeDMPersonalNote = 999
)

type selfMessageFilter struct {
	Scope            string
	IncludeDMs       bool
	IncludeDMsClosed bool
	IncludeGroupDMs  bool
	IncludeGuilds    bool
	GuildFilterMode  string
	ExcludedGuildIDs []string
	IncludedGuildIDs []string
	StartTimestamp   int64 // 0 means "no lower bound"
	EndTimestamp     int64 // 0 means "no upper bound"
}

type selfMessageContext struct {
	openDMChannels map[string]struct{}
	currentGuilds  map[string]struct{}
}

func taskBulkDeleteSelfMessagesImmediate(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("bulkDeleteSelfMessagesImmediate: %w", err)
		}
	}
	userID := stringField(payload, "userId")
	if !isDigits(userID) {
		return fmt.Errorf("bulkDeleteSelfMessagesImmediate: userId is required")
	}
	filter, err := decodeSelfMessageFilter(payload)
	if err != nil {
		return fmt.Errorf("bulkDeleteSelfMessagesImmediate: %w", err)
	}
	row, err := readKV(ctx, "users", bigintSeg(userID))
	if err != nil {
		return fmt.Errorf("bulkDeleteSelfMessagesImmediate: read user: %w", err)
	}
	if row == nil {
		log.Printf("bulkDeleteSelfMessagesImmediate: user %s no longer exists, nothing to do", userID)
		return nil
	}
	eligibility, err := loadSelfMessageContext(ctx, userID)
	if err != nil {
		return fmt.Errorf("bulkDeleteSelfMessagesImmediate: eligibility: %w", err)
	}

	deleted, channels, err := deleteSelfMessagesFiltered(ctx, userID, filter, eligibility)
	if err != nil {
		return fmt.Errorf("bulkDeleteSelfMessagesImmediate: %w", err)
	}
	log.Printf("bulkDeleteSelfMessagesImmediate: user=%s deleted=%d channels=%d", userID, deleted, channels)
	return nil
}

func decodeSelfMessageFilter(payload map[string]any) (selfMessageFilter, error) {
	raw, ok := payload["filter"].(map[string]any)
	if !ok {
		return selfMessageFilter{}, fmt.Errorf("filter is required")
	}
	f := selfMessageFilter{
		Scope:            stringField(raw, "scope"),
		GuildFilterMode:  stringField(raw, "guildFilterMode"),
		ExcludedGuildIDs: stringSliceField(raw, "excludedGuildIds"),
		IncludedGuildIDs: stringSliceField(raw, "includedGuildIds"),
	}
	if f.Scope == "" {
		f.Scope = "selected"
	}
	if f.GuildFilterMode == "" {
		f.GuildFilterMode = "exclude"
	}
	switch f.Scope {
	case "selected", "inaccessible_only":
	default:
		return f, fmt.Errorf("unknown scope %q", f.Scope)
	}
	f.IncludeDMs = boolField(raw, "includeDms")
	f.IncludeDMsClosed = boolField(raw, "includeDmsClosed")
	f.IncludeGroupDMs = boolField(raw, "includeGroupDms")
	f.IncludeGuilds = boolField(raw, "includeGuilds")
	f.StartTimestamp = int64OrZero(raw, "startTimestamp")
	f.EndTimestamp = int64OrZero(raw, "endTimestamp")
	return f, nil
}

// loadSelfMessageContext reads the two sets eligibility needs: which DM channels
// the person still has open, and which guilds they are still a member of. Both
// come from the same tables the api reads.
func loadSelfMessageContext(ctx context.Context, userID string) (selfMessageContext, error) {
	out := selfMessageContext{
		openDMChannels: map[string]struct{}{},
		currentGuilds:  map[string]struct{}{},
	}
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'channel_id'->>'value' FROM fluxer_kv
WHERE table_name = 'private_channels' AND row_data->'user_id'->>'value' = $1`, userID)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var channelID *string
		if err := rows.Scan(&channelID); err != nil {
			rows.Close()
			return out, err
		}
		if channelID != nil && *channelID != "" {
			out.openDMChannels[*channelID] = struct{}{}
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}

	guildRows, err := db.QueryContext(ctx, `
SELECT row_data->'guild_id'->>'value' FROM fluxer_kv
WHERE table_name = 'guild_members_by_user_id' AND row_data->'user_id'->>'value' = $1`, userID)
	if err != nil {
		return out, err
	}
	defer guildRows.Close()
	for guildRows.Next() {
		var guildID *string
		if err := guildRows.Scan(&guildID); err != nil {
			return out, err
		}
		if guildID != nil && *guildID != "" {
			out.currentGuilds[*guildID] = struct{}{}
		}
	}
	return out, guildRows.Err()
}

// deleteSelfMessagesFiltered walks the author index, applies the filter per
// channel, and deletes what survives. Channels that end up empty of matches are
// never touched.
func deleteSelfMessagesFiltered(ctx context.Context, userID string, filter selfMessageFilter, eligibility selfMessageContext) (deleted, channels int, err error) {
	refs, err := listMessagesByAuthor(ctx, userID)
	if err != nil {
		return 0, 0, err
	}
	type target struct {
		messageID string
	}
	byChannel := map[string][]target{}
	channelEligible := map[string]bool{}
	for _, ref := range refs {
		id, perr := strconv.ParseInt(ref.MessageID, 10, 64)
		if perr != nil {
			continue
		}
		if !timestampInWindow(snowflakeTime(id).UnixMilli(), filter) {
			continue
		}
		eligible, known := channelEligible[ref.ChannelID]
		if !known {
			channel, cerr := readChannel(ctx, ref.ChannelID)
			if cerr != nil {
				return deleted, channels, cerr
			}
			eligible = channel != nil && channelEligibleFor(channel, userID, filter, eligibility)
			channelEligible[ref.ChannelID] = eligible
		}
		if !eligible {
			continue
		}
		byChannel[ref.ChannelID] = append(byChannel[ref.ChannelID], target{messageID: ref.MessageID})
	}

	for channelID, targets := range byChannel {
		gone := make([]string, 0, len(targets))
		for _, t := range targets {
			existed, derr := deleteMessageRow(ctx, channelID, t.messageID)
			if derr != nil {
				return deleted, channels, derr
			}
			if existed {
				deleted++
				gone = append(gone, t.messageID)
			}
		}
		if len(gone) > 0 {
			channels++
			dispatchBulkDelete(ctx, channelID, gone)
		}
	}
	return deleted, channels, nil
}

// channelEligibleFor is the port of isChannelEligible: the same rules, including
// the one that looks odd until you read the scope description — in
// "inaccessible_only" mode the guild must *not* still be joined.
func channelEligibleFor(channel map[string]any, userID string, filter selfMessageFilter, eligibility selfMessageContext) bool {
	channelType := intOrZero(channel["type"])
	channelID := typedString(channel["channel_id"])
	switch channelType {
	case channelTypeDMPersonalNote:
		return false
	case channelTypeDM:
		if filter.Scope == "inaccessible_only" {
			return false
		}
		if _, open := eligibility.openDMChannels[channelID]; open {
			return filter.IncludeDMs
		}
		return filter.IncludeDMsClosed
	case channelTypeGroupDM:
		stillMember := channelHasRecipient(channel, userID)
		if filter.Scope == "inaccessible_only" {
			return !stillMember
		}
		return filter.IncludeGroupDMs && stillMember
	}
	guildID := typedString(channel["guild_id"])
	if guildID == "" {
		return false
	}
	_, isCurrentMember := eligibility.currentGuilds[guildID]
	if filter.Scope == "inaccessible_only" {
		return !isCurrentMember
	}
	if !filter.IncludeGuilds || !isCurrentMember {
		return false
	}
	if filter.GuildFilterMode == "include_only" {
		return containsString(filter.IncludedGuildIDs, guildID)
	}
	return !containsString(filter.ExcludedGuildIDs, guildID)
}

// channelHasRecipient reads the recipient list of a group DM.
func channelHasRecipient(channel map[string]any, userID string) bool {
	raw, ok := channel["recipient_ids"]
	if !ok {
		return false
	}
	wrap, ok := raw.(map[string]any)
	if ok {
		if list, ok := wrap["value"].([]any); ok {
			raw = list
		}
	}
	list, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, item := range list {
		if typedString(item) == userID {
			return true
		}
	}
	return false
}

func timestampInWindow(ms int64, filter selfMessageFilter) bool {
	if filter.StartTimestamp > 0 && ms < filter.StartTimestamp {
		return false
	}
	if filter.EndTimestamp > 0 && ms >= filter.EndTimestamp {
		return false
	}
	return true
}

func boolField(m map[string]any, key string) bool {
	b, _ := m[key].(bool)
	return b
}

func int64OrZero(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return 0
}
