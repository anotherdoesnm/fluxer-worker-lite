package main

// The copy half of crossposting: write the copy into the follower's channel, keep
// the source→copy link, and tell that channel's clients.
//
// A crosspost copy is a normal message row with three differences: the authoring
// webhook is recorded on the message, the content carries a reference back to the
// source (guild, channel, message), and a row in `crossposted_messages` ties the
// pair together so edits and deletes can find the copy later. The copy keeps the
// source's content, flags, embeds and attachments: attachments deliberately still
// point at the source channel, which is where the bytes live.
//
// Not ported here, and named in OMISSIONS.md: the permission check on the follower's
// creator, the content-rule check between source and target, the avatar the api
// regenerates from the source guild's icon, and the pending/delivered state machine
// with its reclaim of stale reservations. What is written is the pair of rows the
// state machine needs; the checks are the part that would reject a delivery before
// any rows exist.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// crosspostCopySpec is everything the writer needs; the source row is passed whole so
// fields the api adds later are copied without a schema change here.
type crosspostCopySpec struct {
	SourceGuildID   string
	SourceChannelID string
	SourceMessageID string
	TargetGuildID   string
	TargetChannelID string
	WebhookID       string
	Source          map[string]any
	Fingerprint     string
}

// insertCrosspostCopy writes the copy row, its indexes, the crosspost link and
// returns the new message id.
func insertCrosspostCopy(ctx context.Context, spec crosspostCopySpec) (string, error) {
	ids, err := snowflakeBatch(workerNATS(), 1)
	if err != nil {
		return "", fmt.Errorf("snowflake: %w", err)
	}
	messageID := ids[0]
	bucket, err := bucketFor(messageID)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()

	row := map[string]any{
		"message_id":       bigintValue(messageID),
		"channel_id":       bigintValue(spec.TargetChannelID),
		"author_id":        bigintValue(spec.WebhookID),
		"webhook_id":       bigintValue(spec.WebhookID),
		"content":          spec.Source["content"],
		"type":             messageTypeUser,
		"flags":            copyInt(spec.Source["flags"]),
		"version":          1,
		"bucket":           bucket,
		"has_reaction":     false,
		"mention_everyone": false,
		"message_reference": map[string]any{
			"guild_id":   nullableString(spec.SourceGuildID),
			"channel_id": spec.SourceChannelID,
			"message_id": spec.SourceMessageID,
			"type":       0,
		},
		"crosspost_source": map[string]any{
			"guild_id":   nullableString(spec.SourceGuildID),
			"channel_id": spec.SourceChannelID,
			"message_id": spec.SourceMessageID,
		},
	}
	// Copy the optional collections verbatim: the api's copy carries the same embeds
	// and attachments, and inventing a normalised shape here would be a second
	// implementation of a message writer.
	for _, field := range []string{"embeds", "attachments", "sticker_items"} {
		if value, ok := spec.Source[field]; ok && value != nil {
			row[field] = value
		}
	}

	partition := bigintSeg(spec.TargetChannelID) + kvSep + strconv.FormatInt(bucket, 10)
	key := partition + kvSep + bigintSeg(messageID)
	if err := putKV(ctx, messagesTable, partition, key, row, messageID); err != nil {
		return "", fmt.Errorf("write message: %w", err)
	}
	bucketKey := bigintSeg(spec.TargetChannelID) + kvSep + strconv.FormatInt(bucket, 10)
	if err := putKV(ctx, "channel_message_buckets", bucketKey, bucketKey, map[string]any{
		"channel_id": bigintValue(spec.TargetChannelID),
		"bucket":     bucket,
		"updated_at": dateValue(now),
	}, strconv.FormatInt(bucket, 10)); err != nil {
		return "", fmt.Errorf("write bucket: %w", err)
	}
	authorKey := bigintSeg(spec.WebhookID) + kvSep + bigintSeg(messageID)
	if err := putKV(ctx, messagesByAuthor, authorKey, authorKey, map[string]any{
		"author_id":  bigintValue(spec.WebhookID),
		"channel_id": bigintValue(spec.TargetChannelID),
		"message_id": bigintValue(messageID),
	}, messageID); err != nil {
		return "", fmt.Errorf("write author index: %w", err)
	}
	stateKey := bigintSeg(spec.TargetChannelID)
	state, err := readKV(ctx, "channel_state", stateKey)
	if err != nil {
		return "", err
	}
	stateRow := map[string]any{
		"channel_id":          bigintValue(spec.TargetChannelID),
		"updated_at":          dateValue(now),
		"has_messages":        true,
		"last_message_id":     bigintValue(messageID),
		"last_message_bucket": bucket,
	}
	if state == nil {
		stateRow["created_bucket"] = bucket
	}
	if err := putKV(ctx, "channel_state", stateKey, stateKey, stateRow, messageID); err != nil {
		return "", fmt.Errorf("write channel state: %w", err)
	}

	// The link row: state delivered, with the fingerprint that later syncs compare.
	linkPartition, linkKey := crosspostLinkKeys(spec.SourceMessageID, spec.WebhookID)
	if err := putKV(ctx, "crossposted_messages", linkPartition, linkKey, map[string]any{
		"source_message_id":  bigintValue(spec.SourceMessageID),
		"webhook_id":         bigintValue(spec.WebhookID),
		"source_channel_id":  bigintValue(spec.SourceChannelID),
		"target_guild_id":    bigintValue(spec.TargetGuildID),
		"target_channel_id":  bigintValue(spec.TargetChannelID),
		"target_message_id":  bigintValue(messageID),
		"state":              "delivered",
		"source_fingerprint": spec.Fingerprint,
		"reserved_at":        dateValue(now),
		"created_at":         dateValue(now),
	}, ""); err != nil {
		return "", fmt.Errorf("write crosspost link: %w", err)
	}
	return messageID, nil
}

// crosspostLinkKeys addresses crossposted_messages: (source_message_id, webhook_id).
func crosspostLinkKeys(sourceMessageID, webhookID string) (partition, rowKey string) {
	key := bigintSeg(sourceMessageID) + kvSep + bigintSeg(webhookID)
	return key, key
}

// readCrosspostLink loads the link row, or nil when the pair was never delivered.
func readCrosspostLink(ctx context.Context, sourceMessageID, webhookID string) (map[string]any, error) {
	partition, key := crosspostLinkKeys(sourceMessageID, webhookID)
	_ = partition
	return readKV(ctx, "crossposted_messages", key)
}

// messageFingerprint hashes the parts of a message a copy must follow: content,
// flags, embeds, attachments and stickers. The api hashes the same set, which is how
// a sync decides whether a copy is stale.
func messageFingerprint(row map[string]any) string {
	state := map[string]any{}
	for _, field := range []string{"content", "flags", "embeds", "attachments", "sticker_items"} {
		if value, ok := row[field]; ok {
			state[field] = value
		}
	}
	body, err := json.Marshal(state)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// copyInt reads an integer field that may be wrapped as a typed value or plain.
func copyInt(v any) int {
	if n, ok := intFieldChecked(map[string]any{"v": v}, "v"); ok {
		return n
	}
	return 0
}
