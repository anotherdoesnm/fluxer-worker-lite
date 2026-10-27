package main

// Deleting a message, and the first task built on top of it: messageShred.
//
// The api's MessageRepository.deleteMessage does more than remove one row: the
// message also lives in messages_by_author_id_v2, may be pinned in channel_pins,
// owns its reactions and its attachment_lookup entries, and the bucket counters
// (channel_message_buckets / channel_empty_buckets) have to stay honest or later
// scans walk empty buckets forever. This file ports that bundle once, because
// six of the parked tasks (messageShred, the bulk deletes, guild deletion,
// inactivity deletions) are all "delete a message" with different payloads.
//
// Keys are built the way upstream builds them: primary-key columns in order,
// joined by kvSep, bigint columns wrapped as {"__fluxer_type":"bigint",…}. Where
// a column's encoding is not ours to know (reaction emoji ids, pin timestamps)
// the row is matched by prefix or suffix of the part we do know, which is exact:
// a segment is a whole JSON scalar, not a substring that can appear inside
// another one.
//
// Deliberate gaps, both because this instance cannot express them:
//   - search documents (no search backend configured, so nothing is indexed);
//   - crosspost copies (crosspost only exists between guilds that follow a
//     channel, and this instance is a single community).

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	messagesTable        = "messages"
	messagesByAuthor     = "messages_by_author_id_v2"
	channelPinsTable     = "channel_pins"
	reactionsTable       = "message_reactions"
	attachmentLookupTbl  = "attachment_lookup"
	channelBucketsTable  = "channel_message_buckets"
	emptyBucketsTable    = "channel_empty_buckets"
	attachmentDecayByID  = "attachment_decay_by_id"
	attachmentDecayByExp = "attachment_decay_by_expiry"
	messageShredStatus   = "message_shred_status:"
	messageShredTTL      = time.Hour
	shredInputSlice      = 500
	shredValidationChunk = 25
	shredDeletionChunk   = 10
)

// messageAttachment is the part of an attachment object this worker needs.
type messageAttachment struct {
	AttachmentID string
	Filename     string
}

// messageAttachments reads the attachments array out of a message row.
func messageAttachments(row map[string]any) []messageAttachment {
	raw, ok := row["attachments"].([]any)
	if !ok {
		return nil
	}
	out := make([]messageAttachment, 0, len(raw))
	for _, item := range raw {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id := typedString(obj["attachment_id"])
		name := typedString(obj["filename"])
		if id == "" || name == "" {
			continue
		}
		out = append(out, messageAttachment{AttachmentID: id, Filename: name})
	}
	return out
}

// deleteMessageRow removes a message and its satellites. It reports whether the
// message row existed, which is what callers use to distinguish "deleted" from
// "already gone" in their counters.
func deleteMessageRow(ctx context.Context, channelID, messageID string) (bool, error) {
	row, err := readMessage(ctx, channelID, messageID)
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, nil
	}
	partition, rowKey, bucket := messageKeys(channelID, messageID)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, messagesTable, rowKey); err != nil {
		return false, err
	}

	// messages_by_author_id_v2: (author_id, message_id), so the row key ends with
	// the message segment.
	if author := typedString(row["author_id"]); author != "" {
		authorKey := bigintSeg(author) + kvSep + bigintSeg(messageID)
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, messagesByAuthor, authorKey); err != nil {
			return false, err
		}
	}

	// channel_pins: (channel_id, pinned_timestamp, message_id) — pinned_timestamp
	// sits in the middle and its encoding is upstream's business, so match on the
	// prefix we know and the suffix we know.
	pinPrefix := bigintSeg(channelID) + kvSep
	pinSuffix := kvSep + bigintSeg(messageID)
	if _, err := tx.ExecContext(ctx, `
DELETE FROM fluxer_kv
WHERE table_name = $1 AND left(row_key, length($2)) = $2 AND right(row_key, length($3)) = $3`,
		channelPinsTable, pinPrefix, pinSuffix); err != nil {
		return false, err
	}

	// message_reactions: (channel_id, bucket, message_id, emoji_id, emoji_name,
	// user_id) — message is the third segment, so its prefix is exact.
	reactionPrefix := partition + kvSep + bigintSeg(messageID) + kvSep
	if _, err := tx.ExecContext(ctx, `
DELETE FROM fluxer_kv
WHERE table_name = $1 AND left(row_key, length($2)) = $2`, reactionsTable, reactionPrefix); err != nil {
		return false, err
	}

	// attachment_lookup: (channel_id, attachment_id, filename).
	attachments := messageAttachments(row)
	for _, a := range attachments {
		lookupPrefix := bigintSeg(channelID) + kvSep + bigintSeg(a.AttachmentID) + kvSep
		if _, err := tx.ExecContext(ctx, `
DELETE FROM fluxer_kv
WHERE table_name = $1 AND left(row_key, length($2)) = $2`, attachmentLookupTbl, lookupPrefix); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	// Storage and decay rows live outside the transaction on purpose: an S3 call
	// cannot join it, and the queue makes a failed delete retryable.
	for _, a := range attachments {
		queueMessageAttachmentPurge(ctx, channelID, a)
	}
	markBucketEmptyIfUnused(ctx, channelID, partition, bucket)
	return true, nil
}

// queueMessageAttachmentPurge hands the object to the existing asset-deletion
// queue, the same one expireAttachments fills. The queue item is what makes the
// delete survive a crash between the row going away and S3 being touched.
func queueMessageAttachmentPurge(ctx context.Context, channelID string, a messageAttachment) {
	s3Key := fmt.Sprintf("attachments/%s/%s/%s", channelID, a.AttachmentID, a.Filename)
	if err := queueAssetDeletion(ctx, s3Key, "", "message-deleted"); err != nil {
		log.Printf("delete message: queue attachment %s: %v (continuing)", s3Key, err)
	}
	// The decay bookkeeping rows point at an object that no longer exists, and
	// expireAttachments would later try to queue it a second time.
	_, _ = db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		attachmentDecayByID, bigintSeg(a.AttachmentID))
	_, _ = db.ExecContext(ctx, `
DELETE FROM fluxer_kv
WHERE table_name = $1 AND row_data->'attachment_id'->>'value' = $2`,
		attachmentDecayByExp, a.AttachmentID)
}

// markBucketEmptyIfUnused mirrors the api's post-delete maintenance: when the
// last message of a bucket is gone, the bucket stamp moves from "has messages"
// to the empty-bucket list. Without it every later scan keeps visiting buckets
// that hold nothing.
func markBucketEmptyIfUnused(ctx context.Context, channelID, partition string, bucket int64) {
	var one int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM fluxer_kv WHERE table_name = $1 AND partition_key = $2 LIMIT 1`,
		messagesTable, partition).Scan(&one)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Printf("delete message: bucket check %s: %v (continuing)", partition, err)
		return
	}
	if err == nil {
		return
	}
	emptyKey := partition
	if _, err := db.ExecContext(ctx,
		`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, channelBucketsTable, emptyKey); err != nil {
		log.Printf("delete message: drop bucket stamp %s: %v (continuing)", emptyKey, err)
		return
	}
	if err := putKV(ctx, emptyBucketsTable, partition, emptyKey, map[string]any{
		"channel_id": bigintValue(channelID),
		"bucket":     int64(bucket),
		"updated_at": kvDate(time.Now()),
	}, ""); err != nil {
		log.Printf("delete message: mark bucket %s empty: %v (continuing)", emptyKey, err)
	}
}

// ---------- messageShred ----------

type shredEntry struct {
	ChannelID string `json:"channel_id"`
	MessageID string `json:"message_id"`
}

type shredPayload struct {
	JobID        string       `json:"job_id"`
	AdminUserID  string       `json:"admin_user_id"`
	TargetUserID string       `json:"target_user_id"`
	Entries      []shredEntry `json:"entries"`
}

type shredStatus struct {
	Status      string `json:"status"`
	Requested   int    `json:"requested"`
	Total       int    `json:"total"`
	Processed   int    `json:"processed"`
	Skipped     int    `json:"skipped"`
	StartedAt   string `json:"started_at"`
	CompletedAt string `json:"completed_at,omitempty"`
}

// taskMessageShred ports MessageShred.ts: an admin picks messages by hand (a
// legal or abuse request), and the worker deletes exactly those that still exist
// and still belong to the target author. Everything else is skipped and counted,
// because a shred must never delete somebody else's message.
func taskMessageShred(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("messageShred: %w", err)
		}
	}
	var job shredPayload
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &job); err != nil {
		return fmt.Errorf("messageShred: bad payload: %w", err)
	}
	if job.JobID == "" || job.TargetUserID == "" {
		return fmt.Errorf("messageShred: job_id and target_user_id are required")
	}

	status := shredStatus{
		Status:    "in_progress",
		Requested: len(job.Entries),
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	persist := func(ctx context.Context) {
		body, err := json.Marshal(status)
		if err != nil {
			return
		}
		if err := valkey().Set(ctx, messageShredStatus+job.JobID, body, messageShredTTL).Err(); err != nil {
			log.Printf("task messageShred: persist status: %v (continuing)", err)
		}
	}
	persist(ctx)

	seen := make(map[string]struct{}, len(job.Entries))
	for start := 0; start < len(job.Entries); start += shredInputSlice {
		end := start + shredInputSlice
		if end > len(job.Entries) {
			end = len(job.Entries)
		}
		if err := shredSlice(ctx, job, job.Entries[start:end], seen, &status, persist); err != nil {
			return err
		}
	}

	status.Status = "completed"
	status.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	persist(ctx)
	return nil
}

// shredEntryKey validates one entry and returns the dedupe key. Upstream accepts
// any string and turns it into a typed id, skipping what does not parse; the
// same check is done here up front so a malformed entry never reaches the
// database.
func shredEntryKey(e shredEntry) (string, bool) {
	if e.ChannelID == "" || e.MessageID == "" || !isDigits(e.ChannelID) || !isDigits(e.MessageID) {
		return "", false
	}
	return e.ChannelID + ":" + e.MessageID, true
}

func shredSlice(ctx context.Context, job shredPayload, entries []shredEntry, seen map[string]struct{}, status *shredStatus, persist func(context.Context)) error {
	valid := make([]shredEntry, 0, len(entries))
	for _, e := range entries {
		key, ok := shredEntryKey(e)
		if !ok {
			status.Skipped++
			continue
		}
		if _, dup := seen[key]; dup {
			status.Skipped++
			continue
		}
		seen[key] = struct{}{}
		valid = append(valid, e)
	}
	for start := 0; start < len(valid); start += shredValidationChunk {
		end := start + shredValidationChunk
		if end > len(valid) {
			end = len(valid)
		}
		chunk := valid[start:end]
		deletable := chunk[:0:0]
		for _, e := range chunk {
			row, err := readMessage(ctx, e.ChannelID, e.MessageID)
			if err != nil {
				return fmt.Errorf("messageShred: read %s/%s: %w", e.ChannelID, e.MessageID, err)
			}
			if row == nil || typedString(row["author_id"]) != job.TargetUserID {
				status.Skipped++
				continue
			}
			deletable = append(deletable, e)
		}
		status.Total += len(deletable)
		persist(ctx)
		for start := 0; start < len(deletable); start += shredDeletionChunk {
			end := start + shredDeletionChunk
			if end > len(deletable) {
				end = len(deletable)
			}
			batch := deletable[start:end]
			byChannel := make(map[string][]string, 1)
			for _, e := range batch {
				existed, err := deleteMessageRow(ctx, e.ChannelID, e.MessageID)
				if err != nil {
					return fmt.Errorf("messageShred: delete %s/%s: %w", e.ChannelID, e.MessageID, err)
				}
				if !existed {
					status.Skipped++
					continue
				}
				status.Processed++
				byChannel[e.ChannelID] = append(byChannel[e.ChannelID], e.MessageID)
			}
			persist(ctx)
			for channelID, ids := range byChannel {
				dispatchBulkDelete(ctx, channelID, ids)
			}
		}
	}
	return nil
}

// dispatchBulkDelete mirrors upstream's MESSAGE_DELETE_BULK: one event per
// channel with the ids that just went away. A DM has no guild, and this instance
// has no DMs in the shred path, so that case is logged rather than guessed.
func dispatchBulkDelete(ctx context.Context, channelID string, ids []string) {
	nc := workerNATS()
	if nc == nil {
		return
	}
	channel, err := readChannel(ctx, channelID)
	if err != nil || channel == nil {
		log.Printf("task messageShred: channel %s not readable, MESSAGE_DELETE_BULK not dispatched", channelID)
		return
	}
	guildID := typedString(channel["guild_id"])
	if guildID == "" {
		log.Printf("task messageShred: channel %s has no guild, MESSAGE_DELETE_BULK not dispatched", channelID)
		return
	}
	payloadIDs := make([]any, 0, len(ids))
	for _, id := range ids {
		payloadIDs = append(payloadIDs, id)
	}
	if err := dispatchGuild(nc, guildID, "MESSAGE_DELETE_BULK", map[string]any{
		"channel_id": channelID,
		"ids":        payloadIDs,
	}); err != nil {
		log.Printf("task messageShred: dispatch MESSAGE_DELETE_BULK for %s: %v (continuing)", channelID, err)
	}
}
