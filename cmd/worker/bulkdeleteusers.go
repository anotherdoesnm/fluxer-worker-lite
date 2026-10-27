package main

// taskBulkDeleteMessagesForUsers ports admin_bulk/BulkDeleteMessagesForUsers.ts.
//
// The expensive part of "delete every message from these users" is finding the
// messages, not deleting them: the deletion already has an owner (messageShred,
// with its own status row and batching). So the bulk task collects the author
// index per user, hands the list to one messageShred job per user, records one
// admin audit entry per user and a summary row for the sweep. Nothing is deleted
// synchronously, which is also what makes the job resumable.
//
// Not ported: the enforcement branch. When the basis is an enforcement decision,
// the api first records a decision row and only queues the shred inside it; this
// worker has no decision writer, so an enforcement basis is accepted but the
// decision is not created — recorded in OMISSIONS.md rather than faked.

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/nats-io/nats.go"
)

func taskBulkDeleteMessagesForUsers(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("bulkDeleteMessagesForUsers: %w", err)
		}
	}
	adminUserID := stringField(payload, "admin_user_id")
	reason := stringField(payload, "audit_log_reason")
	userIDs := stringSliceField(payload, "user_ids")
	if adminUserID == "" || !isDigits(adminUserID) {
		return fmt.Errorf("bulkDeleteMessagesForUsers: admin_user_id is required")
	}
	if len(userIDs) == 0 {
		return fmt.Errorf("bulkDeleteMessagesForUsers: user_ids is empty")
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return fmt.Errorf("bulkDeleteMessagesForUsers: jetstream: %w", err)
	}

	queued, skipped, failed := 0, 0, 0
	for _, userID := range userIDs {
		if !isDigits(userID) {
			failed++
			log.Printf("bulkDeleteMessagesForUsers: skipping malformed user id %q", userID)
			continue
		}
		entries, channelCount, messageCount, err := collectUserMessageEntries(ctx, userID)
		if err != nil {
			failed++
			log.Printf("bulkDeleteMessagesForUsers: %s: collect: %v", userID, err)
			continue
		}
		if messageCount == 0 {
			skipped++
			continue
		}
		jobID, err := queueMessageShred(ctx, js, adminUserID, userID, entries)
		if err != nil {
			failed++
			log.Printf("bulkDeleteMessagesForUsers: %s: queue shred: %v", userID, err)
			continue
		}
		queued++
		if err := writeAdminAuditLog(ctx, adminUserID, "message_deletion", userID,
			"delete_all_user_messages", reason, [][2]string{
				{"user_id", userID},
				{"channel_count", strconv.Itoa(channelCount)},
				{"message_count", strconv.Itoa(messageCount)},
				{"dry_run", "false"},
				{"job_id", jobID},
			}); err != nil {
			log.Printf("bulkDeleteMessagesForUsers: %s: audit: %v (shred queued)", userID, err)
		}
	}

	metadata := [][2]string{
		{"user_count", strconv.Itoa(len(userIDs))},
		{"queued", strconv.Itoa(queued)},
	}
	if skipped > 0 {
		metadata = append(metadata, [2]string{"without_messages", strconv.Itoa(skipped)})
	}
	if failed > 0 {
		metadata = append(metadata, [2]string{"failed", strconv.Itoa(failed)})
	}
	if err := writeAdminAuditLog(ctx, adminUserID, "message_deletion", userIDs[0],
		"bulk_delete_user_messages", reason, metadata); err != nil {
		return fmt.Errorf("bulkDeleteMessagesForUsers: summary audit: %w", err)
	}
	log.Printf("bulkDeleteMessagesForUsers: users=%d queued=%d without_messages=%d failed=%d",
		len(userIDs), queued, skipped, failed)
	return nil
}

// collectUserMessageEntries reads the author index once per user and returns the
// (channel, message) pairs messageShred expects.
func collectUserMessageEntries(ctx context.Context, userID string) (entries [][2]string, channels, messages int, err error) {
	refs, err := listMessagesByAuthor(ctx, userID)
	if err != nil {
		return nil, 0, 0, err
	}
	seen := make(map[string]struct{}, len(refs))
	entries = make([][2]string, 0, len(refs))
	for _, ref := range refs {
		if _, dup := seen[ref.ChannelID]; !dup {
			seen[ref.ChannelID] = struct{}{}
		}
		entries = append(entries, [2]string{ref.ChannelID, ref.MessageID})
	}
	return entries, len(seen), len(entries), nil
}

// queueMessageShred publishes the shred job with the same payload shape the api
// writes and the same job key, so a repeated sweep cannot queue the same user
// twice.
func queueMessageShred(ctx context.Context, js nats.JetStreamContext, adminUserID, targetUserID string, entries [][2]string) (string, error) {
	jobID, err := nextSnowflakeID()
	if err != nil {
		return "", fmt.Errorf("snowflake: %w", err)
	}
	list := make([]any, 0, len(entries))
	for _, entry := range entries {
		list = append(list, map[string]any{
			"channel_id": entry[0],
			"message_id": entry[1],
		})
	}
	payload := map[string]any{
		"job_id":         jobID,
		"admin_user_id":  adminUserID,
		"target_user_id": targetUserID,
		"entries":        list,
	}
	key := "message_shred_" + targetUserID + "_" + jobID
	if err := publishJobWithID(js, jobPrefixForPublish(), "messageShred", payload, key); err != nil {
		return "", err
	}
	return jobID, nil
}
