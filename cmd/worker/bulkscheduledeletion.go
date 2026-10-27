package main

// taskBulkScheduleUserDeletion ports admin_bulk/BulkScheduleUserDeletion.ts.
//
// An operator closes a batch of accounts with a grace period. Per account the
// worker writes the closure the api writes: the quiet flag pair
// (SELF_DELETED instead of DELETED for a request; DELETED and no SELF_DELETED for
// enforcement), pending_deletion_at, the reason code, who scheduled it and when,
// then the valkey queue member, the session and token revocation, the private
// USER_UPDATE to the owner and one admin audit entry. Finally a summary entry.
//
// Not ported, all because the machinery is absent rather than because it is
// uninteresting:
//   - the enforcement basis. Upstream first records an enforcement decision and
//     only writes the closure inside it; there is no decision writer here, so an
//     enforcement basis is refused up front instead of scheduling a closure that
//     no decision owns.
//   - the notification email (no email path in this worker).
//   - subscription cancellation and store-entitlement revocation (nothing to
//     cancel on a self-hosted instance without payments).

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

const (
	accountDeletionMinDays = 14
	deletionReasonInactive = 19
)

func taskBulkScheduleUserDeletion(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("bulkScheduleUserDeletion: %w", err)
		}
	}
	adminUserID := stringField(payload, "admin_user_id")
	reason := stringField(payload, "audit_log_reason")
	userIDs := stringSliceField(payload, "user_ids")
	requestedDays := int(int64OrZero(payload, "days_until_deletion"))
	basis, _ := payload["basis"].(map[string]any)
	basisType := ""
	if basis != nil {
		basisType = stringField(basis, "type")
	}
	if adminUserID == "" || !isDigits(adminUserID) {
		return fmt.Errorf("bulkScheduleUserDeletion: admin_user_id is required")
	}
	if len(userIDs) == 0 {
		return fmt.Errorf("bulkScheduleUserDeletion: user_ids is empty")
	}
	if basisType == "enforcement" {
		return fmt.Errorf("bulkScheduleUserDeletion: the enforcement basis needs a decision writer, which this worker does not have")
	}
	days := requestedDays
	if days < accountDeletionMinDays {
		days = accountDeletionMinDays
	}
	reasonCode := deletionReasonInactive
	if basisType == "holder_request" {
		reasonCode = deletionReasonUserRequested
	}

	scheduled, refused, failed := 0, 0, 0
	for _, userID := range userIDs {
		if !isDigits(userID) {
			failed++
			log.Printf("bulkScheduleUserDeletion: skipping malformed user id %q", userID)
			continue
		}
		err := scheduleOneDeletion(ctx, adminUserID, userID, days, reasonCode, reason)
		if err != nil {
			if isRefusedFlagChange(err) {
				refused++
				log.Printf("bulkScheduleUserDeletion: %s: %v", userID, err)
				continue
			}
			failed++
			log.Printf("bulkScheduleUserDeletion: %s: %v", userID, err)
			continue
		}
		scheduled++
	}

	metadata := [][2]string{
		{"user_count", strconv.Itoa(len(userIDs))},
		{"basis", basisType},
		{"days", strconv.Itoa(days)},
	}
	if refused > 0 {
		metadata = append(metadata, [2]string{"refused", strconv.Itoa(refused)})
	}
	if failed > 0 {
		metadata = append(metadata, [2]string{"failed", strconv.Itoa(failed)})
	}
	if err := writeAdminAuditLog(ctx, adminUserID, "user", userIDs[0], "bulk_schedule_deletion", reason, metadata); err != nil {
		return fmt.Errorf("bulkScheduleUserDeletion: summary audit: %w", err)
	}
	log.Printf("bulkScheduleUserDeletion: users=%d scheduled=%d refused=%d failed=%d days=%d",
		len(userIDs), scheduled, refused, failed, days)
	return nil
}

// scheduleOneDeletion writes the closure for one account. The row change, the
// queue member and the owner's update are one unit: a member without a closure
// would be picked up and skipped, and a closure without a member would never be
// processed.
func scheduleOneDeletion(ctx context.Context, adminUserID, userID string, days, reasonCode int, auditReason string) error {
	pendingAt, flags, err := writeDeletionClosure(ctx, adminUserID, userID, days, reasonCode)
	if err != nil {
		return err
	}
	if err := scheduleDeletion(ctx, userID, pendingAt, reasonCode); err != nil {
		return fmt.Errorf("queue: %w", err)
	}
	if nc := workerNATS(); nc != nil {
		if err := revokeUserSessions(ctx, nc, userID); err != nil {
			log.Printf("bulkScheduleUserDeletion: %s: revoke sessions: %v (closure written)", userID, err)
		}
	}
	revokeOAuthTokens(ctx, userID)
	if nc := workerNATS(); nc != nil {
		if updated, err := readKV(ctx, "users", bigintSeg(userID)); err == nil && updated != nil {
			if data, ok := userPrivateResponse(updated, time.Now().UTC()); ok {
				if err := dispatchToUser(nc, userID, "USER_UPDATE", data); err != nil {
					log.Printf("bulkScheduleUserDeletion: %s: dispatch: %v (closure written)", userID, err)
				}
			}
		}
	}
	if err := writeAdminAuditLog(ctx, adminUserID, "user", userID, "schedule_deletion", auditReason, [][2]string{
		{"basis", basisLabel(reasonCode)},
		{"flag_value", strconv.FormatUint(flags, 10)},
		{"pending_deletion_at", pendingAt.Format(time.RFC3339)},
	}); err != nil {
		log.Printf("bulkScheduleUserDeletion: %s: audit: %v (closure written)", userID, err)
	}
	return nil
}

// writeDeletionClosure writes the row half: flags, the schedule and who set it.
// Split out so the row work is testable without valkey or NATS.
func writeDeletionClosure(ctx context.Context, adminUserID, userID string, days, reasonCode int) (time.Time, uint64, error) {
	rowKey := bigintSeg(userID)
	row, err := readKV(ctx, "users", rowKey)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("read user: %w", err)
	}
	if row == nil {
		return time.Time{}, 0, fmt.Errorf("user does not exist")
	}
	if typedString(row["pending_deletion_at"]) != "" {
		return time.Time{}, 0, refusedFlagChange{"a deletion is already scheduled for this account"}
	}
	now := time.Now().UTC()
	pendingAt := now.AddDate(0, 0, days)
	flags := userFlags(row)&^userFlagDeleted | userFlagSelfDeleted

	patch := map[string]any{
		"flags":                            map[string]any{"__fluxer_type": "bigint", "value": strconv.FormatUint(flags, 10)},
		"pending_deletion_at":              kvDate(pendingAt),
		"deletion_reason_code":             reasonCode,
		"deletion_scheduled_by":            bigintValue(adminUserID),
		"deletion_scheduled_at":            kvDate(now),
		"enforcement_decision_id":          nil,
		"pending_bulk_message_deletion_at": nil,
	}
	for field, value := range patch {
		if err := patchRowField(ctx, "users", rowKey, field, value); err != nil {
			return time.Time{}, 0, fmt.Errorf("patch %s: %w", field, err)
		}
	}
	return pendingAt, flags, nil
}

func basisLabel(reasonCode int) string {
	if reasonCode == deletionReasonUserRequested {
		return "holder_request"
	}
	return "housekeeping"
}
