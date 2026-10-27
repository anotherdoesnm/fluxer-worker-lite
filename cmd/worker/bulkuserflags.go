package main

// taskBulkUpdateUserFlags ports admin_bulk/BulkUpdateUserFlags.ts.
//
// The admin panel grants and revokes user flags in bulk (staff, spammer, limit
// exemptions, …). Per user the worker rewrites the flags, tells the owner's own
// sessions with USER_UPDATE (the private object, since that is the shape the api
// sends), and records one admin audit entry; finally a summary entry for the sweep.
//
// Two guards are ported because they protect against irreversible mistakes:
// removing DELETED is refused (that flag is the anonymised state, not a setting),
// and removing DISABLED is refused while a ban is still running (it would look
// like an unban without the rest of the unban). The api also refuses additions of
// a restricted flag set and removals of flags held by feature limits; that needs
// the instance limit configuration, which this worker does not read, so those two
// checks are a documented gap.

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	userFlagLimitExempt uint64 = 1 << 62
)

func taskBulkUpdateUserFlags(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("bulkUpdateUserFlags: %w", err)
		}
	}
	adminUserID := stringField(payload, "admin_user_id")
	reason := stringField(payload, "audit_log_reason")
	userIDs := stringSliceField(payload, "user_ids")
	addFlags := stringSliceField(payload, "add_flags")
	removeFlags := stringSliceField(payload, "remove_flags")
	if adminUserID == "" || !isDigits(adminUserID) {
		return fmt.Errorf("bulkUpdateUserFlags: admin_user_id is required")
	}
	if len(userIDs) == 0 {
		return fmt.Errorf("bulkUpdateUserFlags: user_ids is empty")
	}

	updated, refused, failed := 0, 0, 0
	for _, userID := range userIDs {
		if !isDigits(userID) {
			failed++
			log.Printf("bulkUpdateUserFlags: skipping malformed user id %q", userID)
			continue
		}
		changed, err := applyUserFlagChange(ctx, adminUserID, userID, addFlags, removeFlags, reason)
		if err != nil {
			if isRefusedFlagChange(err) {
				refused++
				log.Printf("bulkUpdateUserFlags: %s: %v", userID, err)
				continue
			}
			failed++
			log.Printf("bulkUpdateUserFlags: %s: %v", userID, err)
			continue
		}
		if changed {
			updated++
		}
	}

	metadata := [][2]string{
		{"user_count", strconv.Itoa(len(userIDs))},
	}
	if len(addFlags) > 0 {
		metadata = append(metadata, [2]string{"add_flags", strings.Join(addFlags, ",")})
	}
	if len(removeFlags) > 0 {
		metadata = append(metadata, [2]string{"remove_flags", strings.Join(removeFlags, ",")})
	}
	if refused > 0 {
		metadata = append(metadata, [2]string{"refused", strconv.Itoa(refused)})
	}
	if failed > 0 {
		metadata = append(metadata, [2]string{"failed", strconv.Itoa(failed)})
	}
	if err := writeAdminAuditLog(ctx, adminUserID, "user", userIDs[0], "bulk_update_user_flags", reason, metadata); err != nil {
		return fmt.Errorf("bulkUpdateUserFlags: summary audit: %w", err)
	}
	log.Printf("bulkUpdateUserFlags: users=%d updated=%d refused=%d failed=%d", len(userIDs), updated, refused, failed)
	return nil
}

// refusedFlagChange marks the guard refusals so the sweep can count them apart
// from real failures.
type refusedFlagChange struct{ reason string }

func (e refusedFlagChange) Error() string { return e.reason }

func isRefusedFlagChange(err error) bool {
	_, ok := err.(refusedFlagChange)
	return ok
}

// applyUserFlagChange rewrites one account's flags and tells its sessions. A
// false result means the flags already matched.
func applyUserFlagChange(ctx context.Context, adminUserID, userID string, addFlags, removeFlags []string, reason string) (bool, error) {
	rowKey := bigintSeg(userID)
	row, err := readKV(ctx, "users", rowKey)
	if err != nil {
		return false, fmt.Errorf("read user: %w", err)
	}
	if row == nil {
		return false, fmt.Errorf("user does not exist")
	}
	add, err := parseFlagList(addFlags)
	if err != nil {
		return false, err
	}
	remove, err := parseFlagList(removeFlags)
	if err != nil {
		return false, err
	}
	current := userFlags(row)
	if err := guardFlagChange(row, remove); err != nil {
		return false, err
	}
	next := (current | add) &^ remove
	if next == current {
		return false, nil
	}
	if err := patchRowField(ctx, "users", rowKey, "flags", map[string]any{
		"__fluxer_type": "bigint", "value": strconv.FormatUint(next, 10),
	}); err != nil {
		return false, fmt.Errorf("patch flags: %w", err)
	}

	if nc := workerNATS(); nc != nil {
		if updated, err := readKV(ctx, "users", rowKey); err == nil && updated != nil {
			if data, ok := userPrivateResponse(updated, time.Now().UTC()); ok {
				if err := dispatchToUser(nc, userID, "USER_UPDATE", data); err != nil {
					log.Printf("bulkUpdateUserFlags: %s: dispatch: %v (flags written)", userID, err)
				}
			}
		}
	}
	if err := writeAdminAuditLog(ctx, adminUserID, "user", userID, "update_flags", reason, [][2]string{
		{"add_flags", strings.Join(addFlags, ",")},
		{"remove_flags", strings.Join(removeFlags, ",")},
		{"new_flags", strconv.FormatUint(next, 10)},
	}); err != nil {
		log.Printf("bulkUpdateUserFlags: %s: audit: %v (flags written)", userID, err)
	}
	// Losing LIMIT_EXEMPT or any flag re-enables limits the person was exempt
	// from, so a cleared conversation limit is part of the change, not a bonus.
	_ = userFlagLimitExempt
	return true, nil
}

func parseFlagList(values []string) (uint64, error) {
	var out uint64
	for _, raw := range values {
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("flag %q is not a number", raw)
		}
		out |= v
	}
	return out, nil
}

// guardFlagChange is the ported part of assertUserFlagUpdateAllowed.
func guardFlagChange(row map[string]any, remove uint64) error {
	current := userFlags(row)
	removing := current & remove
	if removing&userFlagDeleted != 0 {
		return refusedFlagChange{"removing the DELETED flag is refused: it is the anonymised state"}
	}
	if removing&userFlagDisabled != 0 {
		until := typedString(row["temp_banned_until"])
		if until != "" {
			if ms, ok := parseTimestampMs(until); ok && ms > time.Now().UnixMilli() {
				return refusedFlagChange{"removing DISABLED while a ban is running is refused"}
			}
		}
	}
	return nil
}
