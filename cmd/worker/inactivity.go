package main

// taskProcessInactivityDeletions ports ProcessInactivityDeletions.ts.
//
// The instance may delete accounts that have been unused for a long time, but
// never without a warning first: the api emails the person, marks
// `inactivity_warning_sent:<id>` in valkey with a timestamp, and only after the
// warning's grace window does the next tick schedule the deletion. If the instance
// cannot send email, the api returns before warning *or* deleting, and so does this
// port — deleting silently would be worse than leaving the account alone.
//
// Ported: the inactivity branch only (not the abandoned-account sweeps), and the
// warning is only *read*, never sent, because this worker has no email path. The
// consequence on this instance (no SMTP configured) is a task that logs and does
// nothing, which is exactly what the api does there.
//
// The activity source is the api's own: valkey key `user_activity:<id>` written on
// every request, falling back to `last_active_at`, then to the account's snowflake
// (creation time) for accounts that never acted.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	inactivityDefaultDays      = 730
	inactivityWarningLeadDays  = 30
	inactivityWarningExtraDays = 5
	deletionGraceHoursDefault  = 336
	appStoreReviewerFlag       = 1 << 53
)

type inactivityConfig struct {
	Mode            string `json:"mode"`
	InactiveDays    int    `json:"inactive_days"`
	WarningLeadDays int    `json:"warning_lead_days"`
}

func taskProcessInactivityDeletions(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("processInactivityDeletions: %w", err)
		}
	}
	cfg, err := inactivityConfigFromDB(ctx)
	if err != nil {
		return fmt.Errorf("processInactivityDeletions: %w", err)
	}
	if cfg.Mode != "after_inactivity" {
		log.Printf("processInactivityDeletions: inactivity deletion is off, nothing to do")
		return nil
	}

	users, err := allUserRows(ctx)
	if err != nil {
		return fmt.Errorf("processInactivityDeletions: %w", err)
	}
	now := time.Now().UTC()
	var warned, scheduled, skipped int
	for _, user := range users {
		userID := typedString(user["user_id"])
		if !shouldConsiderForInactivity(user) {
			continue
		}
		lastActivity := userLastActivity(ctx, user)
		if now.Sub(lastActivity) < time.Duration(cfg.InactiveDays)*24*time.Hour {
			continue
		}
		warningAt, hasWarning := inactivityWarningAt(ctx, userID)
		if !hasWarning {
			// No email path here, so the warning cannot be sent and nothing may be
			// scheduled: the api returns at the same point when email is disabled.
			warned++
			continue
		}
		if now.Sub(warningAt) < time.Duration(cfg.WarningLeadDays)*24*time.Hour {
			skipped++
			continue
		}
		if err := scheduleInactivityDeletion(ctx, userID); err != nil {
			log.Printf("processInactivityDeletions: %s: %v", userID, err)
			continue
		}
		scheduled++
	}
	log.Printf("processInactivityDeletions: users=%d awaiting_warning=%d in_grace=%d scheduled=%d",
		len(users), warned, skipped, scheduled)
	return nil
}

func inactivityConfigFromDB(ctx context.Context) (inactivityConfig, error) {
	cfg := inactivityConfig{Mode: "after_inactivity", InactiveDays: inactivityDefaultDays, WarningLeadDays: inactivityWarningLeadDays}
	if v := os.Getenv("FLUXER_INACTIVITY_DELETION_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.InactiveDays = n
		}
	}
	var raw string
	err := db.QueryRowContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'instance_configuration' AND row_key LIKE '%accounts_config%'
LIMIT 1`).Scan(&raw)
	if err != nil {
		return cfg, nil
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		return cfg, nil
	}
	// The config value is a JSON string inside `value`, as everywhere in this table.
	inner := typedString(row["value"])
	if inner == "" {
		return cfg, nil
	}
	var stored struct {
		InactivityDeletion inactivityConfig `json:"inactivity_deletion"`
	}
	if err := json.Unmarshal([]byte(inner), &stored); err != nil {
		return cfg, nil
	}
	if stored.InactivityDeletion.Mode != "" {
		cfg.Mode = stored.InactivityDeletion.Mode
	}
	if stored.InactivityDeletion.InactiveDays > 0 {
		cfg.InactiveDays = stored.InactivityDeletion.InactiveDays
	}
	if stored.InactivityDeletion.WarningLeadDays > 0 {
		cfg.WarningLeadDays = stored.InactivityDeletion.WarningLeadDays
	}
	return cfg, nil
}

// shouldConsiderForInactivity is the api's skip list.
func shouldConsiderForInactivity(user map[string]any) bool {
	userID := typedString(user["user_id"])
	if userID == "" || userID == systemUserID {
		return false
	}
	if b, _ := user["bot"].(bool); b {
		return false
	}
	if b, _ := user["system"].(bool); b {
		return false
	}
	if userFlags(user)&appStoreReviewerFlag != 0 {
		return false
	}
	return typedString(user["pending_deletion_at"]) == ""
}

// userLastActivity reads the api's activity key, then last_active_at, then the
// account's snowflake.
func userLastActivity(ctx context.Context, user map[string]any) time.Time {
	userID := typedString(user["user_id"])
	if val, err := valkey().Get(ctx, "user_activity:"+userID).Result(); err == nil {
		if ms, ok := parseTimestampMs(val); ok {
			return time.UnixMilli(ms).UTC()
		}
	}
	if ms, ok := parseTimestampMs(typedString(user["last_active_at"])); ok {
		return time.UnixMilli(ms).UTC()
	}
	if id, err := parseInt64(userID); err == nil {
		return snowflakeTime(id)
	}
	return time.Now().UTC()
}

// inactivityWarningAt reads the marker the api writes when it sends the warning.
func inactivityWarningAt(ctx context.Context, userID string) (time.Time, bool) {
	val, err := valkey().Get(ctx, "inactivity_warning_sent:"+userID).Result()
	if err != nil {
		return time.Time{}, false
	}
	ms, ok := parseTimestampMs(val)
	if !ok {
		return time.Time{}, false
	}
	return time.UnixMilli(ms).UTC(), true
}

// scheduleInactivityDeletion writes the closure the api writes for an inactive
// account: SELF_DELETED, the schedule, the reason, the queue member.
func scheduleInactivityDeletion(ctx context.Context, userID string) error {
	rowKey := bigintSeg(userID)
	row, err := readKV(ctx, "users", rowKey)
	if err != nil || row == nil {
		return fmt.Errorf("read user: %w", err)
	}
	graceHours := deletionGraceHoursDefault
	if v := os.Getenv("FLUXER_DELETION_GRACE_HOURS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			graceHours = n
		}
	}
	now := time.Now().UTC()
	pendingAt := now.Add(time.Duration(graceHours) * time.Hour)
	flags := userFlags(row)&^userFlagDeleted | userFlagSelfDeleted
	for field, value := range map[string]any{
		"flags":                 map[string]any{"__fluxer_type": "bigint", "value": strconv.FormatUint(flags, 10)},
		"pending_deletion_at":   kvDate(pendingAt),
		"deletion_reason_code":  deletionReasonInactive,
		"deletion_scheduled_by": bigintValue(systemUserID),
		"deletion_scheduled_at": kvDate(now),
	} {
		if err := patchRowField(ctx, "users", rowKey, field, value); err != nil {
			return fmt.Errorf("patch %s: %w", field, err)
		}
	}
	if err := scheduleDeletion(ctx, userID, pendingAt, deletionReasonInactive); err != nil {
		return fmt.Errorf("queue: %w", err)
	}
	if nc := workerNATS(); nc != nil {
		if updated, err := readKV(ctx, "users", rowKey); err == nil && updated != nil {
			if data, ok := userPrivateResponse(updated, now); ok {
				if err := dispatchToUser(nc, userID, "USER_UPDATE", data); err != nil {
					log.Printf("processInactivityDeletions: %s: dispatch: %v", userID, err)
				}
			}
		}
	}
	return nil
}

func allUserRows(ctx context.Context) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `SELECT row_data::text FROM fluxer_kv WHERE table_name = 'users' ORDER BY row_key`)
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
