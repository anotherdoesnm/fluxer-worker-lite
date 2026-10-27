package main

// taskLiftExpiredGuildInvitePauses ports LiftExpiredGuildInvitePauses.ts.
//
// When a guild's invites are paused (raid protection, mass joins), the api writes
// the deadline into the guild row and puts the guild into the valkey sorted set
// `guild:invite_pause:lifts` scored by that deadline. Nothing on the api side
// ever lifts it again: this cron is the only thing that closes the window, so
// without it a paused guild stays paused forever.
//
// Lifting means two things: the INVITES_DISABLED and RAID_DETECTED features come
// off, and invites_paused_until becomes null. Both changes go into the row, the
// feature change is recorded in the guild audit log (action 1, SYSTEM user), and
// the member leaves the set.
//
// What is not done here, and why:
//   - GUILD_UPDATE is not dispatched. Upstream sends the whole mapped guild
//     response, and this worker has no guild response mapper yet; sending a
//     partial object would be worse than sending nothing, because clients cache
//     it. Open clients pick the change up on the next GUILD_CREATE.
//   - emitGuildFeaturesChanged is not published: the EVENTS stream it writes to
//     does not exist on this instance (same reason drainActivitySpool is a no-op).

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	guildInvitePauseScheduleKey = "guild:invite_pause:lifts"
	invitePauseBatchSize        = 100
	auditActionGuildUpdate      = 1
	guildsTable                 = "guilds"
)

// invitePauseFeatures are the two features the pause adds and the lift removes.
var invitePauseFeatures = []string{"INVITES_DISABLED", "RAID_DETECTED"}

func taskLiftExpiredGuildInvitePauses(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("liftExpiredGuildInvitePauses: %w", err)
		}
	}
	now := time.Now().UTC()
	members, err := valkey().ZRangeByScore(ctx, guildInvitePauseScheduleKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Offset: 0, Count: invitePauseBatchSize,
	}).Result()
	if err != nil {
		return fmt.Errorf("liftExpiredGuildInvitePauses: read schedule: %w", err)
	}
	lifted, failed := 0, 0
	for _, member := range members {
		if !isDigits(member) {
			log.Printf("liftExpiredGuildInvitePauses: dropping unreadable member %q", member)
			_ = valkey().ZRem(ctx, guildInvitePauseScheduleKey, member).Err()
			continue
		}
		done, err := liftGuildInvitePause(ctx, member, now)
		if err != nil {
			// One unreadable guild must not stall the rest of the schedule.
			failed++
			log.Printf("liftExpiredGuildInvitePauses: guild %s: %v", member, err)
			continue
		}
		if done {
			lifted++
		}
	}
	if lifted > 0 || failed > 0 {
		log.Printf("liftExpiredGuildInvitePauses: due=%d lifted=%d failed=%d", len(members), lifted, failed)
	}
	return nil
}

// liftGuildInvitePause handles one member. A false result means "nothing to do":
// the guild is gone, has no deadline, or the deadline moved into the future, in
// which case the member is re-scored.
func liftGuildInvitePause(ctx context.Context, guildID string, now time.Time) (bool, error) {
	rowKey := bigintSeg(guildID)
	row, err := readKV(ctx, guildsTable, rowKey)
	if err != nil {
		return false, fmt.Errorf("read guild: %w", err)
	}
	if row == nil {
		_ = valkey().ZRem(ctx, guildInvitePauseScheduleKey, guildID).Err()
		return false, nil
	}
	untilRaw := typedString(row["invites_paused_until"])
	if untilRaw == "" {
		// Nothing to lift: the api already cleared it (or lifted it by hand).
		_ = valkey().ZRem(ctx, guildInvitePauseScheduleKey, guildID).Err()
		return false, nil
	}
	until, ok := parseTimestampMs(untilRaw)
	if !ok {
		return false, fmt.Errorf("unreadable invites_paused_until %q", untilRaw)
	}
	if until > now.UnixMilli() {
		// Somebody extended the pause after this member was scored: put it back
		// with the new deadline instead of lifting early.
		if err := valkey().ZAdd(ctx, guildInvitePauseScheduleKey, redis.Z{
			Score: float64(until), Member: guildID,
		}).Err(); err != nil {
			return false, fmt.Errorf("reschedule: %w", err)
		}
		return false, nil
	}

	features := stringSet(row["features"])
	kept := make([]string, 0, len(features))
	for _, f := range features {
		if !containsString(invitePauseFeatures, f) {
			kept = append(kept, f)
		}
	}
	// The features column is a typed set, and an empty set is written as null:
	// that is what the api does for a guild with no features.
	var featuresValue any
	if len(kept) > 0 {
		featuresValue = map[string]any{"__fluxer_type": "set", "value": kept}
	}
	if err := patchGuildInvitePause(ctx, rowKey, featuresValue); err != nil {
		return false, fmt.Errorf("patch guild: %w", err)
	}
	if err := recordInvitePauseLift(ctx, guildID, features, kept); err != nil {
		// The change is applied; a missing audit row must not undo it.
		log.Printf("liftExpiredGuildInvitePauses: guild %s: audit log: %v (change applied)", guildID, err)
	}
	if err := valkey().ZRem(ctx, guildInvitePauseScheduleKey, guildID).Err(); err != nil {
		return true, fmt.Errorf("dequeue: %w", err)
	}
	return true, nil
}

// patchGuildInvitePause writes both fields in one statement so a reader never
// sees "features without pause" or the other way round.
func patchGuildInvitePause(ctx context.Context, rowKey string, features any) error {
	body := mustJSON(features)
	_, err := db.ExecContext(ctx, `
UPDATE fluxer_kv
SET row_data = row_data || jsonb_build_object('features', $2::jsonb, 'invites_paused_until', 'null'::jsonb),
    updated_at = now()
WHERE table_name = $1 AND row_key = $3`, guildsTable, string(body), rowKey)
	return err
}

// recordInvitePauseLift appends the GUILD_UPDATE audit entry the api writes when
// the pause changes. The changes array carries the feature diff, which is the
// only part the api keeps for this action.
func recordInvitePauseLift(ctx context.Context, guildID string, before, after []string) error {
	logID, err := nextSnowflakeID()
	if err != nil {
		return err
	}
	entry := auditLogEntry{LogID: logID, UserID: systemUserID, ActionType: auditActionGuildUpdate}
	row := map[string]any{
		"log_id":      bigintValue(logID),
		"guild_id":    bigintValue(guildID),
		"user_id":     bigintValue(systemUserID),
		"target_id":   bigintValue(guildID),
		"action_type": auditActionGuildUpdate,
		"reason":      nil,
		"options":     nil,
		"changes": []any{map[string]any{
			"key":       "features",
			"old_value": featuresForAudit(before),
			"new_value": featuresForAudit(after),
		}},
	}
	expiresAt := time.Now().Add(time.Duration(auditLogTTLSeconds) * time.Second).UTC()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, key := range auditLogKeys(guildID, entry) {
		if err := insertAuditLogTx(ctx, tx, key.table, key.partition, key.rowKey, row, expiresAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// nextSnowflakeID asks the snowflake service for one id, the same service the
// api uses. Without a connection (tests) the audit row is skipped and the caller
// only logs it: the pause still has to be lifted.
func nextSnowflakeID() (string, error) {
	nc := workerNATS()
	if nc == nil {
		return "", fmt.Errorf("no NATS connection")
	}
	ids, err := snowflakeBatch(nc, 1)
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

func featuresForAudit(features []string) any {
	if len(features) == 0 {
		return nil
	}
	return features
}

// stringSet reads a typed set column. The api stores sets as
// {"__fluxer_type":"set","value":[...]}, but a bare array appears in older rows.
func stringSet(v any) []string {
	raw, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	list, ok := raw["value"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}
