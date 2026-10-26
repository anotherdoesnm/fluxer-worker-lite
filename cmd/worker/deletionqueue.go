package main

// userProcessPendingDeletions: the account-deletion scheduler.
//
// Deleting an account is a two step dance. The api only marks the user (flags
// SELF_DELETED, pending_deletion_at = now + grace) and drops a member into the
// valkey sorted set `deletion_queue`. This handler is the one that decides, minute
// by minute, whose grace has run out, and only then hands the heavy cascade to
// userProcessPendingDeletion.
//
// The queue is a sorted set of `"<userId>|<reasonCode>"` members scored by the
// epoch milliseconds of pending_deletion_at, with a companion string key
// `deletion_queue_by_user:<userId>` holding the member so that a removal does not
// have to guess what the member looked like. Upstream keeps that state in valkey
// because scanning users on every tick is not affordable at its size; at ours it
// would be fine, but the shape is cheap to keep and it is what the api writes to,
// so the two stay interchangeable.
//
// `deletion_queue:state_version` records when the set was last rebuilt from the
// users table: a missing or day-old marker means the set may have drifted (e.g.
// valkey was wiped), and it gets rebuilt from `users` before use. Rebuilds are
// guarded by `deletion_queue:rebuild_lock` so two workers cannot fight.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	deletionQueueKey      = "deletion_queue"
	deletionQueueStateKey = "deletion_queue:state_version"
	deletionQueueLockKey  = "deletion_queue:rebuild_lock"
	deletionQueueByUser   = "deletion_queue_by_user:"

	deletionQueueBatch = 1000
	deletionQueueLock  = 300 * time.Second
	deletionStateAge   = 24 * time.Hour
)

// User flags that matter for deletion, straight from @fluxer/constants.
const (
	userFlagDeleted          uint64 = 1 << 34
	userFlagSelfDeleted      uint64 = 1 << 36
	userFlagAppStoreReviewer uint64 = 1 << 53
)

// DeletionReasons, straight from @fluxer/constants.
const (
	deletionReasonUserRequested = 1
	deletionReasonOther         = 2
)

// userDeletionState is the slice of a users row the scheduler cares about.
type userDeletionState struct {
	UserID    string
	PendingAt time.Time
	StartedAt time.Time
	Flags     uint64
	Bot       bool
	Reason    int // -1 when the column is absent
}

// pendingDeletionDue reports whether the grace period has run out.
func (s *userDeletionState) pendingDeletionDue(now time.Time) bool {
	return !s.PendingAt.IsZero() && !s.PendingAt.After(now)
}

// deletionBlocked mirrors isPendingDeletionBlocked: bots and app-store reviewers
// never get deleted by the automated path.
func (s *userDeletionState) deletionBlocked() bool {
	return s.Bot || s.Flags&userFlagAppStoreReviewer != 0
}

// resolveDeletionReason mirrors resolvePendingDeletionReasonCode: the stored reason
// wins, then the one from the queue member, then whatever the flags imply.
func resolveDeletionReason(s *userDeletionState, queued int) int {
	if s.Reason >= 0 {
		return s.Reason
	}
	if queued != 0 {
		return queued
	}
	if s.Flags&userFlagSelfDeleted != 0 {
		return deletionReasonUserRequested
	}
	if s.Flags&userFlagDeleted != 0 {
		return deletionReasonOther
	}
	return 0
}

// queueMember renders the sorted-set member the api also writes.
func queueMember(userID string, reason int) string {
	return userID + "|" + strconv.Itoa(reason)
}

// parseQueueMember is deliberately strict: the member is the only thing that says
// whose account is about to be destroyed, so anything unexpected is rejected rather
// than guessed at.
func parseQueueMember(value string) (userID string, reason int, err error) {
	if value == "" || len(value) > 19+1+17 {
		return "", 0, fmt.Errorf("member length %d out of range", len(value))
	}
	sep := strings.IndexByte(value, '|')
	if sep <= 0 || sep == len(value)-1 || strings.IndexByte(value[sep+1:], '|') != -1 {
		return "", 0, fmt.Errorf("member %q is not <userId>|<reason>", value)
	}
	userID, payload := value[:sep], value[sep+1:]
	if len(userID) > 19 {
		return "", 0, fmt.Errorf("user id %q is not a snowflake", userID)
	}
	for _, c := range userID {
		if c < '0' || c > '9' {
			return "", 0, fmt.Errorf("user id %q is not a snowflake", userID)
		}
	}
	if len(payload) > 10 {
		return "", 0, fmt.Errorf("reason code %q is too long", payload)
	}
	for _, c := range payload {
		if c < '0' || c > '9' {
			return "", 0, fmt.Errorf("reason code %q is not a number", payload)
		}
	}
	n, convErr := strconv.Atoi(payload)
	if convErr != nil || n < 0 || n > 0x7fffffff {
		return "", 0, fmt.Errorf("reason code %q is out of range", payload)
	}
	return userID, n, nil
}

func parseUserFlags(row map[string]any) uint64 {
	raw := typedString(row["flags"])
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func parseRFC3339(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t
	}
	return time.Time{}
}

// deletionStateFromRow is the single place that knows how a users row maps onto
// userDeletionState, so the queue rebuild and the per-tick lookup cannot drift.
func deletionStateFromRow(row map[string]any) *userDeletionState {
	state := &userDeletionState{
		UserID:    typedString(row["user_id"]),
		PendingAt: parseRFC3339(typedString(row["pending_deletion_at"])),
		StartedAt: parseRFC3339(typedString(row["deletion_started_at"])),
		Flags:     parseUserFlags(row),
		Reason:    -1,
	}
	if bot, ok := row["bot"].(bool); ok {
		state.Bot = bot
	}
	if raw, ok := row["deletion_reason_code"]; ok && raw != nil {
		if n, convErr := strconv.Atoi(typedString(raw)); convErr == nil {
			state.Reason = n
		}
	}
	return state
}

// readUserDeletionState loads the row; a nil result means the user is gone, which
// the caller treats as "drop from the queue".
func readUserDeletionState(ctx context.Context, userID string) (*userDeletionState, error) {
	row, err := readKV(ctx, "users", bigintSeg(userID))
	if err != nil || row == nil {
		return nil, err
	}
	return deletionStateFromRow(row), nil
}

func randomToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// scheduleDeletion replaces any previous entry for the user, the way upstream's
// remove-then-schedule pair does.
func scheduleDeletion(ctx context.Context, userID string, at time.Time, reason int) error {
	member := queueMember(userID, reason)
	secondary := deletionQueueByUser + userID
	if err := valkey().Set(ctx, secondary, member, 0).Err(); err != nil {
		return err
	}
	return valkey().ZAdd(ctx, deletionQueueKey, redis.Z{Score: float64(at.UnixMilli()), Member: member}).Err()
}

func removeFromDeletionQueue(ctx context.Context, userID string) error {
	secondary := deletionQueueByUser + userID
	member, err := valkey().Get(ctx, secondary).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	if err := valkey().ZRem(ctx, deletionQueueKey, member).Err(); err != nil {
		return err
	}
	return valkey().Del(ctx, secondary).Err()
}

// deletionQueueStale reports whether the set has to be rebuilt from the users table.
func deletionQueueStale(ctx context.Context) (bool, error) {
	raw, err := valkey().Get(ctx, deletionQueueStateKey).Result()
	if err == redis.Nil {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	ms, convErr := strconv.ParseInt(raw, 10, 64)
	if convErr != nil {
		return true, nil
	}
	return time.Since(time.UnixMilli(ms)) > deletionStateAge, nil
}

// rebuildDeletionQueue drops the set and repopulates it from `users`. At this
// instance's size one pass is enough; upstream pages a thousand users at a time
// and renews its lock between pages, which matters when the table is millions of
// rows and not here.
func rebuildDeletionQueue(ctx context.Context) (queued int, err error) {
	if err := valkey().Del(ctx, deletionQueueKey, deletionQueueStateKey).Err(); err != nil {
		return 0, err
	}
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'users'
  AND row_data->'pending_deletion_at'->>'value' IS NOT NULL`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return queued, err
		}
		state, err := userDeletionStateFromJSON(raw)
		if err != nil || state.PendingAt.IsZero() {
			continue
		}
		if state.StartedAt.IsZero() && state.deletionBlocked() {
			continue
		}
		if err := scheduleDeletion(ctx, state.UserID, state.PendingAt, resolveDeletionReason(state, 0)); err != nil {
			return queued, err
		}
		queued++
	}
	if err := rows.Err(); err != nil {
		return queued, err
	}
	return queued, valkey().Set(ctx, deletionQueueStateKey,
		strconv.FormatInt(time.Now().UnixMilli(), 10), 0).Err()
}

// userDeletionStateFromJSON decodes a raw row_data document for the rebuild pass.
func userDeletionStateFromJSON(raw string) (*userDeletionState, error) {
	var row map[string]any
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		return nil, err
	}
	return deletionStateFromRow(row), nil
}

// ensureDeletionQueue rebuilds the set when it is stale. A false result means
// another worker holds the rebuild lock, so this tick has nothing to do; upstream
// behaviour, and the reason the lock exists.
func ensureDeletionQueue(ctx context.Context) (bool, error) {
	stale, err := deletionQueueStale(ctx)
	if err != nil || !stale {
		return err == nil, err
	}
	token, err := randomToken()
	if err != nil {
		return false, err
	}
	ok, err := valkey().SetNX(ctx, deletionQueueLockKey, token, deletionQueueLock).Result()
	if err != nil {
		return false, err
	}
	if !ok {
		log.Printf("userProcessPendingDeletions: another worker is rebuilding the deletion queue")
		return false, nil
	}
	defer func() {
		if err := releaseLockScript.Run(ctx, valkey(), []string{deletionQueueLockKey}, token).Err(); err != nil {
			log.Printf("userProcessPendingDeletions: release rebuild lock: %v", err)
		}
	}()
	queued, err := rebuildDeletionQueue(ctx)
	if err != nil {
		return false, fmt.Errorf("rebuild deletion queue: %w", err)
	}
	log.Printf("userProcessPendingDeletions: rebuilt deletion queue from users: queued=%d", queued)
	return true, nil
}

func taskUserProcessPendingDeletions(ctx context.Context, _ map[string]any) error {
	if err := openDB(); err != nil {
		return fmt.Errorf("userProcessPendingDeletions: %w", err)
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return fmt.Errorf("userProcessPendingDeletions: jetstream: %w", err)
	}
	ready, err := ensureDeletionQueue(ctx)
	if err != nil {
		return fmt.Errorf("userProcessPendingDeletions: %w", err)
	}
	if !ready {
		return nil
	}
	now := time.Now().UTC()
	members, err := valkey().ZRangeByScore(ctx, deletionQueueKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Offset: 0, Count: deletionQueueBatch,
	}).Result()
	if err != nil {
		return fmt.Errorf("userProcessPendingDeletions: read queue: %w", err)
	}
	if len(members) == 0 {
		return nil
	}
	var scheduled, dropped, postponed, bad int
	for _, member := range members {
		userID, reason, parseErr := parseQueueMember(member)
		if parseErr != nil {
			bad++
			log.Printf("userProcessPendingDeletions: dropping unreadable member %q: %v", member, parseErr)
			_ = valkey().ZRem(ctx, deletionQueueKey, member).Err()
			continue
		}
		state, err := readUserDeletionState(ctx, userID)
		if err != nil {
			// One unreadable row must not stall everybody else's deletion.
			log.Printf("userProcessPendingDeletions: %s: read user: %v", userID, err)
			continue
		}
		if state == nil || state.PendingAt.IsZero() {
			dropped++
			_ = removeFromDeletionQueue(ctx, userID)
			continue
		}
		if state.StartedAt.IsZero() && state.deletionBlocked() {
			dropped++
			_ = removeFromDeletionQueue(ctx, userID)
			continue
		}
		resolved := resolveDeletionReason(state, reason)
		if !state.pendingDeletionDue(now) {
			postponed++
			if err := scheduleDeletion(ctx, userID, state.PendingAt, resolved); err != nil {
				log.Printf("userProcessPendingDeletions: %s: postpone: %v", userID, err)
			}
			continue
		}
		payload := map[string]any{
			"userId":             userID,
			"deletionReasonCode": resolved,
			"pendingDeletionAt":  state.PendingAt.Format(time.RFC3339Nano),
		}
		if err := publishJob(js, jobPrefixForPublish(), "userProcessPendingDeletion", payload, time.Time{}); err != nil {
			log.Printf("userProcessPendingDeletions: %s: enqueue: %v", userID, err)
			continue
		}
		if err := removeFromDeletionQueue(ctx, userID); err != nil {
			log.Printf("userProcessPendingDeletions: %s: dequeue after enqueue: %v", userID, err)
		}
		scheduled++
	}
	log.Printf("userProcessPendingDeletions: ready=%d scheduled=%d postponed=%d dropped=%d unreadable=%d",
		len(members), scheduled, postponed, dropped, bad)
	return nil
}
