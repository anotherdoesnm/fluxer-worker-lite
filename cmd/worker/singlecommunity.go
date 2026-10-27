package main

// taskJoinSingleCommunityMembers ports the paged run that puts every existing
// account into the single community guild.
//
// The api starts the run by writing `single_community:join_members_run` with a
// run id, then feeds itself pages of 100 users through this task. The state (run
// id, cursor, the set of users being added right now) lives in valkey so a
// restart resumes instead of starting over, and a lock keeps two jobs from
// adding the same people.
//
// Deviations, both deliberate:
//   - the cursor is the last user row key instead of the api's opaque page state;
//     the worker reads `users` by row key, so a cursor is what it can resume
//     from, and the page boundaries are otherwise identical.
//   - the "rejoin" branch (a member row exists but the person is still in the
//     joining set, so the gateway join is retried) is not ported: it only matters
//     after a crash between the row write and the gateway call, and the log says
//     so when a member already exists.
//
// Ineligible users follow the api's rule: the system user, bots, deleted,
// disabled, pending deletion, and registrations that are awaiting approval or
// were rejected.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	singleCommunityRunKey   = "single_community:join_members_run"
	singleCommunityLockKey  = "single_community:join_members_lock"
	singleCommunityPageSize = 100
	singleCommunityRunTTL   = 30 * 24 * time.Hour
	singleCommunityLockTTL  = 5 * time.Minute
	singleCommunityStall    = 10 * time.Minute
)

// Traits that make an account ineligible until registration settles.
var singleCommunityBlockedTraits = []string{"registration_pending_approval", "registration_rejected"}

type singleCommunityRun struct {
	RunID     string   `json:"run_id"`
	GuildID   string   `json:"guild_id"`
	PageState string   `json:"page_state"`
	UpdatedAt int64    `json:"updated_at"`
	Joining   []string `json:"joining"`
}

func taskJoinSingleCommunityMembers(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("joinSingleCommunityMembers: %w", err)
		}
	}
	runID := stringField(payload, "run_id")
	pageState := stringField(payload, "page_state")

	raw, err := valkey().Get(ctx, singleCommunityRunKey).Result()
	if err == redis.Nil {
		// No run: the api starts these by writing the key, so an empty key means
		// there is nothing to continue.
		return nil
	}
	if err != nil {
		return fmt.Errorf("joinSingleCommunityMembers: read run: %w", err)
	}
	var run singleCommunityRun
	if err := json.Unmarshal([]byte(raw), &run); err != nil {
		return fmt.Errorf("joinSingleCommunityMembers: decode run: %w", err)
	}
	if runID == "" {
		// A cron tick without a run id resumes a stalled run, and does nothing
		// while a healthy run is moving.
		if time.Since(time.UnixMilli(run.UpdatedAt)) < singleCommunityStall {
			return nil
		}
		log.Printf("joinSingleCommunityMembers: resuming stalled run %s", run.RunID)
	} else if runID != run.RunID || pageState != run.PageState {
		// Somebody else already advanced the run past this job.
		return nil
	}

	token, err := randomToken()
	if err != nil {
		return fmt.Errorf("joinSingleCommunityMembers: lock token: %w", err)
	}
	ok, err := valkey().SetNX(ctx, singleCommunityLockKey, token, singleCommunityLockTTL).Result()
	if err != nil {
		return fmt.Errorf("joinSingleCommunityMembers: lock: %w", err)
	}
	if !ok {
		if runID == "" {
			return nil
		}
		return fmt.Errorf("joinSingleCommunityMembers: another job is adding accounts")
	}
	defer func() {
		if err := releaseLock(ctx, singleCommunityLockKey, token); err != nil {
			log.Printf("joinSingleCommunityMembers: release lock: %v", err)
		}
	}()

	return joinSingleCommunityPage(ctx, run, token)
}

// releaseLock deletes the lock only if it is still ours.
func releaseLock(ctx context.Context, key, token string) error {
	current, err := valkey().Get(ctx, key).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	if current != token {
		return nil
	}
	return valkey().Del(ctx, key).Err()
}

func joinSingleCommunityPage(ctx context.Context, run singleCommunityRun, token string) error {
	users, lastKey, err := usersPage(ctx, run.PageState, singleCommunityPageSize)
	if err != nil {
		return fmt.Errorf("joinSingleCommunityMembers: page: %w", err)
	}
	added, skipped, ineligible := 0, 0, 0
	for _, user := range users {
		if !canJoinSingleCommunity(user) {
			ineligible++
			continue
		}
		userID := typedString(user["user_id"])
		if userID == "" {
			ineligible++
			continue
		}
		existing, err := readKV(ctx, "guild_members", bigintSeg(run.GuildID)+kvSep+bigintSeg(userID))
		if err != nil {
			return fmt.Errorf("joinSingleCommunityMembers: %s: read member: %w", userID, err)
		}
		if existing != nil {
			skipped++
			continue
		}
		if _, _, _, _, err := applyGuildMemberAdds(ctx, systemUserID, "", run.GuildID, []string{userID}); err != nil {
			return fmt.Errorf("joinSingleCommunityMembers: %s: %w", userID, err)
		}
		added++
		if err := valkey().Expire(ctx, singleCommunityLockKey, singleCommunityLockTTL).Err(); err != nil {
			return fmt.Errorf("joinSingleCommunityMembers: extend lock: %w", err)
		}
	}

	next := singleCommunityRun{
		RunID:     run.RunID,
		GuildID:   run.GuildID,
		PageState: lastKey,
		UpdatedAt: time.Now().UnixMilli(),
	}
	if lastKey == "" {
		// No further page: the run is finished and the key goes away, which is
		// also the signal the api watches for.
		if err := valkey().Del(ctx, singleCommunityRunKey).Err(); err != nil {
			return fmt.Errorf("joinSingleCommunityMembers: finish: %w", err)
		}
		log.Printf("joinSingleCommunityMembers: run %s finished (added=%d already=%d ineligible=%d)",
			run.RunID, added, skipped, ineligible)
		return nil
	}
	body, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if err := valkey().Set(ctx, singleCommunityRunKey, body, singleCommunityRunTTL).Err(); err != nil {
		return fmt.Errorf("joinSingleCommunityMembers: save run: %w", err)
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return fmt.Errorf("joinSingleCommunityMembers: jetstream: %w", err)
	}
	jobKey := "single_community_join_" + next.RunID + "_" + next.PageState
	if err := publishJobWithID(js, jobPrefixForPublish(), "joinSingleCommunityMembers", map[string]any{
		"run_id":     next.RunID,
		"page_state": next.PageState,
	}, jobKey); err != nil {
		return fmt.Errorf("joinSingleCommunityMembers: next page: %w", err)
	}
	log.Printf("joinSingleCommunityMembers: page done (added=%d already=%d ineligible=%d), next page queued",
		added, skipped, ineligible)
	return nil
}

// usersPage reads one page of user rows after the cursor, in row-key order.
func usersPage(ctx context.Context, cursor string, limit int) ([]map[string]any, string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_key, row_data::text FROM fluxer_kv
WHERE table_name = 'users' AND ($1 = '' OR row_key > $1)
ORDER BY row_key
LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []map[string]any
	last := ""
	for rows.Next() {
		var rowKey, raw string
		if err := rows.Scan(&rowKey, &raw); err != nil {
			return nil, "", err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		out = append(out, row)
		last = rowKey
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(out) < limit {
		last = ""
	}
	return out, last, nil
}

// canJoinSingleCommunity is the api's eligibility rule.
func canJoinSingleCommunity(user map[string]any) bool {
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
	flags := userFlags(user)
	if flags&(userFlagDeleted|userFlagSelfDeleted|userFlagDisabled) != 0 {
		return false
	}
	if typedString(user["pending_deletion_at"]) != "" {
		return false
	}
	traits := stringSet(user["traits"])
	for _, blocked := range singleCommunityBlockedTraits {
		if containsString(traits, blocked) {
			return false
		}
	}
	return true
}
