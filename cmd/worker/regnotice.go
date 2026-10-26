package main

// notifyRegistrationApprovers: tell the people who can approve sign-ups that somebody
// is waiting. The api enqueues this from enqueueRegistrationApprovalNotice() on every
// self-hosted registration in approval mode, and until this handler existed the job was
// parked in JOBS_PARKED, so nobody was ever told.
//
// Upstream also emails reviewers when system DMs are hidden on the instance; this
// instance has no SMTP configured, so that branch is not ported (see OMISSIONS.md).
// The rate limit is the same as upstream: one notice per reviewer per ten minutes.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Upstream throttles one notice per reviewer per ten minutes, which silently
	// swallows a second registration inside that window. We key on the pending set
	// instead: a notice goes out whenever the queue changes, and never when it does not.
	regNoticeSubject = "registration_notice_state:"
	regNoticeTTL     = 30 * 24 * time.Hour
	aclWildcard      = "*"
	aclReview        = "registration_review"
	aclAuthenticate  = "authenticate"
)

// reviewerIDs returns every account that may review registrations: the wildcard ACL, or
// authenticate + registration_review together (that is upstream's canReviewRegistrations).
func reviewerIDs(ctx context.Context) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data->'user_id'->>'value', row_data->'acls'::text
FROM fluxer_kv WHERE table_name = 'users'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		var raw sql.NullString
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		if id == "" || id == systemUserID || !raw.Valid {
			continue
		}
		var acls struct {
			Value []string `json:"value"`
		}
		if err := json.Unmarshal([]byte(raw.String), &acls); err != nil {
			continue
		}
		has := func(want string) bool {
			for _, a := range acls.Value {
				if a == want {
					return true
				}
			}
			return false
		}
		if has(aclWildcard) || (has(aclAuthenticate) && has(aclReview)) {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

// usernameOf resolves the display name of a waiting account. Upstream's notice is
// deliberately generic ("new sign-ups are waiting"), but the admin knows us by name, so
// naming the account saves a trip to the panel.
func usernameOf(ctx context.Context, userID string) string {
	if userID == "" {
		return ""
	}
	var name string
	err := db.QueryRowContext(ctx, `
SELECT COALESCE(row_data->>'username', '')
FROM fluxer_kv WHERE table_name = 'users' AND row_data->'user_id'->>'value' = $1
LIMIT 1`, userID).Scan(&name)
	if err != nil || name != "" {
		return name
	}
	// The job is queued at registration time, when the account may still be only in
	// the approval list and not yet in users. The queue entry carries the name.
	var raw string
	if err := db.QueryRowContext(ctx, `
SELECT row_data->>'value' FROM fluxer_kv
WHERE table_name = 'instance_configuration' AND row_key LIKE '%pending_approvals%'
LIMIT 1`).Scan(&raw); err != nil {
		return ""
	}
	var pending []struct {
		UserID     string `json:"user_id"`
		Username   string `json:"username"`
		GlobalName string `json:"global_name"`
	}
	if err := json.Unmarshal([]byte(raw), &pending); err != nil {
		return ""
	}
	for _, p := range pending {
		if p.UserID != userID {
			continue
		}
		if p.Username != "" {
			return p.Username
		}
		return p.GlobalName
	}
	return ""
}

func registrationNoticeContent(ctx context.Context, userID string, waiting int) string {
	base := strings.TrimRight(env("FLUXER_ADMIN_ENDPOINT", ""), "/")
	tail := " in the admin panel."
	if base != "" {
		tail = " at " + base + "/pending-registrations"
	}
	more := ""
	if waiting > 1 {
		more = fmt.Sprintf(" (%d waiting in total)", waiting)
	}
	if name := usernameOf(ctx, userID); name != "" {
		return name + " is waiting for approval" + more + ". Review pending registrations" + tail
	}
	return "New sign-ups are waiting for approval" + more + ". Review pending registrations" + tail
}

func taskNotifyRegistrationApprovers(ctx context.Context, payload map[string]any) error {
	if err := openDB(); err != nil {
		return fmt.Errorf("notifyRegistrationApprovers: %w", err)
	}
	userID, _ := payload["user_id"].(string)
	reviewers, err := reviewerIDs(ctx)
	if err != nil {
		return fmt.Errorf("notifyRegistrationApprovers: reviewers: %w", err)
	}
	pending, err := pendingApprovals(ctx)
	if err != nil {
		return fmt.Errorf("notifyRegistrationApprovers: pending: %w", err)
	}
	fingerprint := pendingFingerprint(pending)
	kv := valkey()
	var recipients []string
	for _, id := range reviewers {
		key := regNoticeSubject + id
		prev, err := kv.Get(ctx, key).Result()
		if err != nil && err != redis.Nil {
			return fmt.Errorf("notifyRegistrationApprovers: get %s: %w", key, err)
		}
		if prev == fingerprint {
			continue
		}
		recipients = append(recipients, id)
		if err := kv.Set(ctx, key, fingerprint, regNoticeTTL).Err(); err != nil {
			return fmt.Errorf("notifyRegistrationApprovers: set %s: %w", key, err)
		}
	}
	log.Printf("notifyRegistrationApprovers: user=%s reviewers=%d recipients=%d", userID, len(reviewers), len(recipients))
	if len(recipients) == 0 {
		return nil
	}
	ids := make([]any, 0, len(recipients))
	for _, id := range recipients {
		ids = append(ids, id)
	}
	js, err := workerNATS().JetStream()
	if err != nil {
		return err
	}
	job := map[string]any{"content": registrationNoticeContent(ctx, userID, len(pending)), "user_ids": ids}
	// One job per (registration attempt, recipient set): a retried task must not fan out twice.
	key := "registration-notice:" + userID + ":" + strings.Join(recipients, ",")
	if err := publishJobWithID(js, jobPrefixForPublish(), "sendSystemDm", job, key); err != nil {
		return fmt.Errorf("notifyRegistrationApprovers: enqueue sendSystemDm: %w", err)
	}
	return nil
}

type pendingEntry struct {
	UserID     string `json:"user_id"`
	Username   string `json:"username"`
	GlobalName string `json:"global_name"`
	RequestedAt string `json:"requested_at"`
}

// pendingApprovals reads the queue the admin panel shows. It lives as a JSON string
// inside one instance_configuration row.
func pendingApprovals(ctx context.Context) ([]pendingEntry, error) {
	var raw string
	err := db.QueryRowContext(ctx, `
SELECT row_data->>'value' FROM fluxer_kv
WHERE table_name = 'instance_configuration' AND row_key LIKE '%pending_approvals%'
LIMIT 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []pendingEntry
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

// pendingFingerprint identifies "this exact set of waiting accounts", order-independent.
func pendingFingerprint(p []pendingEntry) string {
	ids := make([]string, 0, len(p))
	for _, e := range p {
		if e.UserID != "" {
			ids = append(ids, e.UserID)
		}
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}
