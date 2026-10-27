// e2e for batchGuildAuditLogMessageDeletes, against a live instance.
//
// The task only exists because of a chain that no unit test can cover: a real
// MESSAGE_DELETE goes through the api, the api debounces the series into one
// scheduled job, the worker picks it up, collapses the entries and publishes
// GUILD_AUDIT_LOG_ENTRY_CREATE. This program walks that chain as a bot and
// checks the audit log afterwards, so the answer comes from the instance rather
// than from a scratch table.
//
// It needs a bot token whose account is in the target guild with enough rights
// to create a channel, post, delete and read the audit log:
//
//	FLUXER_BOT_TOKEN   required, the "Bot" credential (id.secret)
//	FLUXER_BASE_URL    default https://fluxer.doesnmlab.xyz
//	FLUXER_E2E_GUILD   default the "1GB RAM Fluxer Testing" guild
//
// The channel is created per run and removed at the end, messages are ours, and
// the audit-log entry that the run produces is the evidence. The program does not
// touch anything else in the guild.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	actionMessageBulkDelete = 73
	waitForWorker           = 4 * time.Minute
	pollEvery               = 5 * time.Second
	messagesToDelete        = 3
)

type client struct {
	base  string
	token string
	http  *http.Client
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (c *client) do(method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bot "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var apiErr apiError
		_ = json.Unmarshal(raw, &apiErr)
		return fmt.Errorf("%s %s: HTTP %d %s %s", method, path, resp.StatusCode, apiErr.Code, apiErr.Message)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("%s %s: decode: %w", method, path, err)
		}
	}
	return nil
}

type channel struct {
	ID string `json:"id"`
}

type message struct {
	ID string `json:"id"`
}

type auditEntry struct {
	ID         string         `json:"id"`
	ActionType int            `json:"action_type"`
	UserID     string         `json:"user_id"`
	TargetID   *string        `json:"target_id"`
	Options    map[string]any `json:"options"`
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	token := os.Getenv("FLUXER_BOT_TOKEN")
	if token == "" {
		return fmt.Errorf("FLUXER_BOT_TOKEN is not set")
	}
	base := strings.TrimSuffix(env("FLUXER_BASE_URL", "https://fluxer.doesnmlab.xyz"), "/")
	guild := env("FLUXER_E2E_GUILD", "1563469298515574784")

	c := &client{base: base, token: token, http: &http.Client{Timeout: 20 * time.Second}}

	var me struct {
		ID       string `json:"id"`
		Username string `json:"username"`
	}
	if err := c.do("GET", "/api/v1/users/@me", nil, &me); err != nil {
		return fmt.Errorf("whoami: %w", err)
	}
	fmt.Printf("bot %s (%s) on %s\n", me.Username, me.ID, base)

	// A fresh channel per run keeps the audit log readable and leaves other
	// channels of the guild untouched.
	name := "e2e-" + time.Now().UTC().Format("20060102-150405")
	var ch channel
	if err := c.do("POST", "/api/v1/guilds/"+guild+"/channels", map[string]any{"name": name, "type": 0}, &ch); err != nil {
		return fmt.Errorf("create channel: %w", err)
	}
	fmt.Printf("channel %s (%s)\n", name, ch.ID)
	defer func() {
		if err := c.do("DELETE", "/api/v1/channels/"+ch.ID, nil, nil); err != nil {
			fmt.Printf("warning: channel %s was not removed: %v\n", ch.ID, err)
			return
		}
		fmt.Printf("channel %s removed\n", ch.ID)
	}()

	ids := make([]string, 0, messagesToDelete)
	for i := 1; i <= messagesToDelete; i++ {
		var m message
		body := map[string]any{"content": fmt.Sprintf("e2e probe %d/%d", i, messagesToDelete)}
		if err := c.do("POST", "/api/v1/channels/"+ch.ID+"/messages", body, &m); err != nil {
			return fmt.Errorf("send message %d: %w", i, err)
		}
		ids = append(ids, m.ID)
	}
	fmt.Printf("sent %d messages\n", len(ids))

	for _, id := range ids {
		if err := c.do("DELETE", "/api/v1/channels/"+ch.ID+"/messages/"+id, nil, nil); err != nil {
			return fmt.Errorf("delete message %s: %w", id, err)
		}
	}
	fmt.Printf("deleted %d messages, waiting for the worker (up to %s)\n", len(ids), waitForWorker)

	// The api schedules the task at (window+2)*30 s, so one minute is the floor;
	// the rest of the budget is for the queue and the worker's own lag.
	deadline := time.Now().Add(waitForWorker)
	for {
		var page struct {
			Entries []auditEntry `json:"audit_log_entries"`
		}
		q := url.Values{}
		q.Set("limit", "50")
		if err := c.do("GET", "/api/v1/guilds/"+guild+"/audit-logs?"+q.Encode(), nil, &page); err != nil {
			return fmt.Errorf("audit log: %w", err)
		}
		for _, e := range page.Entries {
			if e.ActionType != actionMessageBulkDelete {
				continue
			}
			if got, _ := e.Options["channel_id"].(string); got != ch.ID {
				continue
			}
			count, _ := e.Options["count"].(float64)
			if int(count) < len(ids) {
				continue
			}
			fmt.Printf("ok: entry %s is MESSAGE_BULK_DELETE for %s with count %d\n", e.ID, ch.ID, int(count))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no MESSAGE_BULK_DELETE for channel %s within %s: the worker did not collapse the deletions", ch.ID, waitForWorker)
		}
		time.Sleep(pollEvery)
	}
}
