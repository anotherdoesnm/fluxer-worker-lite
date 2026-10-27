package main

// Tests for the attachment-decay reconciliation.
//
// The lifetime curve is the piece worth pinning: a tiny file lives years, a huge
// one weeks, and a file above the plan limit gets no decay at all. The database
// half then checks that reconciling writes the row pair and that a second pass does
// not rewrite what already matches.

import (
	"context"
	"testing"
	"time"
)

func TestComputeDecayExpiry(t *testing.T) {
	rules := decayRules{
		minMB: decayMinMB, maxMB: decayMaxMB, planMB: decayPlanMB,
		minDays: decayMinDays, maxDays: decayMaxDays, curve: decayCurve, pricePerTB: decayPriceTB,
		enabled: true,
	}
	uploaded := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Tiny file: the maximum lifetime.
	expires, ok := computeDecayExpiry(1024, uploaded, rules) // 1 KB
	if !ok {
		t.Fatal("a tiny file got no decay")
	}
	if days := expires.Sub(uploaded).Hours() / 24; days != rules.maxDays {
		t.Fatalf("tiny file lifetime = %.0f days, want %.0f", days, rules.maxDays)
	}

	// At the upper bound the minimum lifetime applies.
	expires, ok = computeDecayExpiry(int64(500*1024*1024), uploaded, rules) // 500 MB
	if !ok {
		t.Fatal("a 500 MB file got no decay")
	}
	if days := expires.Sub(uploaded).Hours() / 24; days != rules.minDays {
		t.Fatalf("500 MB lifetime = %.0f days, want %.0f", days, rules.minDays)
	}

	// Above the plan limit: no decay at all.
	if _, ok := computeDecayExpiry(int64(900*1024*1024), uploaded, rules); ok {
		t.Fatal("a 900 MB file was given a decay row")
	}

	// In between: strictly between the bounds.
	expires, ok = computeDecayExpiry(int64(100*1024*1024), uploaded, rules)
	if !ok {
		t.Fatal("a 100 MB file got no decay")
	}
	days := expires.Sub(uploaded).Hours() / 24
	if days <= rules.minDays || days >= rules.maxDays {
		t.Fatalf("100 MB lifetime = %.0f days, want between %.0f and %.0f", days, rules.minDays, rules.maxDays)
	}
}

func TestReconcileAttachmentDecayAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	const (
		channelID = "990000000000006001"
		messageID = "990000000000006002"
		attachID  = "990000000000006003"
	)
	// The message carries one 1 MB attachment; uploaded "now".
	partition, key, bucket := messageKeys(channelID, messageID)
	row := map[string]any{
		"channel_id": kvBigintJSON(channelID),
		"message_id": kvBigintJSON(messageID),
		"author_id":  kvBigintJSON("990000000000006004"),
		"bucket":     bucket,
		"attachments": []any{map[string]any{
			"attachment_id": kvBigintJSON(attachID),
			"filename":      map[string]any{"__fluxer_type": "string", "value": "image.png"},
			"size":          map[string]any{"__fluxer_type": "bigint", "value": "1048576"},
		}},
	}
	if err := putKV(ctx, messagesTable, partition, key, row, messageID); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	// Decay enabled in the instance config.
	cfg := `{"attachment_decay":{"enabled":true}}`
	if err := putKV(ctx, "instance_configuration", "instance_media_config", "instance_media_config",
		map[string]any{"value": cfg}, ""); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	t.Cleanup(func() {
		for _, table := range []string{messagesTable, "instance_configuration", "attachment_decay_by_id", "attachment_decay_by_expiry"} {
			_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1`, table)
		}
	})

	rules, err := decayRulesFromConfig(ctx)
	if err != nil || !rules.enabled {
		t.Fatalf("rules: enabled=%v err=%v", rules.enabled, err)
	}

	attachments, err := allMessageAttachments(ctx)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(attachments) != 1 {
		t.Fatalf("attachments = %d, want 1", len(attachments))
	}
	if attachments[0].SizeBytes != 1048576 {
		t.Fatalf("size = %d", attachments[0].SizeBytes)
	}
	changed, err := reconcileOneAttachment(ctx, attachments[0], rules)
	if err != nil || !changed {
		t.Fatalf("reconcile: changed=%v err=%v", changed, err)
	}

	rec, err := readDecayRecord(ctx, attachID)
	if err != nil || rec == nil {
		t.Fatalf("decay record missing: %v", err)
	}
	expires, ok := rec.ExpiresAt()
	if !ok {
		t.Fatalf("expires_at unreadable: %q", rec.ExpiresAtRaw)
	}
	if days := expires.Sub(attachments[0].UploadedAt).Hours() / 24; days < rules.minDays || days > rules.maxDays {
		t.Fatalf("lifetime %.0f days is outside the bounds", days)
	}

	// A second pass must not rewrite a matching record.
	changed, err = reconcileOneAttachment(ctx, attachments[0], rules)
	if err != nil || changed {
		t.Fatalf("second pass: changed=%v err=%v", changed, err)
	}
}
