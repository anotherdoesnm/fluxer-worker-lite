package main

// taskReconcileAttachmentDecay ports ReconcileAttachmentDecay.ts.
//
// Attachment decay gives each uploaded file a lifetime that depends on its size:
// small files live for years, big ones for weeks, with a linear/log blend in
// between. The api writes those decay rows when a message is sent; this task is
// the repair path for when the instance settings changed (decay switched on, or
// the size/lifetime curve edited) and the existing rows no longer match.
//
// For every attachment in a message it recomputes the decay from the current
// settings, deletes the decay rows of an attachment that is no longer eligible,
// and writes the rows when they are missing or stale.
//
// Deviations, both deliberate:
//   - the api chunks the sweep into jobs with a cursor and a run id; this worker
//     does the whole sweep in one job. At this instance's size that is seconds, and
//     a partial run would need the run bookkeeping the api keeps in valkey.
//   - the api checks that the object still exists in storage before writing a decay
//     row; there is no object-metadata call in this worker, so a row may be written
//     for a file that is already gone. The consequence is bounded: expireAttachments
//     later queues a delete for a missing object and logs it.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"
)

// Defaults from @fluxer/constants (DEFAULT_DECAY_CONSTANTS).
const (
	decayMinMB   = 5.0
	decayMaxMB   = 500.0
	decayPlanMB  = 500.0
	decayMinDays = 14.0
	decayMaxDays = 365.0 * 3
	decayCurve   = 0.5
	decayPriceTB = 811.03
)

type decayRules struct {
	minMB, maxMB, planMB, minDays, maxDays, curve, pricePerTB float64
	enabled                                                   bool
}

func taskReconcileAttachmentDecay(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("reconcileAttachmentDecay: %w", err)
		}
	}
	rules, err := decayRulesFromConfig(ctx)
	if err != nil {
		return fmt.Errorf("reconcileAttachmentDecay: %w", err)
	}
	if !rules.enabled {
		log.Printf("reconcileAttachmentDecay: attachment decay is disabled; nothing to reconcile")
		return nil
	}

	attachments, err := allMessageAttachments(ctx)
	if err != nil {
		return fmt.Errorf("reconcileAttachmentDecay: %w", err)
	}
	changed := 0
	for _, a := range attachments {
		ok, err := reconcileOneAttachment(ctx, a, rules)
		if err != nil {
			return fmt.Errorf("reconcileAttachmentDecay: %s: %w", a.AttachmentID, err)
		}
		if ok {
			changed++
		}
	}
	log.Printf("reconcileAttachmentDecay: attachments=%d changed=%d", len(attachments), changed)
	return nil
}

// decayAttachment is one attachment as found in a message row.
type decayAttachment struct {
	AttachmentID string
	ChannelID    string
	MessageID    string
	Filename     string
	SizeBytes    int64
	UploadedAt   time.Time
}

func allMessageAttachments(ctx context.Context) ([]decayAttachment, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'messages' AND row_data->'attachments' IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []decayAttachment
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			continue
		}
		channelID := typedString(row["channel_id"])
		messageID := typedString(row["message_id"])
		uploaded := time.Time{}
		if id, err := parseInt64(messageID); err == nil {
			uploaded = snowflakeTime(id)
		}
		for _, a := range messageAttachments(row) {
			size, _ := intFieldChecked(map[string]any{"v": rowAttachmentSize(row, a.AttachmentID)}, "v")
			out = append(out, decayAttachment{
				AttachmentID: a.AttachmentID,
				ChannelID:    channelID,
				MessageID:    messageID,
				Filename:     a.Filename,
				SizeBytes:    int64(size),
				UploadedAt:   uploaded,
			})
		}
	}
	return out, rows.Err()
}

// rowAttachmentSize digs the size out of the attachment object in the message row.
func rowAttachmentSize(row map[string]any, attachmentID string) any {
	list, ok := row["attachments"].([]any)
	if !ok {
		return nil
	}
	for _, item := range list {
		obj, ok := item.(map[string]any)
		if !ok || typedString(obj["attachment_id"]) != attachmentID {
			continue
		}
		return obj["size"]
	}
	return nil
}

// reconcileOneAttachment returns whether it wrote or removed anything.
func reconcileOneAttachment(ctx context.Context, a decayAttachment, rules decayRules) (bool, error) {
	existing, err := readDecayRecord(ctx, a.AttachmentID)
	if err != nil {
		return false, err
	}
	expiresAt, ok := computeDecayExpiry(a.SizeBytes, a.UploadedAt, rules)
	if !ok {
		if existing == nil {
			return false, nil
		}
		// No longer eligible: the decay rows go, the file stays.
		if err := deleteDecayRows(ctx, existing); err != nil {
			return false, err
		}
		return true, nil
	}
	if existing != nil {
		if later, known := existing.ExpiresAt(); known && !later.Before(expiresAt) {
			// The stored expiry is already at or beyond what the settings would give:
			// a renewal must not be shortened.
			return false, nil
		}
	}
	if err := writeDecayRecord(ctx, a, expiresAt, rules); err != nil {
		return false, err
	}
	return true, nil
}

// decayRecord is the row pair a decayed attachment owns.
type decayRecord struct {
	ByIDKey      string
	ByExpiryKey  string
	ExpiresAtRaw string
}

func (r *decayRecord) ExpiresAt() (time.Time, bool) {
	ms, ok := parseTimestampMs(r.ExpiresAtRaw)
	if !ok {
		return time.Time{}, false
	}
	return time.UnixMilli(ms).UTC(), true
}

func readDecayRecord(ctx context.Context, attachmentID string) (*decayRecord, error) {
	row, err := readKV(ctx, "attachment_decay_by_id", bigintSeg(attachmentID))
	if err != nil || row == nil {
		return nil, err
	}
	rec := &decayRecord{
		ByIDKey:      bigintSeg(attachmentID),
		ExpiresAtRaw: typedString(row["expires_at"]),
	}
	// The expiry row is found by its columns, the same way expireAttachments does.
	var byExpiryKey string
	err = db.QueryRowContext(ctx, `
SELECT row_key FROM fluxer_kv
WHERE table_name = 'attachment_decay_by_expiry'
  AND row_data->'attachment_id'->>'value' = $1
LIMIT 1`, attachmentID).Scan(&byExpiryKey)
	if err == nil {
		rec.ByExpiryKey = byExpiryKey
	}
	return rec, nil
}

func deleteDecayRows(ctx context.Context, rec *decayRecord) error {
	if rec.ByIDKey != "" {
		if err := deleteKVRows(ctx, "attachment_decay_by_id", rec.ByIDKey); err != nil {
			return err
		}
	}
	if rec.ByExpiryKey != "" {
		if err := deleteKVRows(ctx, "attachment_decay_by_expiry", rec.ByExpiryKey); err != nil {
			return err
		}
	}
	return nil
}

func writeDecayRecord(ctx context.Context, a decayAttachment, expiresAt time.Time, rules decayRules) error {
	lifetimeDays := int(math.Round(expiresAt.Sub(a.UploadedAt).Hours() / 24))
	cost := decayCost(float64(a.SizeBytes), float64(lifetimeDays), rules.pricePerTB)
	bucket := expiryBucket(expiresAt)
	now := time.Now().UTC()

	byIDKey := bigintSeg(a.AttachmentID)
	row := map[string]any{
		"attachment_id":    bigintValue(a.AttachmentID),
		"channel_id":       bigintValue(a.ChannelID),
		"message_id":       bigintValue(a.MessageID),
		"filename":         a.Filename,
		"size_bytes":       bigintValue(fmt.Sprintf("%d", a.SizeBytes)),
		"uploaded_at":      kvDate(a.UploadedAt),
		"expires_at":       kvDate(expiresAt),
		"last_accessed_at": kvDate(now),
		"cost":             cost,
		"lifetime_days":    lifetimeDays,
		"status":           nil,
		"expiry_bucket":    bucket,
	}
	if err := putKV(ctx, "attachment_decay_by_id", byIDKey, byIDKey, row, ""); err != nil {
		return err
	}
	byExpiryKey := fmt.Sprintf("%d%s%s%s%s", bucket, kvSep, expiresAt.Format(time.RFC3339Nano), kvSep, bigintSeg(a.AttachmentID))
	return putKV(ctx, "attachment_decay_by_expiry", byExpiryKey, byExpiryKey, row, "")
}

// computeDecayExpiry is computeDecay from the api: no decay for files above the
// plan limit, otherwise a lifetime between the two bounds with a linear/log blend
// in the middle.
func computeDecayExpiry(sizeBytes int64, uploadedAt time.Time, rules decayRules) (time.Time, bool) {
	sizeMB := float64(sizeBytes) / (1024 * 1024)
	if sizeMB > rules.planMB {
		return time.Time{}, false
	}
	var lifetimeDays float64
	switch {
	case sizeMB <= rules.minMB:
		lifetimeDays = rules.maxDays
	case sizeMB >= rules.maxMB:
		lifetimeDays = rules.minDays
	default:
		linearFrac := (sizeMB - rules.minMB) / (rules.maxMB - rules.minMB)
		logFrac := math.Log(sizeMB/rules.minMB) / math.Log(rules.maxMB/rules.minMB)
		blend := (1-rules.curve)*linearFrac + rules.curve*logFrac
		lifetimeDays = rules.maxDays - blend*(rules.maxDays-rules.minDays)
	}
	return uploadedAt.Add(time.Duration(lifetimeDays * 24 * float64(time.Hour))).UTC(), true
}

// decayCost is computeCost: terabytes-months times the price per TB-month.
func decayCost(sizeBytes, lifetimeDays, pricePerTB float64) float64 {
	terabytes := sizeBytes / (1024 * 1024 * 1024 * 1024)
	months := lifetimeDays / 30
	return terabytes * months * pricePerTB
}

// decayRulesFromConfig reads the instance media config; missing fields fall back
// to the api's defaults.
func decayRulesFromConfig(ctx context.Context) (decayRules, error) {
	rules := decayRules{
		minMB: decayMinMB, maxMB: decayMaxMB, planMB: decayPlanMB,
		minDays: decayMinDays, maxDays: decayMaxDays, curve: decayCurve, pricePerTB: decayPriceTB,
	}
	var raw string
	err := db.QueryRowContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'instance_configuration' AND row_key LIKE '%instance_media_config%'
LIMIT 1`).Scan(&raw)
	if err != nil {
		// No config row: decay is off by default in the api as well.
		return rules, nil
	}
	var outer struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &outer); err != nil {
		return rules, fmt.Errorf("instance_media_config: %w", err)
	}
	var cfg struct {
		AttachmentDecay struct {
			Enabled        bool    `json:"enabled"`
			MinSizeMB      float64 `json:"min_size_mb"`
			MaxSizeMB      float64 `json:"max_size_mb"`
			MaxEligibleMB  float64 `json:"max_eligible_size_mb"`
			MinLifetimeDay float64 `json:"min_lifetime_days"`
			MaxLifetimeDay float64 `json:"max_lifetime_days"`
			Curve          float64 `json:"curve"`
		} `json:"attachment_decay"`
	}
	if err := json.Unmarshal([]byte(outer.Value), &cfg); err != nil {
		return rules, fmt.Errorf("instance_media_config value: %w", err)
	}
	rules.enabled = cfg.AttachmentDecay.Enabled
	if cfg.AttachmentDecay.MinSizeMB > 0 {
		rules.minMB = cfg.AttachmentDecay.MinSizeMB
	}
	if cfg.AttachmentDecay.MaxSizeMB > 0 {
		rules.maxMB = cfg.AttachmentDecay.MaxSizeMB
	}
	if cfg.AttachmentDecay.MaxEligibleMB > 0 {
		rules.planMB = cfg.AttachmentDecay.MaxEligibleMB
	}
	if cfg.AttachmentDecay.MinLifetimeDay > 0 {
		rules.minDays = cfg.AttachmentDecay.MinLifetimeDay
	}
	if cfg.AttachmentDecay.MaxLifetimeDay > 0 {
		rules.maxDays = cfg.AttachmentDecay.MaxLifetimeDay
	}
	if cfg.AttachmentDecay.Curve > 0 {
		rules.curve = cfg.AttachmentDecay.Curve
	}
	return rules, nil
}
