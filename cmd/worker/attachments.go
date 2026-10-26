package main

// expireAttachments: walk the attachment-decay expiry buckets and queue the expired
// objects for deletion.
//
// The queue itself is a valkey list, `asset:deletion:queue`; the actual S3 delete is
// processAssetDeletionQueue's job. Splitting them keeps this task free of any storage
// credentials. Upstream looks back three days of buckets so a missed run is recovered.
//
// Rows live in fluxer_kv, mirroring the Cassandra tables:
//
//	attachment_decay_by_expiry   <expiry_bucket> \x1f <expires_at> \x1f <attachment_id>
//	attachment_decay_by_id       <attachment_id>

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	assetDeletionQueueKey = "asset:deletion:queue"
	attachmentBucketDays  = 4 // today + three days back
	attachmentFetchLimit  = 200
)

func expiryBucket(t time.Time) int64 {
	return int64(t.Year())*10000 + int64(t.Month())*100 + int64(t.Day())
}

// attachmentDecayEnabled reads instance_media_config; decay is only meaningful when the
// instance turned it on, and it is on for this deployment.
func attachmentDecayEnabled(ctx context.Context) (bool, error) {
	var raw string
	err := db.QueryRowContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'instance_configuration' AND row_key LIKE '%instance_media_config%'
LIMIT 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// The stored row keeps its payload as a JSON *string* inside `value` (the api
	// double-encodes config values), so this needs two passes.
	var outer struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &outer); err != nil {
		return false, fmt.Errorf("instance_media_config: %w", err)
	}
	var cfg struct {
		AttachmentDecay struct {
			Enabled bool `json:"enabled"`
		} `json:"attachment_decay"`
	}
	if err := json.Unmarshal([]byte(outer.Value), &cfg); err != nil {
		return false, fmt.Errorf("instance_media_config value: %w", err)
	}
	return cfg.AttachmentDecay.Enabled, nil
}

type expiredAttachment struct {
	RowKey       string
	AttachmentID string
	ChannelID    string
	Filename     string
	ExpiresAt    time.Time
}

func fetchExpired(ctx context.Context, bucket int64, now time.Time) ([]expiredAttachment, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_key, row_data->'attachment_id'->>'value', COALESCE(row_data->'expires_at'->>'value','')
FROM fluxer_kv
WHERE table_name = 'attachment_decay_by_expiry'
  AND row_data->>'expiry_bucket' = $1
  AND (row_data->'expires_at'->>'value')::timestamptz <= $2
LIMIT $3`, fmt.Sprint(bucket), now, attachmentFetchLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []expiredAttachment
	for rows.Next() {
		var e expiredAttachment
		var expires string
		if err := rows.Scan(&e.RowKey, &e.AttachmentID, &expires); err != nil {
			return nil, err
		}
		if expires != "" {
			if t, perr := time.Parse(time.RFC3339Nano, expires); perr == nil {
				e.ExpiresAt = t
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func fetchAttachmentMeta(ctx context.Context, attachmentID string) (*expiredAttachment, error) {
	var raw string
	err := db.QueryRowContext(ctx, `
SELECT row_data::text FROM fluxer_kv
WHERE table_name = 'attachment_decay_by_id'
  AND row_data->'attachment_id'->>'value' = $1
LIMIT 1`, attachmentID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		return nil, err
	}
	meta := &expiredAttachment{
		AttachmentID: attachmentID,
		ChannelID:    typedString(row["channel_id"]),
		Filename:     stringField(row, "filename"),
	}
	if ts := typedString(row["expires_at"]); ts != "" {
		if t, perr := time.Parse(time.RFC3339Nano, ts); perr == nil {
			meta.ExpiresAt = t
		}
	}
	return meta, nil
}

func stringField(m map[string]any, key string) string {
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

func deleteKVRows(ctx context.Context, table string, rowKeys ...string) error {
	for _, k := range rowKeys {
		if _, err := db.ExecContext(ctx,
			`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, table, k); err != nil {
			return err
		}
	}
	return nil
}

func queueAssetDeletion(ctx context.Context, s3Key, cdnURL, reason string) error {
	item := map[string]any{
		"s3Key":    s3Key,
		"cdnUrl":   cdnURL,
		"reason":   reason,
		"queuedAt": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	body, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return valkey().RPush(ctx, assetDeletionQueueKey, string(body)).Err()
}

func taskExpireAttachments(ctx context.Context, _ map[string]any) error {
	if err := openDB(); err != nil {
		return fmt.Errorf("expireAttachments: %w", err)
	}
	enabled, err := attachmentDecayEnabled(ctx)
	if err != nil {
		return fmt.Errorf("expireAttachments: %w", err)
	}
	if !enabled {
		log.Printf("expireAttachments: attachment decay is disabled; nothing to do")
		return nil
	}
	now := time.Now().UTC()
	queued, removed := 0, 0
	for day := 0; day < attachmentBucketDays; day++ {
		bucket := expiryBucket(now.AddDate(0, 0, -day))
		for {
			expired, err := fetchExpired(ctx, bucket, now)
			if err != nil {
				return fmt.Errorf("expireAttachments: fetch bucket %d: %w", bucket, err)
			}
			if len(expired) == 0 {
				break
			}
			for _, e := range expired {
				meta, err := fetchAttachmentMeta(ctx, e.AttachmentID)
				if err != nil {
					return fmt.Errorf("expireAttachments: meta %s: %w", e.AttachmentID, err)
				}
				if meta == nil || (!meta.ExpiresAt.IsZero() && meta.ExpiresAt.After(e.ExpiresAt)) {
					// No metadata, or the expiry was extended after this row was written.
					if err := deleteKVRows(ctx, "attachment_decay_by_expiry", e.RowKey); err != nil {
						return err
					}
					removed++
					continue
				}
				s3Key := fmt.Sprintf("attachments/%s/%s/%s", meta.ChannelID, meta.AttachmentID, meta.Filename)
				cdnURL := env("FLUXER_MEDIA_ENDPOINT", "") + "/" + s3Key
				if err := queueAssetDeletion(ctx, s3Key, cdnURL, "attachment-decay-expired"); err != nil {
					return fmt.Errorf("expireAttachments: queue %s: %w", s3Key, err)
				}
				byIDKey := bigintSeg(meta.AttachmentID)
				if err := deleteKVRows(ctx, "attachment_decay_by_expiry", e.RowKey); err != nil {
					return err
				}
				if err := deleteKVRows(ctx, "attachment_decay_by_id", byIDKey); err != nil {
					return err
				}
				queued++
				removed++
			}
		}
	}
	log.Printf("expireAttachments: queued=%d rows_removed=%d lookback=%dd", queued, removed, attachmentBucketDays)
	return nil
}
