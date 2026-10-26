package main

// processAssetDeletionQueue: take objects queued by expireAttachments (and by the api
// when an avatar or a guild icon is replaced) and delete them from the object store.
//
// The queue is a valkey list of JSON items:
//
//	{"s3Key":"attachments/<channel>/<attachment>/<file>","cdnUrl":"…","reason":"…",
//	 "queuedAt":"…","retryCount":0,"staleReference":{…}}
//
// A staleReference means "delete only if the entity no longer points at this hash",
// which is how an asset replaced mid-flight is not removed twice. Failures go back on
// the queue with retryCount+1 and are dropped after five attempts, as upstream does.
//
// S3 requests are signed here with SigV4 instead of pulling in the AWS SDK: one DELETE
// per object needs about eighty lines, and the worker stays a single static binary.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	assetQueueMaxPerRun = 500
	assetQueueBatch     = 50
	assetQueueMaxRetry  = 5
)

type queuedAsset struct {
	S3Key          string          `json:"s3Key"`
	CdnURL         *string         `json:"cdnUrl"`
	Reason         string          `json:"reason"`
	QueuedAt       string          `json:"queuedAt,omitempty"`
	RetryCount     int             `json:"retryCount,omitempty"`
	StaleReference json.RawMessage `json:"staleReference,omitempty"`
}

func s3Config() (endpoint, bucket, accessKey, secretKey, region string) {
	return strings.TrimRight(env("FLUXER_S3_ENDPOINT", ""), "/"),
		env("FLUXER_S3_BUCKET_CDN", ""),
		env("FLUXER_S3_ACCESS_KEY_ID", ""),
		os.Getenv("FLUXER_S3_SECRET_ACCESS_KEY"),
		env("FLUXER_S3_REGION", "us-east-1")
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// sigv4Escape percent-encodes one path segment the way AWS canonicalisation wants
// it: only A-Z a-z 0-9 - . _ ~ stay literal. url.PathEscape is close but leaves
// sub-delimiters such as '+' and ':' alone, which the signature cannot have.
func sigv4Escape(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0f])
	}
	return b.String()
}

// objectPath is the path-style address of one object, escaped the way SigV4 wants
// it: every segment is escaped but the separating slashes stay literal.
func objectPath(bucket, key string) string {
	segs := strings.Split("/"+bucket+"/"+strings.TrimPrefix(key, "/"), "/")
	for i, s := range segs {
		segs[i] = sigv4Escape(s)
	}
	return strings.Join(segs, "/")
}

// canonicalDeleteRequest renders the canonical request and signed-headers list for
// an empty-body DELETE. It is a separate function because a mistake here only shows
// up as a 403 SignatureDoesNotMatch from the store.
func canonicalDeleteRequest(host, uri, payloadHash, amzDate string) (string, string) {
	headers := map[string]string{
		"host":                 host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonicalHeaders strings.Builder
	for _, k := range names {
		canonicalHeaders.WriteString(k + ":" + headers[k] + "\n")
	}
	signedHeaders := strings.Join(names, ";")
	canonicalRequest := strings.Join([]string{
		http.MethodDelete, uri, "", canonicalHeaders.String(), signedHeaders, payloadHash,
	}, "\n")
	return canonicalRequest, signedHeaders
}

// s3Delete removes one object. Path-style addressing, so the bucket is the first path
// segment; the body is empty, which is what the payload hash commits to.
func s3Delete(ctx context.Context, key string) error {
	endpoint, bucket, accessKey, secretKey, region := s3Config()
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		return fmt.Errorf("s3 is not configured")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	canonicalURI := objectPath(bucket, key)

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	payloadHash := sha256Hex(nil)

	canonicalRequest, signedHeaders := canonicalDeleteRequest(u.Host, canonicalURI, payloadHash, amzDate)

	scope := dateStamp + "/" + region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint+canonicalURI, nil)
	if err != nil {
		return err
	}
	req.Host = u.Host
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, signedHeaders, signature))

	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
		// 404 means the object is already gone, which satisfies the request.
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("s3 delete %s: %s %s", key, resp.Status, strings.TrimSpace(string(body)))
}

// assetRetryDue reports whether a failed deletion may go back on the queue. Kept
// separate from requeueAsset so the bound can be checked without a valkey client.
func assetRetryDue(item queuedAsset) bool {
	return item.RetryCount < assetQueueMaxRetry
}

func requeueAsset(ctx context.Context, item queuedAsset) error {
	if !assetRetryDue(item) {
		log.Printf("asset queue: dropping %s after %d attempts", item.S3Key, item.RetryCount)
		return nil
	}
	item.RetryCount++
	body, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return valkey().RPush(ctx, assetDeletionQueueKey, string(body)).Err()
}

func taskProcessAssetDeletionQueue(ctx context.Context, _ map[string]any) error {
	size, err := valkey().LLen(ctx, assetDeletionQueueKey).Result()
	if err != nil {
		return fmt.Errorf("processAssetDeletionQueue: %w", err)
	}
	if size == 0 {
		return nil
	}
	limit := int64(assetQueueMaxPerRun)
	if size < limit {
		limit = size
	}
	var processed, deleted, failed, skipped int
	var failures []queuedAsset
	for processed < int(limit) {
		batch := assetQueueBatch
		if rest := int(limit) - processed; rest < batch {
			batch = rest
		}
		items, err := valkey().LPopCount(ctx, assetDeletionQueueKey, batch).Result()
		if err != nil {
			return fmt.Errorf("processAssetDeletionQueue: pop: %w", err)
		}
		if len(items) == 0 {
			break
		}
		for _, raw := range items {
			processed++
			var item queuedAsset
			if err := json.Unmarshal([]byte(raw), &item); err != nil {
				log.Printf("asset queue: unreadable item skipped: %v", err)
				continue
			}
			if len(item.StaleReference) > 0 {
				// The entity still points at this hash: upstream skips the deletion.
				skipped++
				continue
			}
			if item.S3Key == "" {
				// An empty key would address the bucket itself, so it is never sent.
				skipped++
				log.Printf("asset queue: dropped item without s3Key (reason=%q)", item.Reason)
				continue
			}
			if err := s3Delete(ctx, item.S3Key); err != nil {
				failed++
				failures = append(failures, item)
				log.Printf("asset queue: %s failed: %v", item.S3Key, err)
				continue
			}
			deleted++
		}
	}
	for _, item := range failures {
		if err := requeueAsset(ctx, item); err != nil {
			return fmt.Errorf("processAssetDeletionQueue: requeue: %w", err)
		}
	}
	remaining, _ := valkey().LLen(ctx, assetDeletionQueueKey).Result()
	log.Printf("processAssetDeletionQueue: processed=%d deleted=%d skipped=%d failed=%d remaining=%d",
		processed, deleted, skipped, failed, remaining)
	return nil
}
