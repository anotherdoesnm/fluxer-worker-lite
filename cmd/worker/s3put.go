package main

// s3Put uploads one object with a SigV4 signature, path-style addressing.
//
// The delete path already signs requests (assetqueue.go); this is its counterpart
// for writes, and it exists because the data exports need to put a zip into the
// bucket. Rather than duplicate the signing dance, the canonical request builder is
// shared by both.
//
// Content-Type is sent but not signed: S3 accepts unsigned headers, and keeping the
// signed set to host + payload hash + date means the same helper works whether the
// store is Ceph, MinIO or S3 itself.

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// canonicalRequest renders the canonical request and the signed-header list for a
// request with a body. The method is part of the canonical request, so the same
// builder serves PUT and DELETE.
func canonicalRequest(method, uri, payloadHash, amzDate string, extraHeaders map[string]string) (string, string) {
	headers := map[string]string{
		"host":                 "", // filled by the caller's host
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	delete(headers, "host")
	for k, v := range extraHeaders {
		headers[k] = v
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
	request := strings.Join([]string{
		method, uri, "", canonicalHeaders.String(), signedHeaders, payloadHash,
	}, "\n")
	return request, signedHeaders
}

// s3Put writes one object. It is deliberately strict: a store that refuses the
// write must produce an error, because every caller (data exports, archives) treats
// the upload as the point of no return.
func s3Put(ctx context.Context, key string, body []byte, contentType string) error {
	endpoint, bucket, accessKey, secretKey, region := s3Config()
	return s3PutTo(ctx, endpoint, bucket, accessKey, secretKey, region, key, body, contentType)
}

// s3PutTo is the same write into an explicitly chosen bucket; the exports live in a
// bucket of their own, not in the CDN one.
func s3PutTo(ctx context.Context, endpoint, bucket, accessKey, secretKey, region, key string, body []byte, contentType string) error {
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
	payloadHash := sha256Hex(body)

	canonical, signedHeaders := canonicalRequest(http.MethodPut, canonicalURI, payloadHash, amzDate, map[string]string{
		"host": u.Host,
	})
	scope := dateStamp + "/" + region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, sha256Hex([]byte(canonical)),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(signingKey(secretKey, dateStamp, region), stringToSign))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint+canonicalURI, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Host = u.Host
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, signedHeaders, signature))

	resp, err := (&http.Client{Timeout: 120 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("s3 put %s: %s %s", key, resp.Status, strings.TrimSpace(string(detail)))
}

// signingKey derives the per-day, per-region, per-service key.
func signingKey(secretKey, dateStamp, region string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	return hmacSHA256(kService, "aws4_request")
}
