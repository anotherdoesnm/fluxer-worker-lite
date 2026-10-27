package main

// Tests for the S3 write path.
//
// A signature mistake only shows up as a 403 from the real store, so the test does
// the two things that can be checked locally: the canonical request is built with
// the headers in sorted order, and a request against a local server really is a PUT
// with a body hash that matches what was sent.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCanonicalRequestSortsHeaders(t *testing.T) {
	canonical, signed := canonicalRequest(http.MethodPut, "/bucket/key", "hash", "20260101T000000Z",
		map[string]string{"host": "s3.example"})
	if signed != "host;x-amz-content-sha256;x-amz-date" {
		t.Fatalf("signed headers = %q", signed)
	}
	if !strings.HasPrefix(canonical, "PUT\n/bucket/key\n\n") {
		t.Fatalf("canonical request starts with %q", canonical[:min(40, len(canonical))])
	}
	if !strings.HasSuffix(canonical, "\nhost;x-amz-content-sha256;x-amz-date\nhash") {
		t.Fatalf("canonical request tail = %q", canonical[len(canonical)-60:])
	}
}

func TestS3PutAgainstLocalServer(t *testing.T) {
	var gotMethod, gotPath, gotAuth, gotSHA string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotSHA = r.Header.Get("x-amz-content-sha256")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("FLUXER_S3_ENDPOINT", srv.URL)
	t.Setenv("FLUXER_S3_BUCKET_CDN", "cdn")
	t.Setenv("FLUXER_S3_ACCESS_KEY_ID", "key")
	t.Setenv("FLUXER_S3_SECRET_ACCESS_KEY", "secret")
	t.Setenv("FLUXER_S3_REGION", "us-east-1")

	if err := s3Put(context.Background(), "archive/1.zip", []byte("payload"), "application/zip"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Fatalf("method = %s", gotMethod)
	}
	if gotPath != "/cdn/archive/1.zip" {
		t.Fatalf("path = %q", gotPath)
	}
	if !strings.HasPrefix(gotAuth, "AWS4-HMAC-SHA256 Credential=key/") {
		t.Fatalf("authorization = %q", gotAuth)
	}
	if gotSHA != sha256Hex([]byte("payload")) {
		t.Fatalf("payload hash = %q", gotSHA)
	}
	if string(gotBody) != "payload" {
		t.Fatalf("body = %q", gotBody)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
