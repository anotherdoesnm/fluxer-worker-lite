package main

// Tests for the archive core.
//
// A zip that cannot be read back is worse than no export, so the round trip is the
// first test. The pre-signature is checked for shape — algorithm, expiry, signature —
// because the only other way to notice a mistake is a 403 from the store when a
// person clicks the link.

import (
	"strings"
	"testing"
	"time"
)

func TestBuildZipRoundTrip(t *testing.T) {
	entries := []zipEntry{
		{Name: "user.json", Data: []byte(`{"username":"alice"}`)},
		{Name: "messages/general.json", Data: []byte(`[{"content":"hi"}]`)},
		{Name: "attachments/1/2/a.png", Data: []byte{0x89, 'P', 'N', 'G'}},
	}
	data, err := buildZip(entries)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	got, err := readZip(data)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("entries = %d, want %d", len(got), len(entries))
	}
	for _, entry := range entries {
		if string(got[entry.Name]) != string(entry.Data) {
			t.Fatalf("%s = %q, want %q", entry.Name, got[entry.Name], entry.Data)
		}
	}
	if archiveHash(data) == "" {
		t.Fatal("no hash for the archive")
	}
}

func TestAWSURIEncode(t *testing.T) {
	if got := awsURIEncode("a b/c+d"); got != "a%20b%2Fc%2Bd" {
		t.Fatalf("encode = %q", got)
	}
	if got := awsURIEncode("safe-name_1.2~3"); got != "safe-name_1.2~3" {
		t.Fatalf("unreserved characters were encoded: %q", got)
	}
}

func TestPresignGetShape(t *testing.T) {
	t.Setenv("FLUXER_S3_ENDPOINT", "https://s3.example")
	t.Setenv("FLUXER_S3_ACCESS_KEY_ID", "key")
	t.Setenv("FLUXER_S3_SECRET_ACCESS_KEY", "secret")
	t.Setenv("FLUXER_S3_REGION", "us-east-1")
	t.Setenv("FLUXER_S3_BUCKET_HARVESTS", "harvests")

	link, err := presignGet("harvests/1/1.zip", time.Hour)
	if err != nil {
		t.Fatalf("presign: %v", err)
	}
	for _, want := range []string{
		"https://s3.example/harvests/harvests/1/1.zip?",
		"X-Amz-Algorithm=AWS4-HMAC-SHA256",
		"X-Amz-Expires=3600",
		"X-Amz-Signature=",
	} {
		if !strings.Contains(link, want) {
			t.Fatalf("link %q does not contain %q", link, want)
		}
	}

	// A missing bucket must be an error, not a URL that 404s later.
	t.Setenv("FLUXER_S3_BUCKET_HARVESTS", "")
	if _, err := presignGet("k", time.Hour); err == nil {
		t.Fatal("a missing harvest bucket was accepted")
	}
}
