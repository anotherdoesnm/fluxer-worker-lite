package main

// Tests for the asset deletion queue. They cover what can be checked without a
// live valkey or object store: the retry bound, the wire format of a queue item
// (the api writes it, the worker reads it) and the SigV4 request the store sees.

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAssetRetryDue(t *testing.T) {
	for retry := 0; retry < assetQueueMaxRetry; retry++ {
		if !assetRetryDue(queuedAsset{RetryCount: retry}) {
			t.Fatalf("retry %d: expected another attempt", retry)
		}
	}
	for _, retry := range []int{assetQueueMaxRetry, assetQueueMaxRetry + 1, 99} {
		if assetRetryDue(queuedAsset{RetryCount: retry}) {
			t.Fatalf("retry %d: expected the item to be dropped", retry)
		}
	}
}

// The api enqueues items without the omitempty fields, so a bare item has to
// decode, and a requeued item has to keep everything it came with.
func TestQueuedAssetRoundTrip(t *testing.T) {
	bare := []byte(`{"s3Key":"attachments/1/2/a.png","cdnUrl":"https://x/media/a.png","reason":"attachment-expired"}`)
	var item queuedAsset
	if err := json.Unmarshal(bare, &item); err != nil {
		t.Fatal(err)
	}
	if item.RetryCount != 0 || item.StaleReference != nil {
		t.Fatalf("unexpected defaults: %+v", item)
	}

	item.StaleReference = json.RawMessage(`{"table":"attachments","id":"2","hash":"abc"}`)
	item.RetryCount++
	again, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	var back queuedAsset
	if err := json.Unmarshal(again, &back); err != nil {
		t.Fatal(err)
	}
	if back.RetryCount != 1 || len(back.StaleReference) == 0 || back.S3Key != item.S3Key {
		t.Fatalf("round trip lost data: %s", again)
	}
}

// A queue item written by the api may carry no cdnUrl at all; that must not stop
// the deletion, otherwise the object stays in the store forever.
func TestQueuedAssetWithoutCdnURL(t *testing.T) {
	var item queuedAsset
	if err := json.Unmarshal([]byte(`{"s3Key":"attachments/1/2/a.png"}`), &item); err != nil {
		t.Fatal(err)
	}
	if item.S3Key == "" || item.CdnURL != nil {
		t.Fatalf("unexpected item: %+v", item)
	}
}

// Attachment keys are derived from snowflakes and a user-supplied file name, so
// they can contain spaces, plus signs and percent signs. The store resolves the
// object by the escaped path, and a mismatch here means a 403 or a wrong object.
func TestObjectPathEscapesSegments(t *testing.T) {
	cases := map[string]string{
		"attachments/1/2/plain.png":      "/fluxer/attachments/1/2/plain.png",
		"attachments/1/2/with space.txt": "/fluxer/attachments/1/2/with%20space.txt",
		"attachments/1/2/a+b.txt":        "/fluxer/attachments/1/2/a%2Bb.txt",
		"attachments/1/2/100%.txt":       "/fluxer/attachments/1/2/100%25.txt",
		"avatars/1/ab/hash":              "/fluxer/avatars/1/ab/hash",
	}
	for key, want := range cases {
		if got := objectPath("fluxer", key); got != want {
			t.Fatalf("%q: got %q, want %q", key, got, want)
		}
	}
}

// The canonical request is the part of SigV4 that is easy to get silently wrong:
// header order, the empty query line and the trailing newline all matter.
func TestCanonicalDeleteRequest(t *testing.T) {
	emptyHash := sha256Hex(nil)
	got, signed := canonicalDeleteRequest("192.168.1.10:26302", "/fluxer/attachments/1/2/a.png", emptyHash, "20261026T130000Z")
	signedWant := "host;x-amz-content-sha256;x-amz-date"
	if signed != signedWant {
		t.Fatalf("signed headers: got %q, want %q", signed, signedWant)
	}
	want := strings.Join([]string{
		"DELETE",
		"/fluxer/attachments/1/2/a.png",
		"",
		"host:192.168.1.10:26302\nx-amz-content-sha256:" + emptyHash + "\nx-amz-date:20261026T130000Z\n",
		signedWant,
		emptyHash,
	}, "\n")
	if got != want {
		t.Fatalf("canonical request mismatch:\n got: %q\nwant: %q", got, want)
	}
}
