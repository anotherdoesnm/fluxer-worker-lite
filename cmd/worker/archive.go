package main

// The archive core the two data exports stand on: build a zip, upload it, hand out a
// download URL, and keep the row that says where it is.
//
// S3 accepts a pre-signed GET for the download, which means an export needs no token
// service and no new API endpoint on our side: the object is private, the URL is
// time-boxed, and the expiry is written into the row so the ui can show it. When
// pre-signing is not configured (no bucket, no keys) the export must fail loudly —
// an archive nobody can download is not an export.
//
// The zip carries the data as JSON files plus, optionally, the attachments as binary
// entries; the file names are the ones the api writes, because people open these
// archives and compare them with the documentation.

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// zipEntry is one file in the archive.
type zipEntry struct {
	Name string
	Data []byte
	Mode os.FileMode
}

// buildZip renders the entries in the order given. Directories are implicit: a
// reader that expects `messages/` to exist gets it from the file names.
func buildZip(entries []zipEntry) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, entry := range entries {
		mode := entry.Mode
		if mode == 0 {
			mode = 0o644
		}
		header := &zip.FileHeader{Name: entry.Name, Method: zip.Deflate}
		header.SetMode(mode)
		header.Modified = time.Now().UTC()
		w, err := zw.CreateHeader(header)
		if err != nil {
			return nil, fmt.Errorf("zip %s: %w", entry.Name, err)
		}
		if _, err := w.Write(entry.Data); err != nil {
			return nil, fmt.Errorf("zip %s: write: %w", entry.Name, err)
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// readZip is used by the tests and by anyone verifying an archive locally.
func readZip(data []byte) (map[string][]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(zr.File))
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		var buf bytes.Buffer
		_, err = buf.ReadFrom(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
		out[f.Name] = buf.Bytes()
	}
	return out, nil
}

// harvestBucket is the bucket exports live in, from the same env the api reads.
func harvestBucket() string {
	return os.Getenv("FLUXER_S3_BUCKET_HARVESTS")
}

// presignGet builds a pre-signed GET URL for one object (SigV4 query signing).
// expiresIn is capped at seven days, which is the protocol limit.
func presignGet(key string, expiresIn time.Duration) (string, error) {
	endpoint, cdnBucket, accessKey, secretKey, region := s3Config()
	bucket := harvestBucket()
	if bucket == "" {
		return "", fmt.Errorf("no harvest bucket configured (FLUXER_S3_BUCKET_HARVESTS)")
	}
	if endpoint == "" || accessKey == "" || secretKey == "" {
		return "", fmt.Errorf("s3 is not configured")
	}
	_ = cdnBucket
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	scope := dateStamp + "/" + region + "/s3/aws4_request"
	seconds := int(expiresIn / time.Second)
	if seconds <= 0 || seconds > 7*24*3600 {
		seconds = 7 * 24 * 3600
	}
	canonicalURI := objectPath(bucket, key)

	query := map[string]string{
		"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
		"X-Amz-Credential":    accessKey + "/" + scope,
		"X-Amz-Date":          amzDate,
		"X-Amz-Expires":       fmt.Sprintf("%d", seconds),
		"X-Amz-SignedHeaders": "host",
	}
	names := make([]string, 0, len(query))
	for name := range query {
		names = append(names, name)
	}
	sort.Strings(names)
	var canonicalQuery strings.Builder
	for i, name := range names {
		if i > 0 {
			canonicalQuery.WriteString("&")
		}
		canonicalQuery.WriteString(awsURIEncode(name) + "=" + awsURIEncode(query[name]))
	}
	canonicalHeaders := "host:" + u.Host + "\n"
	canonicalRequest := strings.Join([]string{
		"GET", canonicalURI, canonicalQuery.String(), canonicalHeaders, "host", "UNSIGNED-PAYLOAD",
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signature := hex.EncodeToString(hmacSHA256(signingKey(secretKey, dateStamp, region), stringToSign))
	return fmt.Sprintf("%s%s?%s&X-Amz-Signature=%s", endpoint, canonicalURI, canonicalQuery.String(), signature), nil
}

// awsURIEncode is the strict encoding SigV4 uses for query parameters: unreserved
// characters only, everything else percent-encoded in upper case.
func awsURIEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// archiveHash is the content address the row stores: the sha256 of the zip, so a
// re-run can tell whether it produced the same archive.
func archiveHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
