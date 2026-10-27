package main

// Tests for the file-sha ban sweep.
//
// Two things are worth pinning: hashes are compared lower-case (the table stores
// them that way, and the panel accepts any case), and a malformed hash is skipped
// without stopping the rest of the list.

import (
	"context"
	"strings"
	"testing"
)

func TestLowerASCII(t *testing.T) {
	if got := lowerASCII("ABCDEF0123"); got != "abcdef0123" {
		t.Fatalf("lowerASCII = %q", got)
	}
	// Not a hex string: non-ASCII stays untouched instead of being mangled by a
	// locale-dependent fold.
	if got := lowerASCII("ПРИВЕТ"); got != "ПРИВЕТ" {
		t.Fatalf("lowerASCII touched non-ASCII: %q", got)
	}
}

func TestApplyFileShaBansAgainstDB(t *testing.T) {
	conn := openTestDB(t)
	ctx := context.Background()

	valid := strings.Repeat("ab", 32)   // 64 hex chars
	mixed := strings.ToUpper(valid)     // same hash, different case: must collapse
	invalid := "not-a-hash"
	admin := "860000000000000001"

	banned, bad, err := applyFileShaBans(ctx, admin, "", []string{valid, mixed, invalid})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if banned != 2 || bad != 1 {
		t.Fatalf("banned=%d invalid=%d, want 2 and 1", banned, bad)
	}

	key := stringSeg(valid)
	t.Cleanup(func() {
		_, _ = conn.Exec(`DELETE FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`, bannedFileShasTable, key)
	})

	row, err := readKV(ctx, bannedFileShasTable, key)
	if err != nil {
		t.Fatalf("read ban row: %v", err)
	}
	if row == nil {
		t.Fatal("the ban row was not written under the lower-case key")
	}
	if got := typedString(row["sha256_hex"]); got != valid {
		t.Fatalf("sha256_hex = %q", got)
	}
	if got := typedString(row["category"]); got != blocklistCategoryManual {
		t.Fatalf("category = %q", got)
	}
	if got := intOrZero(row["severity"]); got != blocklistSeverityBlock {
		t.Fatalf("severity = %d", got)
	}
	if got := typedString(row["added_by"]); got != admin {
		t.Fatalf("added_by = %q", got)
	}

	// The malformed one must not have created a row anywhere.
	var count int
	if err := conn.QueryRow(`SELECT count(*) FROM fluxer_kv WHERE table_name = $1 AND row_key = $2`,
		bannedFileShasTable, stringSeg(invalid)).Scan(&count); err != nil {
		t.Fatalf("count invalid: %v", err)
	}
	if count != 0 {
		t.Fatal("a malformed hash was written")
	}
}
