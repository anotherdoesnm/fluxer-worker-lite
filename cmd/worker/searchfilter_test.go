package main

import (
	"strings"
	"testing"
)

func TestCompileSearchFilter(t *testing.T) {
	cases := []struct {
		name   string
		filter string
		want   []string // fragments the SQL must contain
		args   []any
	}{
		{
			name:   "term",
			filter: "channel_id = 123",
			want:   []string{"(doc->>'channel_id') IS NOT DISTINCT FROM $2"},
			args:   []any{"123"},
		},
		{
			name:   "not equal keeps rows without the field",
			filter: "author_id != 7",
			want:   []string{"(doc->>'author_id') IS DISTINCT FROM $2"},
			args:   []any{"7"},
		},
		{
			name:   "or group",
			filter: "(channel_id = 1 OR channel_id = 2)",
			want:   []string{"OR", "IS NOT DISTINCT FROM $2", "IS NOT DISTINCT FROM $3"},
			args:   []any{"1", "2"},
		},
		{
			name:   "and exists",
			filter: "(guild_id = 9 AND deleted_at EXISTS)",
			want:   []string{"AND", "jsonb_exists(doc, 'deleted_at')"},
			args:   []any{"9"},
		},
		{
			name:   "not exists",
			filter: "deleted_at NOT EXISTS",
			want:   []string{"NOT jsonb_exists(doc, 'deleted_at')"},
		},
		{
			name:   "is null pair",
			filter: "(a IS NULL AND b IS NOT NULL)",
			want:   []string{"(doc->>'a') IS NULL", "(doc->>'b') IS NOT NULL"},
		},
		{
			name:   "range casts to numeric",
			filter: "(created_at > 100 AND created_at <= 200)",
			want:   []string{"::numeric > $2", "::numeric <= $3"},
			args:   []any{float64(100), float64(200)},
		},
		{
			name:   "quoted value with spaces",
			filter: `name = "hello world"`,
			want:   []string{"IS NOT DISTINCT FROM $2"},
			args:   []any{"hello world"},
		},
		{
			name:   "not term",
			filter: "NOT channel_id = 5",
			want:   []string{"(NOT (doc->>'channel_id') IS NOT DISTINCT FROM $2)"},
			args:   []any{"5"},
		},
		{name: "empty", filter: "   ", want: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqlText, args, err := compileSearchFilter(tc.filter, 2)
			if err != nil {
				t.Fatalf("compile %q: %v", tc.filter, err)
			}
			for _, frag := range tc.want {
				if !strings.Contains(sqlText, frag) {
					t.Fatalf("filter %q: SQL %q does not contain %q", tc.filter, sqlText, frag)
				}
			}
			if len(args) != len(tc.args) {
				t.Fatalf("filter %q: got %d args, want %d", tc.filter, len(args), len(tc.args))
			}
			for i := range args {
				if args[i] != tc.args[i] {
					t.Fatalf("filter %q: arg %d = %#v, want %#v", tc.filter, i, args[i], tc.args[i])
				}
			}
		})
	}
}

func TestCompileSearchFilterRejects(t *testing.T) {
	for _, filter := range []string{
		"channel_id",                   // no operator
		"channel_id = 1 AND",           // dangling AND
		"(channel_id = 1",              // unbalanced
		"channel_id === 1",             // not an operator we know
		"channel_id > abc",             // range needs a number
		"bad-field = 1",                // field names are validated
		`name = "unterminated`,         // unterminated string
		"channel_id = 1 garbage_token", // trailing junk
	} {
		if _, _, err := compileSearchFilter(filter, 2); err == nil {
			t.Fatalf("filter %q: expected an error, got none", filter)
		}
	}
}

func TestCompileSearchSort(t *testing.T) {
	got, err := compileSearchSort([]string{"created_at:desc", "score:asc"}, "id")
	if err != nil {
		t.Fatalf("compile sort: %v", err)
	}
	if !strings.Contains(got, "(doc->>'created_at') DESC") || !strings.Contains(got, "(doc->>'score') ASC") {
		t.Fatalf("unexpected ORDER BY %q", got)
	}
	if _, err := compileSearchSort([]string{"bad field:desc"}, "id"); err == nil {
		t.Fatalf("expected an error for an invalid sort field")
	}
	if _, err := compileSearchSort([]string{"field:sideways"}, "id"); err == nil {
		t.Fatalf("expected an error for an invalid direction")
	}
	if got, err := compileSearchSort(nil, "id"); err != nil || !strings.HasPrefix(got, "rank DESC") {
		t.Fatalf("default order = %q, err = %v", got, err)
	}
}

func TestDocumentIDAndSearchableText(t *testing.T) {
	doc := []byte(`{"id":42,"channel_id":"7","content":"hello  world","n":3}`)
	id, err := documentID(doc, "id")
	if err != nil || id != "42" {
		t.Fatalf("documentID = %q, %v; want 42", id, err)
	}
	if _, err := documentID([]byte(`{"channel_id":"7"}`), "id"); err == nil {
		t.Fatalf("expected an error for a document without the primary key")
	}

	// With an explicit searchable list only those fields are indexed.
	body := searchableText(doc, []string{"content"})
	if body != "hello  world" {
		t.Fatalf("searchableText(content) = %q", body)
	}
	// Without one, every scalar is, in a stable order.
	body = searchableText(doc, nil)
	for _, want := range []string{"hello  world", "7", "42", "3"} {
		if !strings.Contains(body, want) {
			t.Fatalf("searchableText(all) = %q, missing %q", body, want)
		}
	}
}

func TestDecodeDocuments(t *testing.T) {
	if docs, err := decodeDocuments([]byte(`[{"id":1},{"id":2}]`)); err != nil || len(docs) != 2 {
		t.Fatalf("array: %v %d", err, len(docs))
	}
	if docs, err := decodeDocuments([]byte(`{"id":1}`)); err != nil || len(docs) != 1 {
		t.Fatalf("single: %v %d", err, len(docs))
	}
	if _, err := decodeDocuments([]byte(`[]`)); err == nil {
		t.Fatalf("expected an error for an empty list")
	}
	if _, err := decodeDocuments([]byte(`"nope"`)); err == nil {
		t.Fatalf("expected an error for a string body")
	}
}
