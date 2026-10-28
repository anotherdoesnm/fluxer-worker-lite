package main

// Integration test for the search store's SQL: full-text, then the substring /
// similarity fallback. Like the other *_db_test.go files it needs a scratch
// database (FLUXER_TEST_PG_DSN) and skips otherwise, and it cleans up the index
// it creates.

import (
	"context"
	"encoding/json"
	"testing"
)

func TestSearchStoreFullTextAndFallback(t *testing.T) {
	openTestDB(t)
	ctx := context.Background()
	store := &pgSearchStore{}
	if err := store.ensure(ctx); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	const uid = "__test_search_store"
	if _, err := db.ExecContext(ctx, `DELETE FROM `+searchDocumentsTable+` WHERE index_uid = $1`, uid); err != nil {
		t.Fatalf("clean: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM `+searchDocumentsTable+` WHERE index_uid = $1`, uid)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM `+searchIndexesTable+` WHERE index_uid = $1`, uid)
	})
	if err := store.createIndex(ctx, uid, "id"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	attrs, _ := json.Marshal([]string{"content"})
	if err := store.putSetting(ctx, uid, "searchableAttributes", attrs); err != nil {
		t.Fatalf("settings: %v", err)
	}
	docs := []json.RawMessage{
		json.RawMessage(`{"id":"1","content":"doesnm hello world","channelId":"10"}`),
		json.RawMessage(`{"id":"2","content":"completely different text","channelId":"10"}`),
	}
	if _, err := store.upsertDocuments(ctx, uid, docs); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	t.Run("full text", func(t *testing.T) {
		resp, err := store.search(ctx, uid, searchRequest{Query: "hello", Limit: 10})
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(resp.Hits) != 1 || resp.TotalHits != 1 {
			t.Fatalf("full text: %d hits (total %d), want 1", len(resp.Hits), resp.TotalHits)
		}
	})

	t.Run("prefix falls back", func(t *testing.T) {
		// "doesn" is not a word in the document, so full text finds nothing and
		// the substring pass has to answer.
		resp, err := store.search(ctx, uid, searchRequest{Query: "doesn", Limit: 10})
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(resp.Hits) != 1 {
			t.Fatalf("prefix: %d hits, want 1", len(resp.Hits))
		}
	})

	t.Run("typo falls back when pg_trgm is present", func(t *testing.T) {
		if !trgmReady() {
			t.Skip("pg_trgm is not installed on this database")
		}
		resp, err := store.search(ctx, uid, searchRequest{Query: "doesnn", Limit: 10})
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(resp.Hits) != 1 {
			t.Fatalf("typo: %d hits, want 1", len(resp.Hits))
		}
	})

	t.Run("relevant word beats unrelated document", func(t *testing.T) {
		resp, err := store.search(ctx, uid, searchRequest{Query: "different", Limit: 10})
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(resp.Hits) != 1 {
			t.Fatalf("got %d hits", len(resp.Hits))
		}
		var doc map[string]any
		if err := json.Unmarshal(resp.Hits[0], &doc); err != nil {
			t.Fatalf("hit is not JSON: %v", err)
		}
		if doc["id"] != "2" {
			t.Fatalf("wrong hit: %v", doc["id"])
		}
	})
}
