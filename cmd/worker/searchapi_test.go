package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeSearchStore records what the handlers asked for and answers with canned
// data, so the HTTP layer and the Meilisearch shapes can be tested without a
// database.
type fakeSearchStore struct {
	exists       bool
	settingsJSON map[string]any
	created      []string
	settingsPut  []string
	upserted     int
	deletedIDs   int
	deletedAll   int
	lastFilter   string
	lastSearch   searchRequest
	searchCalls  int
	ensureCalls  int
}

func (f *fakeSearchStore) ensure(context.Context) error { f.ensureCalls++; return nil }

func (f *fakeSearchStore) indexExists(context.Context, string) (bool, error) { return f.exists, nil }

func (f *fakeSearchStore) createIndex(_ context.Context, uid, _ string) error {
	f.created = append(f.created, uid)
	f.exists = true
	return nil
}

func (f *fakeSearchStore) settings(context.Context, string) (map[string]any, error) {
	if f.settingsJSON == nil {
		return map[string]any{}, nil
	}
	return f.settingsJSON, nil
}

func (f *fakeSearchStore) putSetting(_ context.Context, _, setting string, _ json.RawMessage) error {
	f.settingsPut = append(f.settingsPut, setting)
	return nil
}

func (f *fakeSearchStore) upsertDocuments(_ context.Context, _ string, docs []json.RawMessage) (int, error) {
	f.upserted = len(docs)
	return len(docs), nil
}

func (f *fakeSearchStore) deleteByIDs(_ context.Context, _ string, ids []string) (int, error) {
	f.deletedIDs = len(ids)
	return len(ids), nil
}

func (f *fakeSearchStore) deleteByFilter(_ context.Context, _, filter string) (int, error) {
	f.lastFilter = filter
	return 2, nil
}

func (f *fakeSearchStore) deleteAll(context.Context, string) (int, error) {
	f.deletedAll = 3
	return 3, nil
}

func (f *fakeSearchStore) search(_ context.Context, _ string, req searchRequest) (searchResponse, error) {
	f.searchCalls++
	f.lastSearch = req
	hits := []json.RawMessage{json.RawMessage(`{"id":1,"content":"hello"}`)}
	return searchResponse{Hits: hits, TotalHits: 1, EstimatedTotalHits: 1,
		Query: req.Query, Limit: req.Limit, Offset: req.Offset}, nil
}

func newSearchTestServer(store *fakeSearchStore) http.Handler {
	api := &searchAPI{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /indexes", api.handleCreateIndex)
	mux.HandleFunc("GET /indexes/{uid}", api.handleGetIndex)
	mux.HandleFunc("GET /indexes/{uid}/settings", api.handleGetSettings)
	mux.HandleFunc("PUT /indexes/{uid}/settings/{setting}", api.handlePutSetting)
	mux.HandleFunc("PATCH /indexes/{uid}/settings/{setting}", api.handlePutSetting)
	mux.HandleFunc("POST /indexes/{uid}/documents", api.handleAddDocuments)
	mux.HandleFunc("DELETE /indexes/{uid}/documents", api.handleDeleteAll)
	mux.HandleFunc("POST /indexes/{uid}/documents/delete-batch", api.handleDeleteBatch)
	mux.HandleFunc("POST /indexes/{uid}/documents/delete", api.handleDeleteByFilter)
	mux.HandleFunc("POST /indexes/{uid}/search", api.handleSearch)
	return mux
}

func doJSON(t *testing.T, h http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	out := map[string]any{}
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s %s: response is not JSON: %v (%s)", method, path, err, rec.Body.String())
		}
	}
	return rec.Code, out
}

func TestSearchAPICreateAndGetIndex(t *testing.T) {
	store := &fakeSearchStore{}
	h := newSearchTestServer(store)

	code, body := doJSON(t, h, "POST", "/indexes", `{"uid":"messages","primaryKey":"id"}`)
	if code != http.StatusAccepted {
		t.Fatalf("create index: status %d", code)
	}
	// The API polls /tasks/{uid} after every write, so the response has to carry
	// a taskUid and the task has to look finished.
	if _, ok := body["taskUid"]; !ok {
		t.Fatalf("create index: no taskUid in %v", body)
	}
	if body["status"] != "succeeded" {
		t.Fatalf("create index: task status %v", body["status"])
	}
	if len(store.created) != 1 || store.created[0] != "messages" {
		t.Fatalf("store saw %v", store.created)
	}

	code, body = doJSON(t, h, "GET", "/indexes/messages", "")
	if code != http.StatusOK || body["uid"] != "messages" {
		t.Fatalf("get index: %d %v", code, body)
	}
}

func TestSearchAPIGetMissingIndex(t *testing.T) {
	h := newSearchTestServer(&fakeSearchStore{exists: false})
	code, body := doJSON(t, h, "GET", "/indexes/nope", "")
	if code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", code)
	}
	if body["code"] != "index_not_found" {
		t.Fatalf("body %v", body)
	}
}

func TestSearchAPISettingsRoundTrip(t *testing.T) {
	store := &fakeSearchStore{settingsJSON: map[string]any{
		"searchableAttributes": []string{"content"},
	}}
	h := newSearchTestServer(store)

	code, body := doJSON(t, h, "GET", "/indexes/messages/settings", "")
	if code != http.StatusOK {
		t.Fatalf("get settings: %d", code)
	}
	// The adapter compares these three lists by value, so absent ones must read
	// as empty arrays rather than null.
	for _, key := range []string{"searchableAttributes", "filterableAttributes", "sortableAttributes"} {
		if _, ok := body[key].([]any); !ok {
			t.Fatalf("settings[%q] = %#v, want an array", key, body[key])
		}
	}
	if body["searchableAttributes"].([]any)[0] != "content" {
		t.Fatalf("stored searchableAttributes lost: %v", body["searchableAttributes"])
	}
	if _, ok := body["pagination"].(map[string]any); !ok {
		t.Fatalf("pagination = %#v, want an object", body["pagination"])
	}

	code, _ = doJSON(t, h, "PUT", "/indexes/messages/settings/searchable-attributes", `["content","author"]`)
	if code != http.StatusAccepted {
		t.Fatalf("put setting: %d", code)
	}
	if len(store.settingsPut) != 1 || store.settingsPut[0] != "searchableAttributes" {
		t.Fatalf("store saw settings %v", store.settingsPut)
	}
	// An unknown setting is a client error, not something to drop silently.
	if code, _ := doJSON(t, h, "PUT", "/indexes/messages/settings/nonsense", `[]`); code != http.StatusBadRequest {
		t.Fatalf("unknown setting: %d, want 400", code)
	}
}

func TestSearchAPIDocuments(t *testing.T) {
	store := &fakeSearchStore{}
	h := newSearchTestServer(store)

	code, body := doJSON(t, h, "POST", "/indexes/messages/documents", `[{"id":1},{"id":2},{"id":3}]`)
	if code != http.StatusAccepted || store.upserted != 3 {
		t.Fatalf("add documents: %d, store upserted %d", code, store.upserted)
	}
	details, _ := body["details"].(map[string]any)
	if details["indexedDocuments"] != float64(3) {
		t.Fatalf("details %v", body["details"])
	}

	// delete-batch takes a bare array of ids.
	code, _ = doJSON(t, h, "POST", "/indexes/messages/documents/delete-batch", `[1,2]`)
	if code != http.StatusAccepted || store.deletedIDs != 2 {
		t.Fatalf("delete-batch: %d, store saw %d", code, store.deletedIDs)
	}

	// delete-by-filter takes {filter}.
	code, _ = doJSON(t, h, "POST", "/indexes/messages/documents/delete", `{"filter":"channel_id = 5"}`)
	if code != http.StatusAccepted || store.lastFilter != "channel_id = 5" {
		t.Fatalf("delete-by-filter: %d, store saw %q", code, store.lastFilter)
	}

	code, _ = doJSON(t, h, "DELETE", "/indexes/messages/documents", "")
	if code != http.StatusAccepted || store.deletedAll != 3 {
		t.Fatalf("delete-all: %d, store saw %d", code, store.deletedAll)
	}
}

func TestSearchAPISearchShape(t *testing.T) {
	store := &fakeSearchStore{}
	h := newSearchTestServer(store)

	body := `{"q":"hello","filter":"channel_id = 7","limit":5,"offset":10,"sort":["created_at:desc"]}`
	code, out := doJSON(t, h, "POST", "/indexes/messages/search", body)
	if code != http.StatusOK {
		t.Fatalf("search: %d", code)
	}
	if out["query"] != "hello" {
		t.Fatalf("query echoed as %v", out["query"])
	}
	if _, ok := out["hits"].([]any); !ok {
		t.Fatalf("hits = %#v", out["hits"])
	}
	if out["totalHits"] != float64(1) || out["estimatedTotalHits"] != float64(1) {
		t.Fatalf("totals: %v / %v", out["totalHits"], out["estimatedTotalHits"])
	}
	if store.lastSearch.Filter != "channel_id = 7" || store.lastSearch.Limit != 5 || store.lastSearch.Offset != 10 {
		t.Fatalf("store saw %+v", store.lastSearch)
	}
	// Limits are clamped instead of being passed through to the database.
	if _, _ = doJSON(t, h, "POST", "/indexes/messages/search", `{"q":"x","limit":100000}`); store.lastSearch.Limit != searchMaxLimit {
		t.Fatalf("limit clamp: store saw %d, want %d", store.lastSearch.Limit, searchMaxLimit)
	}
	if _, _ = doJSON(t, h, "POST", "/indexes/messages/search", `{}`); store.lastSearch.Limit != searchDefaultLimit {
		t.Fatalf("default limit: store saw %d, want %d", store.lastSearch.Limit, searchDefaultLimit)
	}
}

func TestSearchAPIRejectsBadBodies(t *testing.T) {
	h := newSearchTestServer(&fakeSearchStore{exists: true})
	for _, tc := range []struct{ method, path, body string }{
		{"POST", "/indexes", `{"uid":""}`},
		{"POST", "/indexes/messages/documents", `[]`},
		{"POST", "/indexes/messages/documents", `"nope"`},
		{"POST", "/indexes/messages/search", `not json`},
	} {
		if code, _ := doJSON(t, h, tc.method, tc.path, tc.body); code != http.StatusBadRequest {
			t.Fatalf("%s %s: status %d, want 400", tc.method, tc.path, code)
		}
	}
}
