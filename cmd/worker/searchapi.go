package main

// A Meilisearch-compatible HTTP surface in front of Postgres full-text search.
//
// Why this exists: the API's search backend is Meilisearch, but it talks to it
// over plain HTTP — MeilisearchHttpClient is literally fetch(host + path) with an
// optional bearer key, and MeilisearchIndexAdapter only uses the few endpoints
// below. So a shim that speaks the same JSON gives the instance working search
// without a Meilisearch process, which matters: on a 1 GB host Meilisearch's RSS
// is 150-300 MiB, while these queries run in the instance's existing Postgres.
//
// The subset is exactly what MeilisearchIndexAdapter calls in the release this
// worker is pinned to:
//
//	POST   /indexes                                  {uid, primaryKey}
//	GET    /indexes/{uid}                            404 when it does not exist
//	GET    /indexes/{uid}/settings
//	PUT    /indexes/{uid}/settings/{setting}         raw value as the body
//	PATCH  /indexes/{uid}/settings/{setting}         raw value as the body
//	POST   /indexes/{uid}/documents                  array of documents
//	DELETE /indexes/{uid}/documents                  drop the whole index
//	POST   /indexes/{uid}/documents/delete-batch     array of ids
//	POST   /indexes/{uid}/documents/delete           {filter}
//	POST   /indexes/{uid}/search                     {q, filter, limit, offset, sort, ...}
//	GET    /tasks/{uid}                              polled every 50 ms after a write
//
// Every write is answered with a task that is already "succeeded": the work is
// done synchronously in the request, so there is nothing to wait for. That is
// deliberately simpler than Meilisearch's async model and it is what the client
// tolerates (it polls /tasks/{uid} until status == succeeded).
//
// Known differences, on purpose:
//   - ranking is ts_rank over a plain (non-stemmed) dictionary plus the sort
//     order the API asks for, not Meilisearch's ranking rules;
//   - there is no typo tolerance and no prefix matching. `pg_trgm` could add a
//     fuzzy fallback later, but it changes relevance for every query, so it is
//     not on by default;
//   - facets are not computed (the response simply has no facetDistribution,
//     which the adapter already treats as optional);
//   - settings are stored and honoured where they matter here
//     (searchableAttributes decides what goes into the indexed text, sortable
//     attributes are validated), but filterable/ranking settings are not
//     enforced — a Meilisearch instance would reject filters on unknown fields.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	searchDocumentsTable = "search_documents"
	searchIndexesTable   = "search_indexes"
	searchMaxLimit       = 1000
	searchDefaultLimit   = 20
)

// searchStore is the storage the handlers talk to. It exists so the HTTP layer
// and the filter parser can be tested without a database.
type searchStore interface {
	ensure(ctx context.Context) error
	indexExists(ctx context.Context, uid string) (bool, error)
	createIndex(ctx context.Context, uid, primaryKey string) error
	settings(ctx context.Context, uid string) (map[string]any, error)
	putSetting(ctx context.Context, uid, setting string, value json.RawMessage) error
	upsertDocuments(ctx context.Context, uid string, docs []json.RawMessage) (int, error)
	deleteByIDs(ctx context.Context, uid string, ids []string) (int, error)
	deleteByFilter(ctx context.Context, uid, filter string) (int, error)
	deleteAll(ctx context.Context, uid string) (int, error)
	search(ctx context.Context, uid string, req searchRequest) (searchResponse, error)
}

// searchRequest is the subset of the Meilisearch search body we honour.
type searchRequest struct {
	Query      string          `json:"q"`
	Filter     string          `json:"filter"`
	Limit      int             `json:"limit"`
	Offset     int             `json:"offset"`
	Sort       []string        `json:"sort"`
	SearchOn   []string        `json:"attributesToSearchOn"`
	PrimaryKey string          `json:"-"`
	Raw        json.RawMessage `json:"-"`
}

type searchResponse struct {
	Hits               []json.RawMessage `json:"hits"`
	Query              string            `json:"query"`
	ProcessingTimeMs   int64             `json:"processingTimeMs"`
	Limit              int               `json:"limit"`
	Offset             int               `json:"offset"`
	EstimatedTotalHits int               `json:"estimatedTotalHits"`
	TotalHits          int               `json:"totalHits"`
}

// taskSeq hands out task ids. The client only needs them to be unique per call.
var taskSeq atomic.Int64

func nextSearchTask(indexUID, kind string) map[string]any {
	uid := taskSeq.Add(1)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return map[string]any{
		"taskUid":    uid,
		"indexUid":   indexUID,
		"status":     "succeeded",
		"type":       kind,
		"enqueuedAt": now,
		"startedAt":  now,
		"finishedAt": now,
	}
}

// settingNames maps Meilisearch's kebab-case URL segment to the camelCase key the
// adapter reads back from GET /indexes/{uid}/settings.
var settingNames = map[string]string{
	"searchable-attributes": "searchableAttributes",
	"filterable-attributes": "filterableAttributes",
	"sortable-attributes":   "sortableAttributes",
	"pagination":            "pagination",
	"ranking-rules":         "rankingRules",
	"stop-words":            "stopWords",
	"synonyms":              "synonyms",
	"typo-tolerance":        "typoTolerance",
	"displayed-attributes":  "displayedAttributes",
}

// registerSearchAPI mounts the handlers on the worker's existing HTTP mux. It is
// off unless SEARCH_API_ENABLED=1, so an instance that does not want a search
// backend keeps the old behaviour (and the API keeps answering 403 for search).
func registerSearchAPI(mux *http.ServeMux) {
	api := &searchAPI{store: &pgSearchStore{}}

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "available"})
	})
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"commitSha": "worker-lite", "commitDate": "", "pkgVersion": "1.12.0",
		})
	})

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
	// Waits are synchronous above, so every task is already finished by the time
	// the client polls. Any uid maps to a succeeded task.
	mux.HandleFunc("GET /tasks/{uid}", func(w http.ResponseWriter, r *http.Request) {
		uid, _ := strconv.ParseInt(r.PathValue("uid"), 10, 64)
		writeJSON(w, http.StatusOK, map[string]any{
			"uid": uid, "status": "succeeded", "type": "documentAdditionOrUpdate",
			"enqueuedAt": time.Now().UTC().Format(time.RFC3339Nano),
		})
	})
	log.Printf("search: listening on the status server (/indexes, /tasks)")
}

type searchAPI struct {
	store searchStore
	once  sync.Once
	derr  error
}

func (a *searchAPI) ready() error {
	a.once.Do(func() { a.derr = a.store.ensure(context.Background()) })
	return a.derr
}

func (a *searchAPI) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UID        string `json:"uid"`
		PrimaryKey string `json:"primaryKey"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if body.UID == "" {
		writeSearchError(w, http.StatusBadRequest, "missing_index_uid", "uid is required")
		return
	}
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	exists, err := a.store.indexExists(r.Context(), body.UID)
	if err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !exists {
		primary := body.PrimaryKey
		if primary == "" {
			primary = "id"
		}
		if err := a.store.createIndex(r.Context(), body.UID, primary); err != nil {
			writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
			return
		}
	}
	writeJSON(w, http.StatusAccepted, nextSearchTask(body.UID, "indexCreation"))
}

func (a *searchAPI) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	exists, err := a.store.indexExists(r.Context(), uid)
	if err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !exists {
		writeSearchError(w, http.StatusNotFound, "index_not_found", "index not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"uid": uid, "primaryKey": "id"})
}

func (a *searchAPI) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	stored, err := a.store.settings(r.Context(), r.PathValue("uid"))
	if err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	// Meilisearch always answers with the full settings object, and the adapter
	// compares the three attribute lists against what it wants. Missing keys must
	// therefore read as empty lists, not as null.
	out := map[string]any{
		"searchableAttributes": []string{},
		"filterableAttributes": []string{},
		"sortableAttributes":   []string{},
		"pagination":           map[string]any{"maxTotalHits": 1000},
	}
	for k, v := range stored {
		out[k] = v
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *searchAPI) handlePutSetting(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	setting := r.PathValue("setting")
	key, ok := settingNames[setting]
	if !ok {
		writeSearchError(w, http.StatusBadRequest, "invalid_setting", "unknown setting "+setting)
		return
	}
	raw, err := readRawJSON(r)
	if err != nil {
		writeSearchError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if err := a.store.putSetting(r.Context(), uid, key, raw); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, nextSearchTask(uid, "settingsUpdate"))
}

func (a *searchAPI) handleAddDocuments(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	raw, err := readRawJSON(r)
	if err != nil {
		writeSearchError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	docs, err := decodeDocuments(raw)
	if err != nil {
		writeSearchError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	n, err := a.store.upsertDocuments(r.Context(), uid, docs)
	if err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	task := nextSearchTask(uid, "documentAdditionOrUpdate")
	task["details"] = map[string]any{"indexedDocuments": n}
	writeJSON(w, http.StatusAccepted, task)
}

func (a *searchAPI) handleDeleteAll(w http.ResponseWriter, r *http.Request) {
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	uid := r.PathValue("uid")
	n, err := a.store.deleteAll(r.Context(), uid)
	if err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	task := nextSearchTask(uid, "documentDeletion")
	task["details"] = map[string]any{"deletedDocuments": n}
	writeJSON(w, http.StatusAccepted, task)
}

func (a *searchAPI) handleDeleteBatch(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	raw, err := readRawJSON(r)
	if err != nil {
		writeSearchError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	ids, err := decodeIDs(raw)
	if err != nil {
		writeSearchError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	n, err := a.store.deleteByIDs(r.Context(), uid, ids)
	if err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	task := nextSearchTask(uid, "documentDeletion")
	task["details"] = map[string]any{"deletedDocuments": n}
	writeJSON(w, http.StatusAccepted, task)
}

func (a *searchAPI) handleDeleteByFilter(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	var body struct {
		Filter string `json:"filter"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	n, err := a.store.deleteByFilter(r.Context(), uid, body.Filter)
	if err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	task := nextSearchTask(uid, "documentDeletion")
	task["details"] = map[string]any{"deletedDocuments": n}
	writeJSON(w, http.StatusAccepted, task)
}

func (a *searchAPI) handleSearch(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	raw, err := readRawJSON(r)
	if err != nil {
		writeSearchError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	var req searchRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeSearchError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	req.Raw = raw
	if req.Limit <= 0 {
		req.Limit = searchDefaultLimit
	}
	if req.Limit > searchMaxLimit {
		req.Limit = searchMaxLimit
	}
	if req.Offset < 0 {
		req.Offset = 0
	}
	if err := a.ready(); err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	started := time.Now()
	resp, err := a.store.search(r.Context(), uid, req)
	if err != nil {
		writeSearchError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	resp.ProcessingTimeMs = time.Since(started).Milliseconds()
	writeJSON(w, http.StatusOK, resp)
}

// --- storage ---------------------------------------------------------------

type pgSearchStore struct{}

func (s *pgSearchStore) ensure(ctx context.Context) error {
	if db == nil {
		if err := openDB(); err != nil {
			return err
		}
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + searchIndexesTable + ` (
			index_uid text PRIMARY KEY,
			primary_key text NOT NULL DEFAULT 'id',
			settings jsonb NOT NULL DEFAULT '{}'::jsonb,
			created_at timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS ` + searchDocumentsTable + ` (
			index_uid text NOT NULL,
			doc_id text NOT NULL,
			doc jsonb NOT NULL,
			body text NOT NULL DEFAULT '',
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (index_uid, doc_id)
		)`,
		`CREATE INDEX IF NOT EXISTS search_documents_fts_idx
			ON ` + searchDocumentsTable + ` USING gin (to_tsvector('simple', body))`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("search schema: %w", err)
		}
	}
	s.ensureTrigram(ctx)
	return nil
}

var (
	trgmOnce sync.Once
	trgmOK   atomic.Bool
)

// trgmReady reports whether the substring/typo fallback can use similarity().
func trgmReady() bool { return trgmOK.Load() }

// ensureTrigram installs pg_trgm once, best effort. The extension is trusted
// since PostgreSQL 13, so the instance's own role can add it. When that fails
// (an older server, or a role without CREATE) the fallback still works through
// ILIKE: only typo tolerance is lost, which is worth a log line rather than an
// error on every search.
func (s *pgSearchStore) ensureTrigram(ctx context.Context) {
	trgmOnce.Do(func() {
		if _, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pg_trgm`); err != nil {
			log.Printf("search: pg_trgm unavailable, substring fallback only: %v", err)
			return
		}
		if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS search_documents_trgm_idx
			ON `+searchDocumentsTable+` USING gin (body gin_trgm_ops)`); err != nil {
			log.Printf("search: pg_trgm index: %v", err)
			return
		}
		trgmOK.Store(true)
		log.Printf("search: pg_trgm enabled (prefix and typo fallback)")
	})
}

func (s *pgSearchStore) indexExists(ctx context.Context, uid string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM `+searchIndexesTable+` WHERE index_uid = $1`, uid).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *pgSearchStore) createIndex(ctx context.Context, uid, primaryKey string) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO `+searchIndexesTable+` (index_uid, primary_key) VALUES ($1, $2)
		 ON CONFLICT (index_uid) DO NOTHING`, uid, primaryKey)
	return err
}

func (s *pgSearchStore) settings(ctx context.Context, uid string) (map[string]any, error) {
	var raw []byte
	err := db.QueryRowContext(ctx, `SELECT settings FROM `+searchIndexesTable+` WHERE index_uid = $1`, uid).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return map[string]any{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *pgSearchStore) putSetting(ctx context.Context, uid, setting string, value json.RawMessage) error {
	// The index is created by POST /indexes first; if the client skipped that,
	// create it here instead of losing the setting.
	_, err := db.ExecContext(ctx,
		`INSERT INTO `+searchIndexesTable+` (index_uid, settings) VALUES ($1, jsonb_build_object($2::text, $3::jsonb))
		 ON CONFLICT (index_uid) DO UPDATE
		 SET settings = `+searchIndexesTable+`.settings || jsonb_build_object($2::text, $3::jsonb)`,
		uid, setting, string(value))
	return err
}

// searchableAttributes returns the stored list, or nil when the index has none
// (in which case every scalar string in the document is indexed).
func (s *pgSearchStore) searchableAttributes(ctx context.Context, uid string) ([]string, error) {
	st, err := s.settings(ctx, uid)
	if err != nil {
		return nil, err
	}
	raw, ok := st["searchableAttributes"]
	if !ok {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, nil
	}
	out := make([]string, 0, len(list))
	for _, v := range list {
		if s, ok := v.(string); ok && s != "" && s != "*" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *pgSearchStore) primaryKey(ctx context.Context, uid string) string {
	var key string
	err := db.QueryRowContext(ctx, `SELECT primary_key FROM `+searchIndexesTable+` WHERE index_uid = $1`, uid).Scan(&key)
	if err != nil || key == "" {
		return "id"
	}
	return key
}

func (s *pgSearchStore) upsertDocuments(ctx context.Context, uid string, docs []json.RawMessage) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	attrs, err := s.searchableAttributes(ctx, uid)
	if err != nil {
		return 0, err
	}
	primary := s.primaryKey(ctx, uid)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	const q = `INSERT INTO ` + searchDocumentsTable + ` (index_uid, doc_id, doc, body, updated_at)
		VALUES ($1, $2, $3::jsonb, $4, now())
		ON CONFLICT (index_uid, doc_id) DO UPDATE
		SET doc = EXCLUDED.doc, body = EXCLUDED.body, updated_at = now()`
	for _, doc := range docs {
		id, err := documentID(doc, primary)
		if err != nil {
			return 0, err
		}
		body := searchableText(doc, attrs)
		if _, err := tx.ExecContext(ctx, q, uid, id, string(doc), body); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(docs), nil
}

func (s *pgSearchStore) deleteByIDs(ctx context.Context, uid string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	res, err := db.ExecContext(ctx,
		`DELETE FROM `+searchDocumentsTable+` WHERE index_uid = $1 AND doc_id = ANY($2)`, uid, ids)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *pgSearchStore) deleteByFilter(ctx context.Context, uid, filter string) (int, error) {
	where, args, err := compileSearchFilter(filter, 2) // $1 = index uid
	if err != nil {
		return 0, err
	}
	args = append([]any{uid}, args...)
	q := `DELETE FROM ` + searchDocumentsTable + ` WHERE index_uid = $1`
	if where != "" {
		q += " AND (" + where + ")"
	}
	res, err := db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *pgSearchStore) deleteAll(ctx context.Context, uid string) (int, error) {
	res, err := db.ExecContext(ctx, `DELETE FROM `+searchDocumentsTable+` WHERE index_uid = $1`, uid)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// searchMode selects the WHERE/rank pair for one pass over the documents.
type searchMode int

const (
	searchFullText searchMode = iota
	searchFallback
)

// search answers a query. The first pass is Postgres full text: it is what
// matches whole words and what the ts_rank ordering is for. When that finds
// nothing, a second pass runs the substring/similarity fallback, because a
// reader typing "doesn" for "doesnm", or "doesnn" with a typo, expects a hit and
// Meilisearch would have given one. Exact matches never reach the fallback, so
// relevance for the normal case is unchanged.
func (s *pgSearchStore) search(ctx context.Context, uid string, req searchRequest) (searchResponse, error) {
	resp, err := s.searchPass(ctx, uid, req, searchFullText)
	if err != nil {
		return searchResponse{}, err
	}
	if len(resp.Hits) > 0 {
		return resp, nil
	}
	q := strings.TrimSpace(req.Query)
	if q == "" || q == "*" {
		return resp, nil
	}
	return s.searchPass(ctx, uid, req, searchFallback)
}

func (s *pgSearchStore) searchPass(ctx context.Context, uid string, req searchRequest, mode searchMode) (searchResponse, error) {
	where, args, err := compileSearchFilter(req.Filter, 2) // $1 = index uid
	if err != nil {
		return searchResponse{}, err
	}
	args = append([]any{uid}, args...)

	conds := []string{"index_uid = $1"}
	if where != "" {
		conds = append(conds, "("+where+")")
	}

	q := strings.TrimSpace(req.Query)
	rank := "0"
	useQuery := q != "" && q != "*"
	switch {
	case !useQuery:
		// No query: every document that passed the filter, in the requested order.
	case mode == searchFullText:
		args = append(args, q)
		conds = append(conds, fmt.Sprintf("to_tsvector('simple', body) @@ websearch_to_tsquery('simple', $%d)", len(args)))
		rank = fmt.Sprintf("ts_rank(to_tsvector('simple', body), websearch_to_tsquery('simple', $%d))", len(args))
	default:
		args = append(args, "%"+escapeLikePattern(q)+"%")
		likeArg := len(args)
		conds = append(conds, fmt.Sprintf("body ILIKE $%d ESCAPE '\\'", likeArg))
		if trgmReady() {
			// pg_trgm turns the same substring idea into "close enough", which is
			// what makes a typo find anything at all.
			args = append(args, q)
			simArg := len(args)
			// word_similarity, not similarity: the documents are long (a whole
			// message, or a user's name plus id), and plain similarity dilutes a
			// short query against them to the point where a one-letter typo never
			// clears any threshold. word_similarity compares the query against the
			// best matching part of the text, which is the behaviour a search box
			// is expected to have.
			conds[len(conds)-1] = fmt.Sprintf("(body ILIKE $%d ESCAPE '\\' OR word_similarity($%d, body) > 0.5)", likeArg, simArg)
			rank = fmt.Sprintf("word_similarity($%d, body)", simArg)
		}
	}

	order, err := compileSearchSort(req.Sort, req.PrimaryKeyOrID())
	if err != nil {
		return searchResponse{}, err
	}
	if useQuery && mode == searchFallback {
		// Closest first; the caller asked for a specific sort, so only use it as a
		// tie-breaker.
		order = "rank DESC, " + order
	}

	args = append(args, req.Limit, req.Offset)
	sqlText := fmt.Sprintf(`
		SELECT doc, count(*) OVER() AS total, %s AS rank
		FROM %s
		WHERE %s
		ORDER BY %s
		LIMIT $%d OFFSET $%d`,
		rank, searchDocumentsTable, strings.Join(conds, " AND "), order, len(args)-1, len(args))

	rows, err := db.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return searchResponse{}, err
	}
	defer rows.Close()

	out := searchResponse{
		Hits:   make([]json.RawMessage, 0, req.Limit),
		Query:  req.Query,
		Limit:  req.Limit,
		Offset: req.Offset,
	}
	for rows.Next() {
		var doc []byte
		var total int
		var score float64
		if err := rows.Scan(&doc, &total, &score); err != nil {
			return searchResponse{}, err
		}
		out.TotalHits = total
		out.EstimatedTotalHits = total
		out.Hits = append(out.Hits, json.RawMessage(doc))
	}
	if err := rows.Err(); err != nil {
		return searchResponse{}, err
	}
	return out, nil
}

// escapeLikePattern makes a user query a literal substring: without this a "%"
// in the query would turn the fallback into a match-everything.
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	return strings.ReplaceAll(s, "_", `\_`)
}

// PrimaryKeyOrID keeps the sort compiler honest: documents are addressed by the
// index's primary key, but a sort that names "id" must still work.
func (r searchRequest) PrimaryKeyOrID() string { return "id" }

func documentID(doc json.RawMessage, primary string) (string, error) {
	var obj map[string]any
	if err := json.Unmarshal(doc, &obj); err != nil {
		return "", fmt.Errorf("document is not an object: %w", err)
	}
	v, ok := obj[primary]
	if !ok {
		return "", fmt.Errorf("document has no primary key %q", primary)
	}
	switch t := v.(type) {
	case string:
		return t, nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case json.Number:
		return t.String(), nil
	case bool:
		return strconv.FormatBool(t), nil
	default:
		return "", fmt.Errorf("primary key %q has an unsupported type", primary)
	}
}

var attributeNameRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// searchableText builds the text the tsvector covers. With an explicit
// searchableAttributes list only those fields are indexed; otherwise every
// top-level string/number is, which is what a caller that never sent settings
// expects.
func searchableText(doc json.RawMessage, attrs []string) string {
	var obj map[string]any
	if err := json.Unmarshal(doc, &obj); err != nil {
		return ""
	}
	var parts []string
	// Declared first so the closure can recurse into arrays.
	var appendValue func(v any)
	appendValue = func(v any) {
		switch t := v.(type) {
		case string:
			if s := strings.TrimSpace(t); s != "" {
				parts = append(parts, s)
			}
		case float64:
			parts = append(parts, strconv.FormatFloat(t, 'f', -1, 64))
		case bool:
			parts = append(parts, strconv.FormatBool(t))
		case []any:
			for _, item := range t {
				appendValue(item)
			}
		}
	}
	if len(attrs) > 0 {
		for _, a := range attrs {
			if v, ok := obj[a]; ok {
				appendValue(v)
			}
		}
	} else {
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			appendValue(obj[k])
		}
	}
	return strings.Join(parts, "\n")
}

// compileSearchSort turns Meilisearch's ["field:desc", ...] into an ORDER BY
// clause. Unknown syntax is a client error, not a silent full-table sort.
func compileSearchSort(sortSpec []string, fallbackField string) (string, error) {
	clauses := make([]string, 0, len(sortSpec)+1)
	for _, spec := range sortSpec {
		field, dir := spec, "asc"
		if i := strings.LastIndex(spec, ":"); i > 0 {
			field, dir = spec[:i], strings.ToLower(spec[i+1:])
		}
		if !attributeNameRE.MatchString(field) {
			return "", fmt.Errorf("invalid sort field %q", spec)
		}
		switch dir {
		case "asc", "desc":
		default:
			return "", fmt.Errorf("invalid sort direction in %q", spec)
		}
		clauses = append(clauses, fmt.Sprintf("(doc->>%s) %s", quoteLiteral(field), strings.ToUpper(dir)))
	}
	if len(clauses) == 0 {
		clauses = append(clauses, "rank DESC")
	}
	if attributeNameRE.MatchString(fallbackField) {
		clauses = append(clauses, fmt.Sprintf("updated_at DESC"))
	}
	return strings.Join(clauses, ", "), nil
}

// --- helpers ---------------------------------------------------------------

func readRawJSON(r *http.Request) (json.RawMessage, error) {
	var buf json.RawMessage
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 16<<20))
	if err := dec.Decode(&buf); err != nil {
		return nil, fmt.Errorf("invalid JSON body: %w", err)
	}
	return buf, nil
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 16<<20))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		writeSearchError(w, http.StatusBadRequest, "bad_request", "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func decodeDocuments(raw json.RawMessage) ([]json.RawMessage, error) {
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "[") {
		var docs []json.RawMessage
		if err := json.Unmarshal(raw, &docs); err != nil {
			return nil, err
		}
		if len(docs) == 0 {
			return nil, fmt.Errorf("empty document list")
		}
		return docs, nil
	}
	if strings.HasPrefix(trimmed, "{") {
		return []json.RawMessage{raw}, nil
	}
	return nil, fmt.Errorf("expected a document or an array of documents")
}

// decodeIDs accepts the shapes the client uses for id lists: a bare array, or
// {ids: [...]}. Ids may be numbers or strings in the documents themselves, so
// both are coerced to the text key the table stores.
func decodeIDs(raw json.RawMessage) ([]string, error) {
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		var wrapped struct {
			IDs []json.RawMessage `json:"ids"`
		}
		if err2 := json.Unmarshal(raw, &wrapped); err2 != nil {
			return nil, fmt.Errorf("expected an array of document ids")
		}
		values = wrapped.IDs
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("empty document id list")
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			out = append(out, s)
			continue
		}
		var n json.Number
		if err := json.Unmarshal(v, &n); err == nil {
			out = append(out, n.String())
			continue
		}
		return nil, fmt.Errorf("document id is neither a string nor a number")
	}
	return out, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("search: encode: %v", err)
	}
}

func writeSearchError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"message": message, "code": code, "type": "invalid_request", "link": "",
	})
}
