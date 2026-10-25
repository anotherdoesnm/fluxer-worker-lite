package main

// /status — the window into what the worker is actually doing.
//
// Without it the parked stream is just a number: "0 parked" says nothing about
// which of the unported tasks are waiting on traffic and which never fire. The
// port is written task by task, so "what is next" has to be answered with
// counts, not guesses.
//
// The endpoint is loopback-only and JSON, because it is read by a person with
// curl far more often than by a monitor.

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// taskStat is per-task bookkeeping. Integers are atomics because lanes run
// concurrently; the last error needs a lock because it is a string.
type taskStat struct {
	Parked      atomic.Int64
	CronSkipped atomic.Int64
	Ignored     atomic.Int64
	Handled     atomic.Int64
	Failed      atomic.Int64

	mu          sync.Mutex
	lastError   string
	lastErrorAt time.Time
}

var taskStats sync.Map // task name -> *taskStat

func statFor(task string) *taskStat {
	if v, ok := taskStats.Load(task); ok {
		return v.(*taskStat)
	}
	v, _ := taskStats.LoadOrStore(task, &taskStat{})
	return v.(*taskStat)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// note* are the write side of the counters. Each call site also increments a
// global counter; these add the per-task dimension and never fail.
func noteHandled(task string)  { statFor(task).Handled.Add(1) }
func noteParked(task string)   { statFor(task).Parked.Add(1) }
func noteCronSkip(task string) { statFor(task).CronSkipped.Add(1) }
func noteIgnored(task string)  { statFor(task).Ignored.Add(1) }

func noteFailed(task string, err error) {
	st := statFor(task)
	st.Failed.Add(1)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.lastError = truncate(err.Error(), 300)
	st.lastErrorAt = time.Now().UTC()
}

func (st *taskStat) entry() map[string]any {
	e := map[string]any{}
	if n := st.Handled.Load(); n > 0 {
		e["handled"] = n
	}
	if n := st.Parked.Load(); n > 0 {
		e["parked"] = n
	}
	if n := st.CronSkipped.Load(); n > 0 {
		e["cron_skipped"] = n
	}
	if n := st.Ignored.Load(); n > 0 {
		e["ignored"] = n
	}
	if n := st.Failed.Load(); n > 0 {
		e["failed"] = n
	}
	st.mu.Lock()
	if st.lastError != "" {
		e["last_error"] = st.lastError
		e["last_error_at"] = st.lastErrorAt.Format(time.RFC3339)
	}
	st.mu.Unlock()
	return e
}

// statusServer serves /status and /healthz on addr.
func statusServer(addr string, js nats.JetStreamContext, parkStream string, cronScheduled, cronTotal int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		api := runningAPI()
		out := map[string]any{
			"spec_release": specRelease,
			"spec_digest":  shortDigest(specAPIDigest),
			"spec_build":   specAPIBuild,
			"api": map[string]any{
				"build":        api.Build,
				"digest":       shortDigest(api.Digest),
				"matches_spec": api.Digest == specAPIDigest,
			},
			"uptime_s": int64(time.Since(started).Seconds()),
			"rss_mb":   rssMB(),
			"counters": map[string]int64{
				"seen":    atomic.LoadInt64(&seen),
				"handled": atomic.LoadInt64(&handled),
				"real":    atomic.LoadInt64(&realRuns),
				"ignored": atomic.LoadInt64(&ignored),
				"retired": atomic.LoadInt64(&retired),
				"parked":  atomic.LoadInt64(&parked),
				"cron":    atomic.LoadInt64(&cronSkip),
				"nak":     atomic.LoadInt64(&nakked),
				"dlq":     atomic.LoadInt64(&dlqed),
				"unknown": atomic.LoadInt64(&unknown),
			},
			"crons": map[string]int{"scheduled": cronScheduled, "registered": cronTotal},
		}
		lanesOut := make([]map[string]any, 0, len(lanes))
		for _, l := range lanes {
			lanesOut = append(lanesOut, map[string]any{
				"name": l.Name, "consumer": l.Consumer,
				"tasks": len(l.Tasks), "retired": len(l.Retired),
			})
		}
		out["lanes"] = lanesOut

		// Parked depth is the backlog of the port: a task that never parks is
		// handled, ignored on purpose, or never published at all.
		if js != nil {
			if info, err := js.StreamInfo(parkStream); err == nil {
				out["parked_stream"] = map[string]any{
					"name":     parkStream,
					"messages": info.State.Msgs,
					"bytes":    info.State.Bytes,
				}
			}
		}
		tasksOut := map[string]any{}
		taskStats.Range(func(k, v any) bool {
			tasksOut[k.(string)] = v.(*taskStat).entry()
			return true
		})
		out["tasks"] = tasksOut

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			log.Printf("status: encode: %v", err)
		}
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("status: listening on %s/status", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("status: %v", err)
		}
	}()
	return srv
}
