// worker-lite: a Go replacement for the Fluxer worker, built to the same wire
// contract (jobs.<task> subjects, {payload, run_at, created_at} envelope,
// dlq.<task> for failures, /tmp/fluxer-worker-heartbeat every 5 s).
//
// Safety rules, learned from watching other projects leak:
//   - fixed-size fetch batches, never a goroutine per message without a
//     per-lane semaphore (concurrency comes from the consumer, not from spawns)
//   - every handler runs under a context with a deadline
//   - the whole process exits when RSS crosses MAX_RSS_MB, so a leak is a
//     visible crash instead of a silent OOM later
//   - restart policy is "no" in compose: a dead worker must look dead
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/robfig/cron/v3"
)

var (
	seen     int64
	handled  int64
	nakked   int64
	dlqed    int64
	unknown  int64
	realRuns int64
	stubRuns int64
	parked   int64
	lastRSS  int64
	started  = time.Now()
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func rssMB() int64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return 0
	}
	pages, _ := strconv.ParseInt(f[1], 10, 64)
	return pages * int64(os.Getpagesize()) / (1024 * 1024)
}

// ---------- heartbeat ----------

type hbComponent struct {
	Name         string
	LastReported atomic.Int64 // unix nanos
	StaleAfter   time.Duration
}

func newComponent(name string, stale time.Duration) *hbComponent {
	c := &hbComponent{Name: name, StaleAfter: stale}
	c.LastReported.Store(time.Now().UnixNano())
	return c
}

func (c *hbComponent) report() { c.LastReported.Store(time.Now().UnixNano()) }

func (c *hbComponent) age() time.Duration {
	return time.Since(time.Unix(0, c.LastReported.Load()))
}

type envelope struct {
	Payload   map[string]any `json:"payload"`
	RunAt     string         `json:"run_at,omitempty"`
	CreatedAt string         `json:"created_at,omitempty"`
}

func main() {
	natsURL := env("NATS_URL", "nats://nats:4222")
	stream := env("STREAM", "JOBS_GO")
	jobPrefix := env("JOB_PREFIX", "jobs.go.")
	dlqStream := env("DLQ_STREAM", "JOBS_GO_DLQ")
	dlqPrefix := env("DLQ_PREFIX", "dlq.go.")
	hbPath := env("HEARTBEAT_PATH", "/tmp/fluxer-worker-heartbeat")
	maxRSS := int64(envInt("MAX_RSS_MB", 96))
	statsEvery := time.Duration(envInt("STATS_SEC", 30)) * time.Second

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("worker-lite start stream=%s prefix=%s heartbeat=%s max_rss=%dMB", stream, jobPrefix, hbPath, maxRSS)

	nc, err := nats.Connect(natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, e error) { log.Printf("nats: disconnect %v", e) }),
		nats.ReconnectHandler(func(c *nats.Conn) { log.Printf("nats: reconnected %s", c.ConnectedUrl()) }),
	)
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	ensureStream(js, stream, jobPrefix)
	ensureStream(js, dlqStream, dlqPrefix)

	components := map[string]*hbComponent{}
	for _, l := range lanes {
		ensureConsumer(js, stream, l, jobPrefix)
		components["lane:"+l.Name] = newComponent("lane:"+l.Name, 30*time.Second)
	}
	cronComp := newComponent("cron", 90*time.Second)
	components["cron"] = cronComp

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, l := range lanes {
		for i := 0; i < l.Concurrency; i++ {
			sub, err := js.PullSubscribe("", l.Consumer, nats.Bind(stream, l.Consumer))
			if err != nil {
				log.Fatalf("pull subscribe %s: %v", l.Consumer, err)
			}
			go laneLoop(ctx, l, sub, js, jobPrefix, dlqPrefix, components["lane:"+l.Name])
		}
	}

	c := cron.New(cron.WithSeconds())
	for _, cs := range crons {
		task := cs.Task
		if _, err := c.AddFunc(cs.Expr, func() {
			cronComp.report()
			if err := publishJob(js, jobPrefix, task, map[string]any{"cron": true}, time.Time{}); err != nil {
				log.Printf("cron %s: publish failed: %v", task, err)
			}
		}); err != nil {
			log.Fatalf("cron %s (%s): %v", task, cs.Expr, err)
		}
	}
	c.Start()
	defer c.Stop()
	log.Printf("cron: %d entries scheduled", len(crons))

	go heartbeatLoop(ctx, hbPath, components)
	go statsLoop(ctx, statsEvery, maxRSS, nc, stream, jobPrefix)

	if envInt("SEED_COUNT", 0) > 0 {
		seed(js, jobPrefix, envInt("SEED_COUNT", 0))
	}
	if tasks := env("SEED_TASKS", ""); tasks != "" {
		for _, t := range strings.Split(tasks, ",") {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			if err := publishJob(js, jobPrefix, t, map[string]any{"seed": "explicit"}, time.Time{}); err != nil {
				log.Printf("seed task %s: %v", t, err)
			} else {
				log.Printf("seed task %s published", t)
			}
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down: seen=%d handled=%d nak=%d dlq=%d unknown=%d uptime=%s",
		seen, handled, nakked, dlqed, unknown, time.Since(started).Round(time.Second))
}

func ensureStream(js nats.JetStreamContext, name, prefix string) {
	cfg := &nats.StreamConfig{
		Name:      name,
		Subjects:  []string{prefix + ">"},
		Retention: nats.WorkQueuePolicy,
		MaxAge:    24 * time.Hour,
	}
	if _, err := js.StreamInfo(name); err == nil {
		return
	}
	if _, err := js.AddStream(cfg); err != nil {
		log.Fatalf("add stream %s: %v", name, err)
	}
	log.Printf("stream %s created", name)
}

func ensureConsumer(js nats.JetStreamContext, stream string, l laneSpec, prefix string) {
	filters := make([]string, 0, len(l.Tasks))
	for _, t := range l.Tasks {
		filters = append(filters, prefix+t)
	}
	cfg := &nats.ConsumerConfig{
		Durable:        l.Consumer,
		FilterSubjects: filters,
		AckPolicy:      nats.AckExplicitPolicy,
		MaxAckPending:  l.MaxAckPending,
		AckWait:        time.Duration(l.AckWaitSec) * time.Second,
		MaxDeliver:     l.MaxDeliver,
		DeliverPolicy:  nats.DeliverAllPolicy,
	}
	if _, err := js.AddConsumer(stream, cfg); err != nil {
		if _, uerr := js.UpdateConsumer(stream, cfg); uerr != nil {
			log.Fatalf("consumer %s: add=%v update=%v", l.Consumer, err, uerr)
		}
	}
	log.Printf("consumer %s ready (%d filters, concurrency %d)", l.Consumer, len(filters), l.Concurrency)
}

func publishJob(js nats.JetStreamContext, prefix, task string, payload map[string]any, runAt time.Time) error {
	env := envelope{Payload: payload, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if !runAt.IsZero() {
		env.RunAt = runAt.UTC().Format(time.RFC3339Nano)
	}
	body, _ := json.Marshal(env)
	_, err := js.Publish(prefix+task, body)
	return err
}

func laneLoop(ctx context.Context, l laneSpec, sub *nats.Subscription, js nats.JetStreamContext, jobPrefix, dlqPrefix string, hb *hbComponent) {
	timeout := time.Duration(l.AckWaitSec) / 2 * time.Second
	if timeout < 5*time.Second {
		timeout = 5 * time.Second
	}
	for ctx.Err() == nil {
		msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
		hb.report()
		if err == nats.ErrTimeout {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("lane %s: fetch: %v", l.Name, err)
			time.Sleep(time.Second)
			continue
		}
		for _, m := range msgs {
			atomic.AddInt64(&seen, 1)
			task := strings.TrimPrefix(m.Subject, jobPrefix)
			handleMessage(js, l, m, task, timeout, dlqPrefix, jobPrefix)
		}
	}
}

func handleMessage(js nats.JetStreamContext, l laneSpec, m *nats.Msg, task string, timeout time.Duration, dlqPrefix, jobPrefix string) {
	var env envelope
	if err := json.Unmarshal(m.Data, &env); err != nil {
		toDLQ(js, dlqPrefix, task, nil, fmt.Sprintf("bad envelope: %v", err), l.Name, m)
		return
	}
	if env.RunAt != "" {
		if at, err := time.Parse(time.RFC3339Nano, env.RunAt); err == nil {
			if d := time.Until(at); d > 0 {
				d2 := d
				if d2 > 30*time.Second {
					d2 = 30 * time.Second
				}
				_ = m.NakWithDelay(d2)
				atomic.AddInt64(&nakked, 1)
				return
			}
		}
	}
	if _, ok := knownTask(task); !ok {
		atomic.AddInt64(&unknown, 1)
		toDLQ(js, dlqPrefix, task, env.Payload, "unknown task type for this worker", l.Name, m)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := runTask(ctx, task, env.Payload); err != nil {
		if errors.Is(err, errNotImplemented) {
			delay := time.Duration(envInt("PARK_DELAY_SEC", 30)) * time.Second
			_ = m.NakWithDelay(delay)
			return
		}
		log.Printf("task %s failed: %v (nack)", task, err)
		_ = m.Nak()
		atomic.AddInt64(&nakked, 1)
		return
	}
	if err := m.Ack(); err != nil {
		log.Printf("task %s: ack failed: %v", task, err)
		return
	}
	atomic.AddInt64(&handled, 1)
}

// errNotImplemented marks a task this worker cannot perform yet. Such a job is
// NOT acked: acking would delete it from the workqueue stream and silently drop
// the work upstream expects to happen (deletions, expiry, unfurls). It is
// parked with a delayed nack instead, so it ages until the handler exists.
var errNotImplemented = errors.New("task not implemented in worker-lite")

// handlers maps a task name to its ported implementation. Everything not in
// here is parked, and the stats line reports how many tasks are real and how
// many are stubs — no silent gaps.
var handlers = map[string]func(context.Context, map[string]any) error{
	"prunePostgresKvTtl": taskPrunePostgresKvTtl,
}

func runTask(ctx context.Context, task string, payload map[string]any) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if h, ok := handlers[task]; ok {
		atomic.AddInt64(&realRuns, 1)
		return h(ctx, payload)
	}
	atomic.AddInt64(&stubRuns, 1)
	atomic.AddInt64(&parked, 1)
	logParkedOnce(task)
	return errNotImplemented
}

var parkedLogged sync.Map

func logParkedOnce(task string) {
	if _, loaded := parkedLogged.LoadOrStore(task, true); !loaded {
		log.Printf("PARK task %s is not implemented yet; jobs are kept in the stream (delayed nack)", task)
	}
}

func toDLQ(js nats.JetStreamContext, dlqPrefix, task string, payload map[string]any, reason, lane string, m *nats.Msg) {
	meta, _ := m.Metadata()
	seq := uint64(0)
	deliveries := uint64(0)
	if meta != nil {
		seq = meta.Sequence.Stream
		deliveries = meta.NumDelivered
	}
	body, _ := json.Marshal(map[string]any{
		"original_subject": m.Subject,
		"original_seq":     seq,
		"payload":          payload,
		"error_message":    reason,
		"delivery_count":   deliveries,
		"lane":             lane,
		"failed_at":        time.Now().UTC().Format(time.RFC3339Nano),
	})
	if _, err := js.Publish(dlqPrefix+task, body); err != nil {
		log.Printf("dlq publish %s: %v (nack, will retry)", task, err)
		_ = m.Nak()
		return
	}
	atomic.AddInt64(&dlqed, 1)
	log.Printf("dlq: %s -> %s (%s)", task, dlqPrefix+task, reason)
	_ = m.Ack()
}

// streamSubjects asks the JetStream API for the per-subject message counts.
// nats.go v1.37 has no SubjectsFilter option on StreamInfo, so this is a raw
// request; without it the server omits the subject map entirely.
func streamSubjects(nc *nats.Conn, stream, prefix string) map[string]uint64 {
	body, _ := json.Marshal(map[string]string{"subjects_filter": prefix + ">"})
	resp, err := nc.Request("$JS.API.STREAM.INFO."+stream, body, 5*time.Second)
	if err != nil {
		return nil
	}
	var out struct {
		State struct {
			Subjects map[string]uint64 `json:"subjects"`
		} `json:"state"`
	}
	if json.Unmarshal(resp.Data, &out) != nil {
		return nil
	}
	return out.State.Subjects
}

func heartbeatLoop(ctx context.Context, path string, comps map[string]*hbComponent) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	stalledLogged := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		type comp struct {
			Name  string `json:"name"`
			AgeMS int64  `json:"ageMs"`
		}
		out := struct {
			At         string `json:"at"`
			Components []comp `json:"components"`
		}{At: time.Now().UTC().Format(time.RFC3339Nano)}
		stalled := false
		for _, c := range comps {
			age := c.age()
			out.Components = append(out.Components, comp{Name: c.Name, AgeMS: age.Milliseconds()})
			if age > c.StaleAfter {
				stalled = true
			}
		}
		if stalled {
			if !stalledLogged {
				log.Printf("heartbeat stalled: components=%v", out.Components)
				stalledLogged = true
			}
			continue
		}
		stalledLogged = false
		b, _ := json.Marshal(out)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			log.Printf("heartbeat write %s: %v", path, err)
		}
	}
}

// statsLoop prints counters and sweeps the stream for subjects this worker
// does not know. That sweep is the real drift alarm: an upstream release can
// add jobs.<newTask>, and because every lane consumer filters by name, such a
// message is never delivered to anyone — it just ages out. Only the stream's
// own subject map reveals it.
func statsLoop(ctx context.Context, every time.Duration, maxRSS int64, nc *nats.Conn, stream, prefix string) {
	alerted := map[string]bool{}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rss := rssMB()
		lastRSS = rss
		log.Printf("stats seen=%d handled=%d nak=%d dlq=%d unknown=%d real=%d stub=%d parked=%d rss=%dMB uptime=%s",
			atomic.LoadInt64(&seen), atomic.LoadInt64(&handled), atomic.LoadInt64(&nakked),
			atomic.LoadInt64(&dlqed), atomic.LoadInt64(&unknown),
			atomic.LoadInt64(&realRuns), atomic.LoadInt64(&stubRuns), atomic.LoadInt64(&parked), rss,
			time.Since(started).Round(time.Second))
		for subj, count := range streamSubjects(nc, stream, prefix) {
			if !strings.HasPrefix(subj, prefix) {
				continue
			}
			task := strings.TrimPrefix(subj, prefix)
			if _, ok := knownTask(task); ok {
				continue
			}
			atomic.AddInt64(&unknown, 1)
			if !alerted[subj] {
				alerted[subj] = true
				log.Printf("ALERT unknown job subject %q (%d stored) — upstream added a task this worker cannot run", subj, count)
			}
		}
		if maxRSS > 0 && rss > maxRSS {
			log.Printf("FATAL rss %dMB over limit %dMB, exiting so the leak is visible", rss, maxRSS)
			os.Exit(1)
		}
	}
}

// seed publishes synthetic jobs (and one unknown task) so the skeleton can be
// proven without touching the production JOBS stream.
func seed(js nats.JetStreamContext, prefix string, n int) {
	all := make([]string, 0, 64)
	for _, l := range lanes {
		all = append(all, l.Tasks...)
	}
	for i := 0; i < n; i++ {
		task := all[i%len(all)]
		if err := publishJob(js, prefix, task, map[string]any{"seed": i}, time.Time{}); err != nil {
			log.Printf("seed %s: %v", task, err)
		}
	}
	_ = publishJob(js, prefix, "totallyUnknownTask", map[string]any{"seed": "unknown"}, time.Time{})
	log.Printf("seeded %d jobs + 1 unknown", n)
}
