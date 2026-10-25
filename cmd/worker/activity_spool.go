package main

// drainActivitySpool moves activity events that the API could not publish into
// the EVENTS stream.
//
// The shape of the problem, from the release's own sources:
//
//	src/api/worker/tasks/DrainActivitySpool.ts       the task: lock, then drain
//	src/api/infrastructure/activity/ActivitySpool.ts the queue: a Redis list
//	src/api/infrastructure/activity/ActivityEvents.ts the publisher and the modes
//	src/api/infrastructure/activity/Contract.generated.ts subjects, headers, TTLs
//
// Why a spool exists at all: when the EVENTS stream is missing (or a publish is
// rejected), the API cannot lose the event, so it RPUSHes it to the Redis list
// `activity:spool` with the subject and payload it would have published. The API
// itself refuses to drain (`drainActivitySpoolNow` returns null unless its mode
// is `publish`), because an API whose stream is missing would just spool the
// entries again. Draining belongs to the worker, which is the component that
// exists to be the scheduler.
//
// Two details that a naive port gets wrong:
//
//   - The lock is not decoration. Draining is publish-then-trim: two workers
//     doing it at once would publish the same entries twice. Upstream takes
//     `activity:spool:drain` for 30 s and extends it before every trim, so a
//     slow publish does not hand the batch to a second worker.
//   - A failed publish must not trim. The loop stops on the first publish error
//     and trims only what it actually consumed, so the entry that failed is the
//     first thing the next run sees. Trimming a batch after a partial publish is
//     how events disappear silently.
//
// The task is a cron tick, every 5 s, so it must be cheap when idle: one LLEN is
// the whole cost of an empty spool.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

const (
	// activitySpoolKey is ACTIVITY_SPOOL_KEY from ActivitySpool.ts. Redis keys in
	// this release are used raw, not prefixed (KVClient hands them to ioredis).
	activitySpoolKey = "activity:spool"
	// activityDrainLock / TTL mirror DrainActivitySpool.ts.
	activityDrainLock    = "activity:spool:drain"
	activityDrainLockTTL = 30 * time.Second
	// activityDrainBatch is DRAIN_BATCH; upstream reads and trims in batches of
	// 200 so one huge spool does not blow the 15 s budget in a single round.
	activityDrainBatch = 200
	// activityDrainBudget is DRAIN_BUDGET_MS: a soft deadline, not a hard limit.
	// Stopping early is fine — the next tick continues where this one stopped.
	activityDrainBudget = 15 * time.Second
	// activitySpoolMaxAge is ACTIVITY_SPOOL_MAX_AGE_MS: anything older than a week
	// is expired rather than published, because the events that consume it have
	// their own retention and a week-old login is noise.
	activitySpoolMaxAge = 7 * 24 * time.Hour
	// activityEventsStream is EVENTS_STREAM from Contract.generated.ts.
	activityEventsStream = "EVENTS"
	// Header names from HEADER in Contract.generated.ts. Nats-Msg-Id is also what
	// JetStream deduplicates on, which is why a republished entry is harmless.
	activityHeaderSpooled = "Fluxer-Spooled"
	activityHeaderTTL     = "Nats-TTL"
	activityHeaderMsgID   = "Nats-Msg-Id"
	// activityPublishTimeout is PUBLISH_TIMEOUT_MS.
	activityPublishTimeout = time.Second
)

// activityEntry is one element of the spool list (ActivitySpoolEntry).
//
// `data` is not an object: the API stores the serialized event, so a Go struct
// field of type string is the faithful representation, and it also means we can
// republish the bytes without re-encoding them and losing fields we do not model.
type activityEntry struct {
	Kind    string  `json:"kind"`
	Subject string  `json:"subject"`
	ID      string  `json:"id"`
	TTL     string  `json:"ttl"`
	AtMs    float64 `json:"at_ms"`
	Data    string  `json:"data"`
}

func parseActivityEntry(raw string) (activityEntry, bool) {
	var e activityEntry
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return e, false
	}
	// parseEntry in ActivitySpool.ts checks exactly these fields; a missing one
	// means the list holds something we do not understand, and publishing it
	// would put garbage into the stream.
	if e.Subject == "" || e.ID == "" || e.TTL == "" || e.Data == "" {
		return e, false
	}
	return e, true
}

type activityStreamState struct {
	Exists bool
	TTL    bool
}

var (
	activityStreamMu     sync.Mutex
	activityStreamCached activityStreamState
	activityStreamAt     time.Time
	activityStreamLogged bool
)

// activityStream inspects the EVENTS stream through the JetStream API.
//
// Two questions, and they fail differently: does the stream exist (without it
// there is nowhere to publish and the API is spooling instead), and does it allow
// per-message TTL (without that, every publish carrying Nats-TTL is rejected with
// "per-message TTL is disabled"). The second one is a deployment mistake that no
// amount of retrying fixes, so it is reported once and then skipped, not nacked
// twelve times a minute.
func activityStream(nc *nats.Conn) activityStreamState {
	activityStreamMu.Lock()
	defer activityStreamMu.Unlock()
	if time.Since(activityStreamAt) < time.Minute {
		return activityStreamCached
	}
	resp, err := nc.Request("$JS.API.STREAM.INFO."+activityEventsStream, nil, 5*time.Second)
	if err != nil {
		if !activityStreamLogged {
			activityStreamLogged = true
			log.Printf("activity spool: cannot read stream %s: %v (will retry)", activityEventsStream, err)
		}
		return activityStreamCached
	}
	var info struct {
		Config struct {
			AllowMsgTTL bool `json:"allow_msg_ttl"`
		} `json:"config"`
		Error struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	state := activityStreamState{}
	if json.Unmarshal(resp.Data, &info) == nil && info.Error.Code == 0 {
		state.Exists = true
		state.TTL = info.Config.AllowMsgTTL
	}
	if !state.Exists && !activityStreamLogged {
		activityStreamLogged = true
		log.Printf("activity spool: stream %s does not exist, so there is nowhere to drain into; entries stay in %s",
			activityEventsStream, activitySpoolKey)
	}
	if state.Exists && !state.TTL && !activityStreamLogged {
		activityStreamLogged = true
		log.Printf("activity spool: stream %s exists but has no per-message TTL (allow_msg_ttl); "+
			"recreate it with that option or the events cannot carry their Nats-TTL", activityEventsStream)
	}
	activityStreamCached, activityStreamAt = state, time.Now()
	return state
}

var extendLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('EXPIRE', KEYS[1], ARGV[2]) end
return 0`)

func activityLockToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// publishActivity republishes one spooled entry into the EVENTS stream.
//
// The payload bytes are reused as-is; the headers are the contract: Nats-Msg-Id
// makes a duplicate republish a no-op on the server, Nats-TTL carries the event's
// own lifetime, and Fluxer-Spooled tells consumers this event arrived late.
func publishActivity(ctx context.Context, js nats.JetStreamContext, e activityEntry) error {
	msg := &nats.Msg{Subject: e.Subject, Data: []byte(e.Data), Header: nats.Header{}}
	msg.Header.Set(activityHeaderMsgID, e.ID)
	msg.Header.Set(activityHeaderSpooled, "1")
	if e.TTL != "" && e.TTL != "never" {
		msg.Header.Set(activityHeaderTTL, e.TTL)
	}
	pctx, cancel := context.WithTimeout(ctx, activityPublishTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := js.PublishMsg(msg)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-pctx.Done():
		return fmt.Errorf("publish %s: %w", e.Subject, pctx.Err())
	}
}

// drainActivity runs one drain with the upstream loop: read a batch, publish what
// is publishable, trim exactly what was consumed, repeat until the spool is empty
// or the budget is spent. Returns the counts for the log line.
func drainActivity(ctx context.Context, js nats.JetStreamContext, lockToken string) (published, expired, failed int, remaining int64, err error) {
	deadline := time.Now().Add(activityDrainBudget)
	kv := valkey()
	for time.Now().Before(deadline) {
		batch, lerr := kv.LRange(ctx, activitySpoolKey, 0, activityDrainBatch-1).Result()
		if lerr != nil {
			err = fmt.Errorf("lrange %s: %w", activitySpoolKey, lerr)
			break
		}
		if len(batch) == 0 {
			break
		}
		consumed := 0
		for _, raw := range batch {
			if !time.Now().Before(deadline) {
				break
			}
			entry, ok := parseActivityEntry(raw)
			if !ok {
				// Upstream counts an unparseable element as failed and consumes it:
				// keeping it would make every future drain stop at the same element.
				failed++
				consumed++
				continue
			}
			at := time.UnixMilli(int64(entry.AtMs))
			if at.Before(time.Now().Add(-activitySpoolMaxAge)) {
				expired++
				consumed++
				continue
			}
			if perr := publishActivity(ctx, js, entry); perr != nil {
				// Stop without consuming: the entry stays at the head of the list and
				// the next tick retries it. This is the ordering that makes the spool
				// lossless.
				err = perr
				break
			}
			published++
			consumed++
		}
		if err == nil && consumed > 0 {
			// Extend before trim: a trim that outlives the lock is how a second
			// worker publishes the same batch.
			if kept, kerr := extendLockScript.Run(ctx, kv, []string{activityDrainLock}, lockToken,
				int(activityDrainLockTTL.Seconds())).Int(); kerr != nil {
				err = fmt.Errorf("extend %s: %w", activityDrainLock, kerr)
			} else if kept != 1 {
				err = fmt.Errorf("activity spool drain lock lost before trim")
			}
		}
		if err == nil && consumed > 0 {
			if terr := kv.LTrim(ctx, activitySpoolKey, int64(consumed), -1).Err(); terr != nil {
				err = fmt.Errorf("ltrim %s: %w", activitySpoolKey, terr)
			}
		}
		if err != nil || consumed < len(batch) {
			break
		}
	}
	if n, lerr := valkey().LLen(ctx, activitySpoolKey).Result(); lerr == nil {
		remaining = n
	}
	return published, expired, failed, remaining, err
}

// taskDrainActivitySpool is the handler behind the `drainActivitySpool` cron.
func taskDrainActivitySpool(ctx context.Context, _ map[string]any) error {
	js, err := workerNATS().JetStream()
	if err != nil {
		return fmt.Errorf("activity spool: jetstream: %w", err)
	}
	if st := activityStream(workerNATS()); !st.Exists || !st.TTL {
		return nil
	}
	token, err := activityLockToken()
	if err != nil {
		return fmt.Errorf("activity spool: token: %w", err)
	}
	ok, err := valkey().SetNX(ctx, activityDrainLock, token, activityDrainLockTTL).Result()
	if err != nil {
		return fmt.Errorf("activity spool: lock: %w", err)
	}
	if !ok {
		// Another worker is draining. Not an error, and not worth a log line every
		// five seconds.
		return nil
	}
	defer func() {
		if _, rerr := releaseLockScript.Run(context.Background(), valkey(), []string{activityDrainLock}, token).Result(); rerr != nil {
			log.Printf("activity spool: release lock: %v", rerr)
		}
	}()
	published, expired, failed, remaining, err := drainActivity(ctx, js, token)
	if published+expired+failed > 0 || err != nil {
		log.Printf("activity spool drained: published=%d expired=%d failed=%d remaining=%d err=%v",
			published, expired, failed, remaining, err)
	}
	return err
}
