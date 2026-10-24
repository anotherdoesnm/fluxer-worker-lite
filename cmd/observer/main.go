// fluxer-observer: read-only peek at the Fluxer worker queue.
//
// It does NOT create a consumer. JOBS is a workqueue stream, so any pull
// consumer needs an explicit ack policy, and acking on a workqueue stream
// REMOVES the message — that would steal jobs from the real workers. A
// consumer that never acks would instead compete with them for delivery.
//
// So it reads stored messages directly by sequence ($JS.API.STREAM.MSG.GET),
// which touches no consumer cursor and no ack state. Messages already consumed
// by the worker are simply gone (ErrMsgNotFound) and skipped.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

var printed int64
var seen int64
var skipped int64

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// shape renders a JSON value as a compact type sketch, depth- and width-capped
// so a huge payload can neither blow up the log nor the heap.
func shape(v any, depth int) string {
	switch t := v.(type) {
	case map[string]any:
		if depth <= 0 {
			return "{...}"
		}
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for i, k := range keys {
			if i >= 24 {
				parts = append(parts, fmt.Sprintf("...%d more", len(keys)-24))
				break
			}
			parts = append(parts, fmt.Sprintf("%q:%s", k, shape(t[k], depth-1)))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case []any:
		if len(t) == 0 {
			return "[]"
		}
		return "[" + shape(t[0], depth-1) + fmt.Sprintf(" x%d]", len(t))
	case string:
		return "string"
	case float64:
		if t == float64(int64(t)) {
			return "int"
		}
		return "float"
	case bool:
		return "bool"
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%T", t)
	}
}

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://nats:4222"
	}
	stream := os.Getenv("STREAM")
	if stream == "" {
		stream = "JOBS"
	}
	maxLogs := envInt("MAX_LOGS", 400)
	poll := time.Duration(envInt("POLL_MS", 3000)) * time.Millisecond

	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, e error) {
			fmt.Printf("nats: disconnect: %v\n", e)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			fmt.Printf("nats: reconnected to %s\n", c.ConnectedUrl())
		}),
	)
	if err != nil {
		fmt.Printf("nats: connect failed: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.MaxWait(5 * time.Second))
	if err != nil {
		fmt.Printf("jetstream: %v\n", err)
		os.Exit(1)
	}
	info, err := js.StreamInfo(stream)
	if err != nil {
		fmt.Printf("stream %s: %v\n", stream, err)
		os.Exit(1)
	}
	next := info.State.LastSeq + 1
	if v := envInt("START_FROM", 0); v > 0 {
		next = uint64(v)
	}
	fmt.Printf("observer: stream=%s from_seq=%d stored=%d\n", stream, next, info.State.Msgs)

	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for range ticker.C {
		info, err := js.StreamInfo(stream)
		if err != nil {
			fmt.Printf("streaminfo: %v\n", err)
			continue
		}
		if info.State.FirstSeq > next && info.State.Msgs > 0 {
			fmt.Printf("notice: trimmed %d -> %d\n", next, info.State.FirstSeq)
			next = info.State.FirstSeq
		}
		for next <= info.State.LastSeq {
			m, err := js.GetMsg(stream, next)
			if err != nil {
				atomic.AddInt64(&skipped, 1)
				next++
				continue
			}
			atomic.AddInt64(&seen, 1)
			if atomic.LoadInt64(&printed) < int64(maxLogs) {
				atomic.AddInt64(&printed, 1)
				var payload map[string]any
				sketch := "not-json"
				if json.Unmarshal(m.Data, &payload) == nil {
					sketch = shape(payload, 3)
				}
				fmt.Printf("job seq=%d subject=%s bytes=%d %s\n", m.Sequence, m.Subject, len(m.Data), sketch)
			}
			next = m.Sequence + 1
		}
	}
}
