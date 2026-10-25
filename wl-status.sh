#!/bin/sh
# Read worker-lite's /status and print the parts that matter for the port: which
# tasks are waiting on real traffic, which only ever tick from cron, and whether
# the worker serves the API release its tables were built from.
#
#   wl-status           summary
#   wl-status --json    raw JSON
#   wl-status --tasks   every task with counters
set -e

url=${WL_URL:-http://127.0.0.1:9099/status}
body=$(curl -fsS "$url")

case "${1:-}" in
--json)
	printf '%s\n' "$body"
	exit 0
	;;
esac

python3 - "$body" "${1:-}" <<'PY'
import json
import sys

data = json.loads(sys.argv[1])
mode = sys.argv[2] if len(sys.argv) > 2 else ""
c = data["counters"]
api = data["api"]

print(f"spec  {data['spec_release']}  image {data['spec_digest']}  build {data['spec_build']}")
print(f"api   {api['build'] or '?'}  image {api['digest'] or '?'}  matches_spec={api['matches_spec']}")
print(f"up    {data['uptime_s']}s  rss {data['rss_mb']} MB  crons {data['crons']['scheduled']}/{data['crons']['registered']}")
ps = data.get("parked_stream") or {}
print(f"queue parked={ps.get('messages', 0)} msgs  "
      f"seen={c['seen']} handled={c['handled']} real={c['real']} cron={c['cron']} "
      f"ignored={c['ignored']} nak={c['nak']} dlq={c['dlq']} unknown={c['unknown']}")

tasks = data.get("tasks") or {}
if mode == "--tasks":
    for name in sorted(tasks):
        t = tasks[name]
        parts = [f"{k}={v}" for k, v in sorted(t.items()) if k != "last_error"]
        print(f"  {name:44s} " + " ".join(parts))
        if "last_error" in t:
            print(f"  {'':44s} last_error: {t['last_error']}")
else:
    parked = {n: t.get("parked", 0) for n, t in tasks.items() if t.get("parked")}
    if parked:
        print("\nwaiting on traffic (parked):")
        for n, v in sorted(parked.items(), key=lambda kv: -kv[1]):
            print(f"  {n:44s} {v}")
    cron = {n: t.get("cron_skipped", 0) for n, t in tasks.items() if t.get("cron_skipped")}
    if cron:
        print("\ncron-only, no handler (ticks dropped):")
        for n, v in sorted(cron.items(), key=lambda kv: -kv[1]):
            print(f"  {n:44s} {v}")
    handled = {n: t for n, t in tasks.items() if t.get("handled") or t.get("failed")}
    if handled:
        print("\nwith a handler:")
        for n in sorted(handled):
            t = handled[n]
            print(f"  {n:44s} handled={t.get('handled', 0)} failed={t.get('failed', 0)}")
            if "last_error" in t:
                print(f"  {'':44s} last_error: {t['last_error']}")
PY
