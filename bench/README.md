# bench: what a worker costs

`bench.sh` measures the upstream node worker (`node dist/WorkerEntrypoint.js` from the
api image) against worker-lite on the same host, with the same burst.

## How it isolates itself

- **Its own NATS and valkey**, created per run and destroyed afterwards: the real queue
  is never touched, and a fresh broker avoids the "filtered consumer not unique" error
  that durable consumers cause when a second worker starts on the same stream.
- **Its own snowflakes service**: the node worker resolves `svc.snowflakes` over NATS at
  startup, so the bench broker gets its own copy of that stateless service.
- **The instance's real database.** A copy is not practical while the api holds
  connections to it (a template copy needs an idle source), and the burst task is a
  read-only no-op on an instance without reports. Startup backfills may therefore run
  against real data — they are idempotent by design (see `OMISSIONS.md`).

## Usage

```sh
sh bench/bench.sh                      # 45 s burst window, 40 jobs
sh bench/bench.sh --seconds 60 --jobs 100 --out results.md
```

Prints a markdown table: image size, idle memory (min/median), peak during the burst,
and how many jobs each worker saw. Jobs include the worker's own cron ticks, so the
counts are comparable between the two, not equal to `--jobs`.

## Measured (this instance, CT 150, 2026-10-27)

| implementation | image | idle MiB (min/med) | burst peak MiB | jobs seen |
|---|---|---|---|---|
| worker-lite (Go) | 20 | 3.4 / 4.5 | 9.9 | 48 |
| node worker (upstream) | 432 | 184.9 / 185.0 | 189.9 | 80 |

Second run, same settings: worker-lite 4.5 / 4.7 idle, 9.6 peak; node worker
184.5 / 185.8 idle, 188.7 peak. The node worker's floor is the dependency graph it
builds at startup (repositories, S3 clients, `sharp`, moderation caches), not the work
it does; worker-lite's floor is a Go binary with an idle connection pool.

Caveats worth repeating: the node worker covers all 78 tasks while worker-lite covers
48 (the rest are ignored on purpose, see `REPLACING-NODE-WORKER.md`), and the node
worker runs 15 lane workers against worker-lite's 58, so this is a comparison of two
running systems, not of two functions.
