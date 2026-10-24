# fluxer-worker-lite

Experimental Go replacement for the Node `worker` of a self-hosted Fluxer stack.

## What it implements

Read from upstream `fluxer_api/src/api/worker`:

- five durable lane consumers with the exact names, task filters and limits
  (`workers_realtime` 2 tasks / conc 10 / ack 15 s, `workers_unfurl` 1 / 20 / 30 s,
  `workers_lifecycle` 26 / 8 / 60 s, `workers_batch` 29 / 12 / 120 s,
  `workers_crosspost` 5 / 8 / 60 s);
- the job envelope `{payload, run_at, created_at}` on subject `jobs.<task>`;
- dead letters on `dlq.<task>`;
- the 21 `cron.upsert()` entries from `WorkerMain.ts` (six fields, seconds first);
- the heartbeat file `/tmp/fluxer-worker-heartbeat`, same JSON shape, every 5 s.

## Status

- skeleton proven: consumers, cron, heartbeat, counters, ~7 MB RSS (Node: 182 MB);
- `prunePostgresKvTtl` ported for real (CTE delete on `fluxer_kv`, 20 × 5000
  batches) and verified against a live database;
- every other task is **parked**, not acked: a delayed nack keeps the job in the
  workqueue stream until the handler exists, because acking would silently drop
  work the API expects to happen.

## Two design findings

1. An unknown task type is never delivered to any consumer (lane filters are
   name-based), so drift cannot be caught at the handler. It is detected by
   comparing the stream's subject map (`$JS.API.STREAM.INFO` with
   `subjects_filter`) against the known task table; the worker logs
   `ALERT unknown job subject ...`.
2. Test on a separate stream. `JOBS` is a workqueue stream, so a second acking
   consumer would steal jobs from the real worker. Use `STREAM=JOBS_TEST`,
   `JOB_PREFIX=testjobs.` and the `*_go` consumer names.

## Build

Do not build inside the Fluxer container (1 vCPU / 1 GiB / no swap): a Go build
there OOM-killed `dockerd` and took the whole stack down. Build on a bigger box:

    podman build -t fluxer-worker-lite:dev .
    podman save fluxer-worker-lite:dev | gzip -1 > img.gz
    # on the target:
    docker load < img.gz

## Configuration

| var | default | meaning |
|---|---|---|
| `NATS_URL` | `nats://nats:4222` | JetStream endpoint |
| `STREAM` / `JOB_PREFIX` | `JOBS_GO` / `jobs.go.` | jobs stream (use a test namespace!) |
| `DLQ_STREAM` / `DLQ_PREFIX` | `JOBS_GO_DLQ` / `dlq.go.` | dead letters |
| `PGHOST` `PGPORT` `PGUSER` `PGPASSWORD` `PGDATABASE` | — | database for ported tasks |
| `MAX_RSS_MB` | `96` | exit when RSS crosses it, so a leak is visible |
| `PARK_DELAY_SEC` | `30` | redelivery delay for unimplemented tasks |
| `SEED_TASKS` | — | comma-separated tasks to publish for testing |
