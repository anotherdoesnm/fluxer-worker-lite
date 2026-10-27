# Replacing the node worker with worker-lite

This is the operator-facing document: whether worker-lite can stand in for the
upstream worker, what it does not do, how to switch, how to verify, how to roll
back, and what to do when the api is upgraded.

## Short answer

**Not drop-in.** It speaks the same queue with the same envelope and consumers, so it
is a *swap*, not a rewrite of your stack — but it implements 48 of the 78 tasks the
spec knows, and the other 30 are ignored on purpose, each with a reason in
`OMISSIONS.md` §2. Whether that is acceptable depends on which features your instance
has turned on:

| group (count) | why it is ignored here | when it matters to you |
|---|---|---|
| billing, premium, app-store (17) | self-hosted has no payment provider | you run Stripe or app-store purchases — then those tasks are not implemented at all |
| search indexing (7) | this instance has no search backend, so the api disables search | you run Elasticsearch/Meilisearch — channel/member/thread indexing would stop |
| blocklist feeds, visit analytics, cache purge (6) | switched off in this instance's `.env` | you enable the URL/file blocklist feeds, guild-visit analytics, or a cache purge adapter |

Everything else — message lifecycle, bulk operations, threads, guild and user
deletion, data exports, crosspost, report and attachment maintenance — is ported.

## The two workers are mutually exclusive

Both consume the **same** JetStream subjects (`jobs.<task>`) on the **same** workqueue
stream. A workqueue stream refuses two consumers with overlapping filters:

```
nats: filtered consumer not unique on workqueue stream
```

So it is a switch, not a side-by-side rollout: the api's embedded worker (or its
separate `dist/WorkerEntrypoint.js` process) must be off while worker-lite runs, and
vice versa. `FLUXER_API_EMBEDDED_WORKER=false` on the api container is what turns the
embedded one off; that is already the case on this instance.

## What the worker needs

It derives nothing from the api's config files; everything comes from the environment.
`deploy.sh` reads these off the live `fluxer-api-1` container, and the compose example
below shows the same set.

| variable | what it is |
|---|---|
| `NATS_URL` | JetStream address (`nats://nats:4222`) |
| `STREAM`, `JOB_PREFIX` | `JOBS`, `jobs.` |
| `DLQ_STREAM`, `DLQ_PREFIX`, `PARK_STREAM`, `PARK_PREFIX` | `JOBS_DLQ`/`dlq.`, `JOBS_PARKED`/`parked.` |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | the api's Postgres (the `fluxer_kv` table) |
| `VALKEY_ADDR`, `VALKEY_PASSWORD` | the api's cache |
| `FLUXER_S3_ENDPOINT`, `FLUXER_S3_BUCKET_CDN`, `FLUXER_S3_ACCESS_KEY_ID`, `FLUXER_S3_SECRET_ACCESS_KEY`, `FLUXER_S3_REGION` | object storage (asset deletion, exports) |
| `FLUXER_MEDIA_ENDPOINT`, `FLUXER_MEDIA_PROXY_SECRET_KEY`, `FLUXER_ATTACHMENT_URL_SECRET` | signing attachment URLs it hands to the api's message service |
| `API_DIGEST`, `API_BUILD_VERSION` | lets the worker say whether its task table matches the running api |
| `STATUS_ADDR`, `MAX_RSS_MB`, `STATS_SEC`, `LOG_TASKS` | `/status` endpoint and its warning threshold |

## Switching

1. **Build or pull the image.** Either `docker build -t fluxer-worker-lite:dev .` from
   this repository, or use the image your registry serves.
2. **Turn the node worker off.** With the stock compose file that is
   `FLUXER_API_EMBEDDED_WORKER=false` on the api service (a restart of the api is
   enough); if you run the worker as its own container, stop that container.
3. **Start worker-lite** (compose example: `docker-compose.worker-lite.yml`, or
   `sh deploy.sh` on a host with the api container present). It recreates the
   `workers_*_go` consumers on first start.
4. **Verify** — see the checklist below — and leave it running for a day before
   deleting the old worker definition.

## Verification checklist

```sh
curl -s http://127.0.0.1:9099/status | jq '.api, .counters, .parked_stream'
```

- `api.matches_spec` is `true`. If it is `false`, the worker's task table was generated
  for a different api release — see *Upgrading*.
- `counters.parked` stays `0`. A parked task means the worker has no handler for
  something the api enqueues; the copy is safe in `JOBS_PARKED`, but it is not work
  being done.
- `counters.unknown` stays `0` as well: it counts tasks that are in no lane.
- `crons.registered` equals the number of cron entries in the spec, and
  `crons.scheduled` is what passed the gates (features switched on).
- Consumers exist and are moving: `nats consumer ls JOBS` shows `workers_batch_go`,
  `workers_crosspost_go`, `workers_lifecycle_go`, `workers_realtime_go`,
  `workers_unfurl_go`.

A functional check that exercises the queue end to end: `go run ./cmd/e2e` with a bot
token — it creates a channel, posts and deletes three messages, and waits for the
worker to collapse them into one `MESSAGE_BULK_DELETE` audit entry.

## Upgrading the api

`cmd/worker/version.go` pins the release the task table was generated from
(`spec_release`). After an api upgrade the worker keeps running, but the log says:

```
ALERT new release: published fluxer-api is <new>, running <old> — upgrade when convenient
```

To move the table forward:

1. Extract the lane configuration from the new image — it ships TypeScript sources:
   `docker cp $(docker create <image>):/usr/src/app/fluxer_api/src/api/worker/WorkerLaneConfig.ts .`
2. Compare its `tasks`, `retiredTasks` and `concurrency` per lane with `cmd/worker/spec.go`;
   adjust the lanes, and update `specRelease`/`specAPIBuild` in `cmd/worker/version.go`.
3. Run `go test ./cmd/worker` — `TestEveryTaskIsAccountedFor` fails on any task that is
   neither handled nor ignored, so a new api task cannot be forgotten silently.
4. Port what is newly enqueued, or add it to `ignoredTasks` with a reason in
   `OMISSIONS.md`, rebuild, redeploy.

## Rolling back

1. Stop worker-lite.
2. Delete its durable consumers, otherwise the node worker cannot create its own:
   ```sh
   for c in workers_batch_go workers_lifecycle_go workers_crosspost_go \
            workers_realtime_go workers_unfurl_go; do
     nats --server "$NATS" consumer rm JOBS "$c" -f
   done
   ```
3. Set `FLUXER_API_EMBEDDED_WORKER=true` again (or start the node worker container).
4. Anything worker-lite parked stays in `JOBS_PARKED`; the node worker will re-receive
   jobs that were never acked, and parked copies can be replayed or discarded by hand.

## What it deliberately does not do

No api surface, no schema changes, no migrations, no search backend, no payment
provider, no email. Those are the api's. `OMISSIONS.md` §3 lists the per-task gaps
inside the ported handlers (for example: crosspost copies are delivered without the
follower-permission check, and the data exports do not bundle attachments).
