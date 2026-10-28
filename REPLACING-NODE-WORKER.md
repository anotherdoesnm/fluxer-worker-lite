# Replacing the node worker with worker-lite

This is the operator-facing document: whether worker-lite can stand in for the
upstream worker, what it does not do, how to switch, how to verify, how to roll
back, and what to do when the api is upgraded.

State: 2026-10-28, against Fluxer API release `2026.1027.210447`.

## Short answer

**Not drop-in.** It speaks the same queue with the same envelope and consumers, so it
is a *swap*, not a rewrite of your stack — but it implements 55 of the 78 tasks the
spec knows, and the other 23 are ignored on purpose, each with a reason in
`OMISSIONS.md` §2. Nothing is parked: every task is either handled or deliberately
dropped. Whether that is acceptable depends on what your instance has turned on:

| group (count) | why it is ignored here | when it matters to you |
|---|---|---|
| billing, premium, Stripe (6) | self-hosted has no payment provider, and upstream registers these crons only when `!Config.instance.selfHosted` | you run a payment provider — then those tasks are not implemented at all |
| app-store purchases (6) | App Store / Google Play purchases are cloud-only | you ship a mobile build that buys through a store |
| operator pass (5) | operator pass is a cloud feature | you hand out operator passes |
| blocklist feeds (2) | `FLUXER_*_BLOCKLIST*` feeds are disabled in `.env` | you enable the URL or SHA blocklist feeds |
| guild-visit analytics (3) | visits are not collected on this instance | you turn visit tracking on |
| cache purge (1) | `FLUXER_CACHE_PURGE_ADAPTER` is unset (default `none`, so the api queues nothing either) | you plug a purge adapter in |

Everything else — message lifecycle, bulk operations, threads, guild and user
deletion, data exports, crosspost, report and attachment maintenance, audit logs and
**all seven search-indexing tasks** — is ported.

An earlier revision of this document said search indexing was ignored because the
instance had no search backend. That is no longer true: the worker carries a
Meilisearch-compatible surface over Postgres full-text search, and the api writes its
index calls to it. See *Search* below.

## Search

The worker is the search backend. The api is pointed at it by a compose overlay that
loads **after** `docker-compose.no-search.yml` (which would otherwise set
`FLUXER_SEARCH_ENGINE=none`):

```yaml
# docker-compose.search-shim.yml
services:
  api:
    environment:
      FLUXER_SEARCH_ENGINE: meilisearch   # the engine name the api's adapter speaks
      FLUXER_SEARCH_URL: http://fluxer-worker-lite:9099
      FLUXER_SEARCH_API_KEY: ""
      FLUXER_SEARCH_USERNAME: ""
```

and by listing that file in `COMPOSE_FILE` after the no-search one. The worker side is
`SEARCH_API_ENABLED=1` (`deploy.sh` passes it by default); with `0` the status port
serves `/status` only.

How it answers. Two passes over one table of documents (`search_documents`, one row per
indexed object):

1. `websearch_to_tsquery` + `ts_rank` over the generated `tsvector` — this is what
   orders ordinary queries;
2. if that returns nothing, `ILIKE` plus `word_similarity(...) > 0.5` for substring and
   typo matches. `pg_trgm` is installed on the first write, best effort: since
   PostgreSQL 13 the extension is trusted, so the instance's own role can create it.
   Where that fails the worker logs it and keeps the `ILIKE` pass — substring still
   works, typo tolerance is lost.

Indexes served: `messages`, `threads`, `guilds`, `users`, `guild_members`,
`audit_logs`, `reports`.

What to expect from the api side: `POST /search/messages` needs `context_channel_id` /
`context_guild_id` in the body (snake_case) or the api answers as if it had no
permission context. And until a channel's messages are written, the answer is
`{"indexing": true}` — that flag follows `indexed_at` in the channel row, not the
worker's health.

What the surface does not do (all of it is a deliberate subset of Meilisearch):

- no facets — the response simply has no `facetDistribution`, which the adapter treats
  as optional;
- ranking is `ts_rank`, then `word_similarity`, not Meilisearch's ranking rules, so
  relevance differs on ties;
- `GET /indexes` (the listing) is not served — only `GET /indexes/{uid}`. Anything that
  enumerates indexes rather than addressing them by name will get 404/405;
- filter support is a subset of the Meilisearch dialect (`=`, `!=`, `>`, `>=`, `<`,
  `<=`, `EXISTS`, `NOT EXISTS`, `IS [NOT] NULL`, `AND`/`OR`/`NOT`);
- the `reports` document mapping has not been checked against real report rows on this
  instance — the index exists and stays empty because there is nothing to put in it.

## Getting the image

Three ways, in order of what an operator usually has:

1. **Build from this repository** — `docker build -t fluxer-worker-lite:dev .`.
2. **Load a tarball** — `gunzip -c worker-lite-2026.1027.210447.tar.gz | docker load`.
   That is how the image for this instance is shipped (8.7 MiB compressed, 21 MiB
   loaded); the file name carries the api release it was pinned to.
3. **Push to your registry** — `sh publish.sh registry.example/fluxer-worker-lite`
   tags it with the spec release and `latest` and pushes both. Pushing needs a token
   with **package write**; a repository-scoped token answers
   `unauthorized: reqPackageAccess` even when the login itself succeeded.

The image is a static binary on Alpine: no shell, no package manager, 20 MiB. That is
also why the compose file has no container healthcheck — probe `/status` from the host.

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
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | the api's Postgres (the `fluxer_kv` table, plus `search_documents`/`search_indexes` once search runs) |
| `VALKEY_ADDR`, `VALKEY_PASSWORD` | the api's cache |
| `FLUXER_S3_ENDPOINT`, `FLUXER_S3_BUCKET_CDN`, `FLUXER_S3_ACCESS_KEY_ID`, `FLUXER_S3_SECRET_ACCESS_KEY`, `FLUXER_S3_REGION` | object storage (asset deletion, exports) |
| `FLUXER_MEDIA_ENDPOINT`, `FLUXER_MEDIA_PROXY_SECRET_KEY`, `FLUXER_ATTACHMENT_URL_SECRET` | signing attachment URLs it hands to the api's message service |
| `API_DIGEST`, `API_BUILD_VERSION` | lets the worker say whether its task table matches the running api |
| `STATUS_ADDR`, `MAX_RSS_MB`, `STATS_SEC`, `LOG_TASKS` | `/status` endpoint and its warning threshold |
| `SEARCH_API_ENABLED` | serves the Meilisearch-compatible surface (`/indexes`, `/tasks`) on the status port; on by default, because the api's `FLUXER_SEARCH_URL` points here |

One operational note on the database role: creating `pg_trgm` and the GIN index on
first write needs `CREATE` on the database (or the extension pre-created by an admin).
Without it search still answers, minus typo tolerance.

## Switching

1. **Build or pull the image.** Either `docker build -t fluxer-worker-lite:dev .` from
   this repository, or use the image your registry serves.
2. **Turn the node worker off.** With the stock compose file that is
   `FLUXER_API_EMBEDDED_WORKER=false` on the api service (a restart of the api is
   enough); if you run the worker as its own container, stop that container.
3. **Start worker-lite** (compose example: `docker-compose.worker-lite.yml`, or
   `sh deploy.sh` on a host with the api container present). It recreates the
   `workers_*_go` consumers on first start.
4. **Point the api's search at it** — add `docker-compose.search-shim.yml` to
   `COMPOSE_FILE` *after* `docker-compose.no-search.yml` and recreate the api.
5. **Verify** — see the checklist below — and leave it running for a day before
   deleting the old worker definition.

## Verification checklist

```sh
curl -s http://127.0.0.1:9099/status | jq '.api, .counters, .crons, .parked_stream'
```

- `api.matches_spec` is `true`. If it is `false`, the worker's task table was generated
  for a different api release — see *Upgrading*.
- `counters.parked` stays `0`. A parked task means the worker has no handler for
  something the api enqueues; the copy is safe in `JOBS_PARKED`, but it is not work
  being done.
- `counters.unknown` stays `0` as well: it counts tasks that are in no lane.
- `crons.registered` equals the number of cron entries in the spec (28), and
  `crons.scheduled` is what passed the gates (16 on this instance: the rest belong to
  payments, store purchases and other features that are off).
- `parked_stream.messages` is `0` — that is the backlog of the port, not a counter.
- Consumers exist and are moving: `nats consumer ls JOBS` shows `workers_batch_go`,
  `workers_crosspost_go`, `workers_lifecycle_go`, `workers_realtime_go`,
  `workers_unfurl_go`.

Search, end to end — the worker side first, then the api side:

```sh
# an index answers with its primary key
curl -s http://127.0.0.1:9099/indexes/messages | jq -c '{uid, primaryKey}'

# and returns hits for an ordinary word
curl -s -X POST http://127.0.0.1:9099/indexes/messages/search \
     -H 'Content-Type: application/json' -d '{"q":"the"}' | jq '.hits | length'

# a prefix that no full-text pass would match exercises the pg_trgm fallback
curl -s -X POST http://127.0.0.1:9099/indexes/users/search \
     -H 'Content-Type: application/json' -d '{"q":"doesn"}' | jq '.hits | length'
```

A functional check that exercises the queue end to end: `go run ./cmd/e2e` with a bot
token — it creates a channel, posts and deletes three messages, and waits for the
worker to collapse them into one `MESSAGE_BULK_DELETE` audit entry.

The test suite is the other half of the contract: `go test ./cmd/worker`, and
`FLUXER_TEST_PG_DSN=... go test ./cmd/worker -run TestSearchStore` for the parts that
need a real Postgres (CI runs them as a separate job).

## Upgrading the api

`cmd/worker/version.go` pins the release the task table was generated from
(`specRelease`, `specAPIBuild`) and the image digest it was read out of
(`specAPIDigest`). After an api upgrade the worker keeps running, but the log says:

```
ALERT new release: published fluxer-api is <new>, running <old> — upgrade when convenient
```

That line compares against the **published self-host bundle**
(`https://fluxer.dev/compose/latest`), not against the newest tag in the registry. The
two can disagree: `ghcr.io/fluxerapp/fluxer-api:v1` moved to `2026.1028.143032` while
the bundle still pinned `2026.1027.210447`. Pin what the bundle pins — that is the
release upstream actually tests as a compose stack, and it is the one that silences the
alert.

To move the table forward:

1. Extract the lane configuration from the new image — it ships TypeScript sources:
   `docker cp $(docker create <image>):/usr/src/app/fluxer_api/src/api/worker/WorkerLaneConfig.ts .`
   (and `WorkerMain.ts` for the cron list).
2. Compare its `tasks`, `retiredTasks` and `concurrency` per lane with `cmd/worker/spec.go`;
   adjust the lanes, and update `specRelease`/`specAPIBuild`/`specAPIDigest` in
   `cmd/worker/version.go`.
3. Run `go test ./cmd/worker`. Three tests hold the table together:
   `TestEveryTaskIsAccountedFor` fails on a task that is neither handled nor ignored,
   `registry_test.go` fails when a task listed as handled in `OMISSIONS.md` §1 has no
   handler, and `TestEveryIgnoredTaskIsDocumented` fails when `policy.go` ignores
   something §2 does not name.
4. Port what is newly enqueued, or add it to `ignoredTasks` with a reason in
   `OMISSIONS.md`, rebuild, redeploy.

The task contract does not change with every release: `2026.1026.151750`,
`2026.1027.210447` and `2026.1028.143032` carry byte-identical `src/api/worker/tasks`
and `WorkerLaneConfig.ts`. When a diff of those trees is empty, the upgrade is a digest
bump and nothing else.

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
4. Drop `docker-compose.search-shim.yml` from `COMPOSE_FILE` and recreate the api — with
   the overlay gone, `docker-compose.no-search.yml` puts search back to `none`, which
   is the stock self-host behaviour.
5. Anything worker-lite parked stays in `JOBS_PARKED`; the node worker will re-receive
   jobs that were never acked, and parked copies can be replayed or discarded by hand.

## What it deliberately does not do

No api surface, no schema changes, no migrations, no payment provider, no email. Those
are the api's. `OMISSIONS.md` §3 lists the per-task gaps inside the ported handlers (for
example: crosspost copies are delivered without the follower-permission check, and the
data exports do not bundle attachments). Search is a working substitute, not a
Meilisearch: the differences are listed under *Search* above.
