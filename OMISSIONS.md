# What worker-lite does not implement

State of 2026-10-25, against Fluxer API release `2026.1025.144746` (digest `408429273dc8`).

Counts from `cmd/worker/spec.go`: **76 tasks**, of which 4 have handlers, 30 are ignored on purpose
(a map value with a reason in `cmd/worker/policy.go`), and **42 are parked**.

Two different things are listed here: whole tasks we do not run, and gaps *inside* the four handlers we do run.
A parked job is not lost — it is copied to `JOBS_PARKED` before the ack — except cron ticks, which are dropped
deliberately because the scheduler regenerates them. What "dropped" means for a missing handler is therefore:
the work simply never happens until the handler exists.

## 1. Handled today

| task | what it does | notes |
|---|---|---|
| `prunePostgresKvTtl` | deletes expired `fluxer_kv` rows; runs the two one-time legacy passes (default-TTL backfill, job-ledger expiry) | full port, including the marker-hash contract |
| `expireStaleJobs` | deadletters jobs stuck in `jobs_active` past 8 days, drops finished ones | full port |
| `extractEmbeds` | link previews: URL extraction, `svc.unfurl`, KV write, `MESSAGE_UPDATE` | see §3 for the parts left out |
| `drainActivitySpool` | moves spooled activity events from the Redis list `activity:spool` into the `EVENTS` stream, under the `activity:spool:drain` lock | no-op on this instance: the `EVENTS` stream does not exist, so it reports that once and returns (see §2.7) |

## 2. Not implemented — 42 tasks

### 2.1 Data rights and user lifecycle — the most consequential gap

| task | what breaks without it |
|---|---|
| `userProcessPendingDeletion`, `userProcessPendingDeletions` | a user asks to delete their account; the request sits in the queue forever |
| `messageShred` | messages that must disappear for good stay readable |
| `guildProcessDeletion`, `applicationProcessDeletion` | deleting a guild/app leaves its rows behind |
| `harvestUserData`, `harvestGuildData` | GDPR-style "export my data" requests never produce a file |

### 2.2 Attachments and storage

| task | what breaks |
|---|---|
| `expireAttachments`, `reconcileAttachmentDecay` | expired attachments are never removed from the bucket; S3 grows forever |
| `processAssetDeletionQueue` | deletions queued by the API are never executed |

### 2.3 Notifications

| task | what breaks |
|---|---|
| `handleMentions`, `handleMentionChunk` | no "you were mentioned" state, no `recent_mentions` rows, no presence push |
| `sendSystemDm` | system DMs (moderation notices, registration) are never delivered |
| `notifyRegistrationApprovers` | registration mode here is **`approval`** (`registration_config.mode`), so this one matters: nobody is told that a person is waiting |

### 2.4 Threads

`seedThreadPermissions`, `archiveInactiveThreads`, `deleteChannelThreads`, `repairThreadIndexes`,
`removeThreadMembershipsForGuildMember`, `liftExpiredGuildInvitePauses` — thread permissions are not seeded,
inactive threads are never archived, deleted channels keep their thread rows.

### 2.5 Crosspost — unusable in a one-guild instance, not disabled

`crosspostMessage`, `crosspostMessageChunk`, `syncCrosspostedMessage`, `syncCrosspostCopies`,
`removeChannelFollowers`.

Crosspost is **not** switched off by single-community mode; the bundle has no such gate, and this instance has
`single_community_enabled: true` (`instance_policy_config`, guild `1563469298515574784`). What crosspost needs is an
announcement channel (type `5`) plus channels **in other guilds** that follow it — the API describes follower stats
as "channels and distinct guilds", and the publish rate limits are "per channel … and per community". In a
single-community instance there is exactly one guild, so nothing can follow anything: an announcement channel there
would show zero followers forever. The instance also has no type-`5` channel today (only `0`, `2`, `4`, `999`), so no
crosspost job is ever produced.

### 2.6 Bulk admin operations (rare, but user-visible when used)

`bulkUpdateUserFlags`, `bulkUpdateGuildFeatures`, `bulkAddGuildMembers`, `bulkBanFileShas`,
`bulkScheduleUserDeletion`, `bulkDeleteSelfMessagesImmediate`, `bulkDeleteUserMessages`,
`bulkDeleteMessagesForUsers`, `deleteUserMessagesInGuildByTime`, `processPendingBulkMessageDeletions`,
`batchGuildAuditLogMessageDeletes` — the admin panel reports success, the queue holds the job, nothing happens.

### 2.7 Activity, caches, reports, misc

| task | what breaks |
|---|---|
| `flushUserActivityBuffer` | the activity buffer is never flushed into the spool (the spool drain itself is implemented, but the `EVENTS` stream is missing) |
| `processCachePurgeQueue` | cached entries the API asked to purge stay in valkey |
| `expireReportSnapshots`, `clearAuthenticatedReporterEmails` | report snapshots are never pruned; reporter emails stay stored |
| `processInactivityDeletions` | the 730-day inactivity deletion never runs (config is `off` anyway) |
| `backfillPasskeyTrait` | one-time passkey backfill for pre-passkey accounts |
| `joinSingleCommunityMembers` | single-community mode is ON, so new accounts can be left waiting for the automatic guild join; it is parked (not ignored) since 2026-10-25 and its cron is now scheduled (17/28) |

## 3. Gaps inside `extractEmbeds`

| omitted | reason | effect |
|---|---|---|
| content moderation (`scanEmbedsForBannedContent`) | upstream *deletes the message* when a banned URL is unfurled; reimplementing the match rules and the delete is a data-loss risk | a link that was fine at send time and is banned later can still get a preview |
| crosspost sync (`jobs.syncCrosspostedMessage`) | only fires for crossposted messages, which do not exist here | none, until §2.5 is enabled |
| legacy `bucket = 0` read repair | no pre-bucket rows on this instance | none |
| exact WHATWG URL normalisation | `net/url` + IDNA is close but not identical (`stripOwnAttachmentSignature` is not ported) | a self-CDN link may occasionally be unfurled instead of skipped |
| `areEmbedsEquivalent` normalisation | compared by JSON instead of field-by-field normalisation | an extra write in rare cases, never a wrong one |

## 4. Order I would port them in

1. `userProcessPendingDeletion(s)` + `messageShred` — data rights, and the only gap a user can notice and complain about.
2. `expireAttachments` + `reconcileAttachmentDecay` — the bucket only grows.
3. `handleMentions` + `handleMentionChunk` — the last everyday UX feature.
4. `sendSystemDm`, `notifyRegistrationApprovers` — small, and they unblock registration flows.
5. Content moderation as **skip, not delete** (§3), if the blocklists are ever used.
