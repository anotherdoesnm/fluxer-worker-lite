# What worker-lite does not implement

State of 2026-10-27, against Fluxer API release `2026.1026.151750`.

Counts from `cmd/worker/spec.go`: **78 tasks**, of which 22 have handlers, 30 are ignored on purpose
(a map value with a reason in `cmd/worker/policy.go`), and **26 are parked**.

Two tests keep this table honest: `registry_test.go` reads this section and fails when a task listed
here has no entry in `handlers[]`, and fails again when a handler names a task no lane publishes.

Two different things are listed here: whole tasks we do not run, and gaps *inside* the handlers we do run.
A parked job is not lost — it is copied to `JOBS_PARKED` before the ack — except cron ticks, which are dropped
deliberately because the scheduler regenerates them. What "dropped" means for a missing handler is therefore:
the work simply never happens until the handler exists.

## 1. Handled today

| task | what it does | notes |
|---|---|---|
| `prunePostgresKvTtl` | deletes expired `fluxer_kv` rows; runs the two one-time legacy passes (default-TTL backfill, job-ledger expiry) | full port, including the marker-hash contract |
| `expireStaleJobs` | deadletters jobs stuck in `jobs_active` past 8 days, drops finished ones | full port |
| `extractEmbeds` | link previews: URL extraction, `svc.unfurl`, KV write, `MESSAGE_UPDATE` | see §4 for the parts left out |
| `drainActivitySpool` | moves spooled activity events from the Redis list `activity:spool` into the `EVENTS` stream, under the `activity:spool:drain` lock | no-op on this instance: the `EVENTS` stream does not exist, so it reports that once and returns (see §2.7) |
| `handleMentions` | asks the gateway who a message mentioned (`rpc.gateway.guild.resolve_mention_sources_page`, paged) and publishes `handleMentionChunk` jobs of 250 users | DM mentions are taken straight from the payload; thread scope is not ported (see §3) |
| `handleMentionChunk` | per-user effect: `read_states.mention_count` +1 under the read-position guard, `recent_mentions` / `recent_mentions_by_guild` rows with a 7-day TTL, guild `suppress_everyone` / `suppress_roles` honoured | threads are written only if the payload says `thread: true`, which upstream sets through the thread scope |
| `clearAuthenticatedReporterEmails` | erases the stored reporter email from `iar_submissions` rows that belong to an authenticated account | full port, paged by `report_id` like upstream; one failing row is counted and skipped, not fatal |
| `bulkDeleteUserMessages` | deletes a person's own messages up to the moment they asked for, then clears `pending_bulk_message_deletion_{at,channel_count,message_count}` | full port; walks `messages_by_author_id_v2` and reuses the per-message deletion bundle |
| `liftExpiredGuildInvitePauses` | closes an expired invite pause: removes `INVITES_DISABLED`/`RAID_DETECTED` from the guild row, clears `invites_paused_until`, records the change in the guild audit log and dispatches `GUILD_UPDATE` | full port except the features-changed event (no `EVENTS` stream on this instance) |
| `bulkBanFileShas` | bans a pasted list of SHA-256 hashes: one `banned_file_shas` row and one admin audit entry per hash, then a single Redis publish so every process reloads the blocklist, then a summary audit row | full port except the job progress feed; malformed hashes are counted and skipped, as the api's bulk runner does |
| `bulkUpdateGuildFeatures` | admin sweep: adds/removes features on a list of guilds, `GUILD_UPDATE` per guild, guild audit entry, one admin audit summary row | full port except the job progress feed the admin panel uses (no job-progress writer yet); removal wins when a feature is on both sides, as upstream |
| `deleteUserMessagesInGuildByTime` | part of a guild ban: deletes what the person wrote inside the ban window (the last `seconds`) across the guild's channels | full port for channels; **threads are not walked**, so a thread message survives a ban until the thread repository is ported |
| `processPendingBulkMessageDeletions` | cron that turns due members of the `bulk_message_deletion_queue` sorted set into `bulkDeleteUserMessages` jobs | full port, including the state-version marker and rebuild lock: valkey is a cache, the user rows are the truth |
| `messageShred` | deletes a hand-picked list of messages by one author (`messageShred` is the admin's "shred these" job), and reports progress in `message_shred_status:<job_id>` | full port of the per-message deletion bundle: `messages`, `messages_by_author_id_v2`, `channel_pins`, `message_reactions`, `attachment_lookup`, bucket stamps, S3 objects via the asset-deletion queue. Search documents and crosspost copies are not ported (no search backend, single community) |

## 2. Not implemented — 33 tasks

### 2.1 Data rights and user lifecycle — the most consequential gap

| task | what breaks without it |
|---|---|
| `userProcessPendingDeletion`, `userProcessPendingDeletions` | a user asks to delete their account; the request sits in the queue forever |
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
| `sendSystemDm` | system DMs (moderation notices, registration) are never delivered |
| `notifyRegistrationApprovers` | registration mode here is **`approval`** (`registration_config.mode`), so this one matters: nobody is told that a person is waiting |

### 2.4 Threads

`seedThreadPermissions`, `archiveInactiveThreads`, `deleteChannelThreads`, `repairThreadIndexes`,
`removeThreadMembershipsForGuildMember` — thread permissions are not seeded,
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
`bulkScheduleUserDeletion`, `bulkDeleteSelfMessagesImmediate`,
`bulkDeleteMessagesForUsers`,
`batchGuildAuditLogMessageDeletes` — the admin panel reports success, the queue holds the job, nothing happens.

### 2.7 Activity, caches, reports, misc

| task | what breaks |
|---|---|
| `flushUserActivityBuffer` | the activity buffer is never flushed into the spool (the spool drain itself is implemented, but the `EVENTS` stream is missing) |
| `expireReportSnapshots` | report snapshots are never pruned |
| `processInactivityDeletions` | the 730-day inactivity deletion never runs (config is `off` anyway) |
| `backfillPasskeyTrait` | one-time passkey backfill for pre-passkey accounts |
| `joinSingleCommunityMembers` | single-community mode is ON, but the task is a one-shot backfill the API starts by writing `single_community:join_members_run`; the cron is now gated per tick on that key (absent right now, so it does not tick), and if a run does start the tick is parked until the handler exists |

`processCachePurgeQueue` is **not** in this section: `FLUXER_CACHE_PURGE_ADAPTER` is unset, the default adapter is `none`, the API
wires `NoopPurgeQueue` so nothing is ever queued, and upstream's own cron is not registered in that case either. It is
one of the 31 ignored tasks, and `deploy.sh` now passes the API's real adapter value to the worker so the gate closes
automatically if a purge adapter is ever configured.

## 3. Gaps inside the ported handlers

### 3.1 `handleMentions` / `handleMentionChunk`

| omitted | reason | effect |
|---|---|---|
| thread mention scope (`ThreadMentionScope`) | upstream loads the thread's participants and filters the resolved mentions to them, and only then sets the thread read-state marker; the thread tables are another subsystem we do not read | in a thread channel a mention can reach members who are not in that thread, and the read state keeps its channel marker instead of the thread marker |

## 4. Gaps inside `extractEmbeds`

| omitted | reason | effect |
|---|---|---|
| content moderation (`scanEmbedsForBannedContent`) | upstream *deletes the message* when a banned URL is unfurled; reimplementing the match rules and the delete is a data-loss risk | a link that was fine at send time and is banned later can still get a preview |
| crosspost sync (`jobs.syncCrosspostedMessage`) | only fires for crossposted messages, which do not exist here | none, until §2.5 is enabled |
| legacy `bucket = 0` read repair | no pre-bucket rows on this instance | none |
| exact WHATWG URL normalisation | `net/url` + IDNA is close but not identical (`stripOwnAttachmentSignature` is not ported) | a self-CDN link may occasionally be unfurled instead of skipped |
| `areEmbedsEquivalent` normalisation | compared by JSON instead of field-by-field normalisation | an extra write in rare cases, never a wrong one |

## 5. Order I would port them in

1. **Bulk message deletion** — `bulkDeleteSelfMessagesImmediate` (the person's own request with a
   channel/date filter) and `bulkDeleteMessagesForUsers` (an admin sweep over several users with an
   audit-log summary). Same deletion bundle, different filters.
2. **Lifecycle deletions** — `guildProcessDeletion`, `applicationProcessDeletion`,
   `processInactivityDeletions`. Deleting the wrong rows is unrecoverable, so they go after the
   deletion bundle has tests of its own.
3. **Data export** — `harvestUserData`, `harvestGuildData`. They are the only tasks whose output
   leaves the machine, so they need the storage and archive path to be right first.
4. **Admin bulk operations** — `bulkUpdateUserFlags`, `bulkAddGuildMembers`,
   `bulkScheduleUserDeletion`, `joinSingleCommunityMembers`, `bulkDeleteSelfMessagesImmediate`,
   `bulkDeleteMessagesForUsers`. The guild response mapper and the admin audit writer now exist, so
   what is missing per task is the user/member response mapper and, for the job-based ones, the
   progress feed.
5. **Attachment and report maintenance** — `reconcileAttachmentDecay`, `expireReportSnapshots`,
   `backfillPasskeyTrait`, `backfillDeletedUsers`, `backfillHiddenProfileBadges`.
6. **Threads** — `seedThreadPermissions`, `archiveInactiveThreads`, `repairThreadIndexes`,
   `deleteChannelThreads`, `removeThreadMembershipsForGuildMember`. Needs the thread repository
   ported as a whole, which is why it is not earlier.
7. **Crosspost** — five tasks that only matter once a second guild follows a channel; a
   single-community instance can never trigger them.
