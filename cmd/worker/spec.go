package main

// Generated from the fluxer-api image of build 2026.1025.144746 (digest
// 408429273dc8), read from src/api/worker/WorkerLaneConfig.ts and
// WorkerMain.ts in that image. The 2026.1025.35621 tables were re-checked
// against this release: 5 lanes, 76 tasks, 28 crons, unchanged. Do not hand-edit the task lists: regenerate them when the
// release moves, because a consumer filter that misses one task silently
// stops that work while everything still looks healthy.
//
// retiredTasks are tasks upstream no longer publishes. They stay in our
// filters on purpose: a leftover message from an older release must be
// acked, otherwise a workqueue stream keeps it forever and the consumer
// keeps redelivering it.
type laneSpec struct {
	Name          string
	Consumer      string
	Tasks         []string
	Retired       []string
	Concurrency   int
	MaxAckPending int
	AckWaitSec    int
	MaxDeliver    int
}

var lanes = []laneSpec{
	{
		Name: "realtime", Consumer: "workers_realtime_go",
		Concurrency: 10, MaxAckPending: 50, AckWaitSec: 15, MaxDeliver: 3,
		Tasks: []string{
			"handleMentions", "handleMentionChunk",
		},
	},
	{
		Name: "unfurl", Consumer: "workers_unfurl_go",
		Concurrency: 20, MaxAckPending: 200, AckWaitSec: 30, MaxDeliver: 3,
		Tasks: []string{
			"extractEmbeds",
		},
	},
	{
		Name: "lifecycle", Consumer: "workers_lifecycle_go",
		Concurrency: 8, MaxAckPending: 50, AckWaitSec: 60, MaxDeliver: 25,
		Tasks: []string{
			"processStripeWebhook", "closePaymentDisputeAccounts", "sendSystemDm", "notifyRegistrationApprovers",
			"userProcessPendingDeletion", "userProcessPendingDeletions", "applicationProcessDeletion", "guildProcessDeletion",
			"bulkDeleteSelfMessagesImmediate", "bulkDeleteUserMessages", "deleteUserMessagesInGuildByTime", "messageShred",
			"harvestGuildData", "harvestUserData", "batchGuildAuditLogMessageDeletes", "reconcileUserPayments",
			"syncStripeCustomerEmail", "processAppStoreNotification", "processGooglePlayNotification", "refreshStorePurchase",
			"bulkUpdateUserFlags", "bulkScheduleUserDeletion", "bulkUpdateGuildFeatures", "bulkAddGuildMembers",
			"joinSingleCommunityMembers", "bulkBanFileShas", "bulkDeleteMessagesForUsers", "drainActivitySpool",
			"liftExpiredGuildInvitePauses", "endGuildVisitsForCode", "pruneGuildVisits", "endUnassignedGuildVisits",
		},
		Retired: []string{"sendScheduledMessage", "finalizeNcmecAttachmentReport"},
	},
	{
		Name: "batch", Consumer: "workers_batch_go",
		Concurrency: 12, MaxAckPending: 100, AckWaitSec: 120, MaxDeliver: 25,
		Tasks: []string{
			"expireAttachments", "reconcileAttachmentDecay", "expireReportSnapshots", "clearAuthenticatedReporterEmails",
			"expireStaleJobs", "indexChannelMessages", "indexGuildMembers", "processAssetDeletionQueue",
			"processCachePurgeQueue", "processExpiredPremiumSweep", "processInactivityDeletions", "processPendingBulkMessageDeletions",
			"processPremiumStateReconciliationQueue", "processStorePurchaseRefreshQueue", "pollGooglePlayVoidedPurchases", "pollAppStoreNotificationHistory",
			"prunePostgresKvTtl", "refreshSearchIndex", "seedThreadPermissions", "archiveInactiveThreads",
			"repairThreadIndexes", "deleteChannelThreads", "removeThreadMembershipsForGuildMember", "syncThreadSearchDocument",
			"backfillThreadSearch", "backfillPasskeyTrait", "backfillUserAclIndex", "syncDiscoveryIndex",
			"syncUrlBlocklists", "syncFileShaBlocklists", "flushUserActivityBuffer", "operatorPassSaleBroadcast",
			"operatorPassWaitlistCleanup", "operatorPassRemovalExpiry", "operatorPassOfferCompletion", "operatorPassWallRetention",
		},
	},
	{
		Name: "crosspost", Consumer: "workers_crosspost_go",
		Concurrency: 8, MaxAckPending: 64, AckWaitSec: 60, MaxDeliver: 25,
		Tasks: []string{
			"crosspostMessage", "crosspostMessageChunk", "syncCrosspostedMessage", "syncCrosspostCopies",
			"removeChannelFollowers",
		},
	},
}

// cronSpec mirrors the cron.upsert() calls in WorkerMain.ts. Six fields,
// seconds first (robfig/cron with cron.WithSeconds()).
//
// Gate is the condition upstream wraps the registration in; an entry whose
// gate is closed on this instance must not be scheduled here either, or we
// would run cloud-only billing sweeps against a self-hosted database.
type cronSpec struct {
	Task string
	Expr string
	Gate string
}

var crons = []cronSpec{
	{"processAssetDeletionQueue", "0 */5 * * * *", "always"},
	{"processCachePurgeQueue", "*/10 * * * * *", "cachepurge"},
	{"processPendingBulkMessageDeletions", "0 */10 * * * *", "always"},
	{"userProcessPendingDeletions", "0 * * * * *", "always"},
	{"processPremiumStateReconciliationQueue", "0 * * * * *", "stripe"},
	{"processExpiredPremiumSweep", "0 0 * * * *", "always"},
	{"closePaymentDisputeAccounts", "0 40 * * * *", "always"},
	{"processStorePurchaseRefreshQueue", "0 */5 * * * *", "cloud"},
	{"pollGooglePlayVoidedPurchases", "0 30 4 * * *", "cloud"},
	{"pollAppStoreNotificationHistory", "0 45 4 * * *", "cloud"},
	{"operatorPassSaleBroadcast", "0 * * * * *", "cloud"},
	{"operatorPassWaitlistCleanup", "0 20 * * * *", "cloud"},
	{"operatorPassRemovalExpiry", "0 */5 * * * *", "cloud"},
	{"operatorPassWallRetention", "0 40 3 * * *", "cloud"},
	{"operatorPassOfferCompletion", "30 */5 * * * *", "cloud"},
	{"processInactivityDeletions", "0 0 */6 * * *", "always"},
	{"expireAttachments", "0 0 */12 * * *", "always"},
	{"expireReportSnapshots", "0 15 5 * * *", "always"},
	{"expireStaleJobs", "0 45 3 * * *", "always"},
	{"prunePostgresKvTtl", "0 */5 * * * *", "always"},
	{"syncDiscoveryIndex", "0 */15 * * * *", "always"},
	{"syncUrlBlocklists", "0 0 */6 * * *", "blocklists"},
	{"syncFileShaBlocklists", "0 0 */12 * * *", "blocklists"},
	{"flushUserActivityBuffer", "0 * * * * *", "always"},
	{"drainActivitySpool", "*/5 * * * * *", "activity"},
	{"joinSingleCommunityMembers", "0 * * * * *", "always"},
	{"liftExpiredGuildInvitePauses", "0 * * * * *", "always"},
	{"archiveInactiveThreads", "0 * * * * *", "always"},
}

func knownTask(name string) (laneSpec, bool) {
	for _, l := range lanes {
		for _, t := range l.Tasks {
			if t == name {
				return l, true
			}
		}
	}
	return laneSpec{}, false
}

// retiredTask reports a task name upstream has retired, so the message can be
// dropped instead of parked or sent to the dead-letter stream.
func retiredTask(l laneSpec, name string) bool {
	for _, t := range l.Retired {
		if t == name {
			return true
		}
	}
	return false
}

// cronEnabled answers whether the instance turns a cron registration on. The
// gates mirror WorkerMain.ts; the values come from this instance's .env, and an
// unset gate is closed, because scheduling a cloud-only sweep against a
// self-hosted database is worse than not scheduling it.
func cronEnabled(gate string) bool {
	switch gate {
	case "always":
		return true
	case "cloud":
		return env("FLUXER_CLOUD", "0") == "1"
	case "stripe":
		return env("STRIPE_ENABLED", "0") == "1"
	case "blocklists":
		return env("BLOCKLIST_FEEDS_ENABLED", "0") == "1"
	case "cachepurge":
		return env("CACHE_PURGE_ADAPTER", "valkey") != "none"
	case "activity":
		return env("ACTIVITY_EVENTS_PUBLISHING", "1") != "0"
	case "community":
		return env("SINGLE_COMMUNITY_JOIN", "0") == "1"
	}
	return false
}
