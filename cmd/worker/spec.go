package main

// Lane definitions copied from fluxer_api/src/api/worker/WorkerLaneConfig.ts.
// Task lists matter: the real queue filters each durable consumer by
// `jobs.<task>` for the tasks of its lane, so a Go worker must subscribe to
// exactly the same set or it will silently miss work.
type laneSpec struct {
	Name          string
	Consumer      string
	Tasks         []string
	Concurrency   int
	MaxAckPending int
	AckWaitSec    int
	MaxDeliver    int
}

var lanes = []laneSpec{
	{
		Name: "realtime", Consumer: "workers_realtime_go",
		Tasks:       []string{"handleMentions", "handleMentionChunk"},
		Concurrency: 10, MaxAckPending: 50, AckWaitSec: 15, MaxDeliver: 3,
	},
	{
		Name: "unfurl", Consumer: "workers_unfurl_go",
		Tasks:       []string{"extractEmbeds"},
		Concurrency: 20, MaxAckPending: 200, AckWaitSec: 30, MaxDeliver: 3,
	},
	{
		Name: "lifecycle", Consumer: "workers_lifecycle_go",
		Tasks: []string{
			"processStripeWebhook", "sendSystemDm", "userProcessPendingDeletion",
			"userProcessPendingDeletions", "applicationProcessDeletion", "guildProcessDeletion",
			"bulkDeleteSelfMessagesImmediate", "bulkDeleteUserMessages", "deleteUserMessagesInGuildByTime",
			"messageShred", "harvestGuildData", "harvestUserData", "batchGuildAuditLogMessageDeletes",
			"reconcileUserPayments", "syncStripeCustomerEmail", "processAppStoreNotification",
			"processGooglePlayNotification", "refreshStorePurchase", "bulkUpdateUserFlags",
			"bulkScheduleUserDeletion", "bulkUpdateGuildFeatures", "bulkAddGuildMembers",
			"bulkBanFileShas", "bulkDeleteMessagesForUsers", "drainActivitySpool",
			"liftExpiredGuildInvitePauses",
		},
		Concurrency: 8, MaxAckPending: 50, AckWaitSec: 60, MaxDeliver: 25,
	},
	{
		Name: "batch", Consumer: "workers_batch_go",
		Tasks: []string{
			"expireAttachments", "reconcileAttachmentDecay", "expireReportSnapshots",
			"clearAuthenticatedReporterEmails", "expireStaleJobs", "indexChannelMessages",
			"indexGuildMembers", "processAssetDeletionQueue", "processCachePurgeQueue",
			"processExpiredPremiumSweep", "processInactivityDeletions",
			"processPendingBulkMessageDeletions", "processPremiumStateReconciliationQueue",
			"processStorePurchaseRefreshQueue", "pollGooglePlayVoidedPurchases",
			"pollAppStoreNotificationHistory", "prunePostgresKvTtl", "refreshSearchIndex",
			"seedThreadPermissions", "archiveInactiveThreads", "repairThreadIndexes",
			"deleteChannelThreads", "removeThreadMembershipsForGuildMember",
			"syncThreadSearchDocument", "backfillThreadSearch", "syncDiscoveryIndex",
			"syncUrlBlocklists", "syncFileShaBlocklists", "flushUserActivityBuffer",
		},
		Concurrency: 12, MaxAckPending: 100, AckWaitSec: 120, MaxDeliver: 25,
	},
	{
		Name: "crosspost", Consumer: "workers_crosspost_go",
		Tasks: []string{
			"crosspostMessage", "crosspostMessageChunk", "syncCrosspostedMessage",
			"syncCrosspostCopies", "removeChannelFollowers",
		},
		Concurrency: 8, MaxAckPending: 64, AckWaitSec: 60, MaxDeliver: 25,
	},
}

// cronSpec mirrors the 21 cron.upsert() calls in WorkerMain.ts. Six fields,
// seconds first — robfig/cron with cron.WithSeconds().
type cronSpec struct {
	Task string
	Expr string
}

var crons = []cronSpec{
	{"processCachePurgeQueue", "*/10 * * * * *"},
	{"flushUserActivityBuffer", "*/10 * * * * *"},
	{"drainActivitySpool", "*/5 * * * * *"},
	{"processAssetDeletionQueue", "0 */5 * * * *"},
	{"processStorePurchaseRefreshQueue", "0 */5 * * * *"},
	{"prunePostgresKvTtl", "0 */5 * * * *"},
	{"processPendingBulkMessageDeletions", "0 */10 * * * *"},
	{"syncDiscoveryIndex", "0 */15 * * * *"},
	{"userProcessPendingDeletions", "0 * * * * *"},
	{"processPremiumStateReconciliationQueue", "0 * * * * *"},
	{"liftExpiredGuildInvitePauses", "0 * * * * *"},
	{"archiveInactiveThreads", "0 * * * * *"},
	{"processExpiredPremiumSweep", "0 0 * * * *"},
	{"processInactivityDeletions", "0 0 */6 * * *"},
	{"syncUrlBlocklists", "0 0 */6 * * *"},
	{"expireAttachments", "0 0 */12 * * *"},
	{"syncFileShaBlocklists", "0 0 */12 * * *"},
	{"expireStaleJobs", "0 45 3 * * *"},
	{"pollGooglePlayVoidedPurchases", "0 30 4 * * *"},
	{"pollAppStoreNotificationHistory", "0 45 4 * * *"},
	{"expireReportSnapshots", "0 15 5 * * *"},
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
