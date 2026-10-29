package main

// What this worker does with a task it receives.
//
// The first version parked everything it could not run: a delayed nack kept the
// message in the workqueue stream, so no work was lost while handlers were
// missing. As a steady state that is wrong, and it showed up in production: the
// parked redeliveries fill MaxAckPending, so the tasks this worker *can* run
// (prunePostgresKvTtl) wait behind them and the queue stops moving.
//
// So a task now has one of three fates, and each one is a decision someone made
// on purpose:
//
//	handle  there is an entry in handlers[]; run it
//	ignore  the feature behind the task is off on this instance; ack and drop,
//	        with a reason recorded in ignoredTasks so the next admin can tell
//	        whether the decision still holds
//	park    the work is real and must not be dropped, but the handler is not
//	        written yet: copy the job into the parked stream, then ack
//
// Park is the default. A task nobody classified is parked, never dropped.
type policy int

const (
	policyPark policy = iota
	policyHandle
	policyIgnore
)

// ignoredTasks maps a task this instance never needs to run to the reason it
// can be dropped. The reason is load-bearing: "we do not sell anything" is true
// until the instance starts selling something, and then this line is wrong.
var ignoredTasks = map[string]string{
	// Billing, premium and app-store purchases: self-hosted has no payment
	// provider, so upstream only registers most of these crons when the
	// instance is not self-hosted (WorkerMain.ts, `if (!Config.instance.selfHosted)`).
	"processStripeWebhook":                   "no payment provider configured",
	"sendBillingEmail":                       "no payment provider configured",
	"scanBillingNotices":                     "no payment provider configured",
	"processPremiumStateReconciliationQueue": "no payment provider configured",
	"processExpiredPremiumSweep":             "no premium SKUs on this instance",
	"closePaymentDisputeAccounts":            "no payment provider configured",
	"reconcileUserPayments":                  "no payment provider configured",
	"syncStripeCustomerEmail":                "no payment provider configured",
	"processAppStoreNotification":            "app-store purchases are cloud-only",
	"processGooglePlayNotification":          "app-store purchases are cloud-only",
	"refreshStorePurchase":                   "app-store purchases are cloud-only",
	"processStorePurchaseRefreshQueue":       "app-store purchases are cloud-only",
	"pollGooglePlayVoidedPurchases":          "app-store purchases are cloud-only",
	"pollAppStoreNotificationHistory":        "app-store purchases are cloud-only",
	"operatorPassSaleBroadcast":              "operator pass is a cloud feature",
	"operatorPassWaitlistCleanup":            "operator pass is a cloud feature",
	"operatorPassRemovalExpiry":              "operator pass is a cloud feature",
	"operatorPassWallRetention":              "operator pass is a cloud feature",
	"operatorPassOfferCompletion":            "operator pass is a cloud feature",

	// Optional feeds and features that are switched off in .env.
	"syncUrlBlocklists":        "blocklist feeds are disabled",
	"syncFileShaBlocklists":    "blocklist feeds are disabled",
	"endGuildVisitsForCode":    "guild visit analytics are not collected",
	"pruneGuildVisits":         "guild visit analytics are not collected",
	"endUnassignedGuildVisits": "guild visit analytics are not collected",
	"processCachePurgeQueue":   "cache purge adapter is none (FLUXER_CACHE_PURGE_ADAPTER unset; the default is none, so the API queues nothing and upstream skips the run too)",
}

// taskPolicy resolves a task name to its fate. handlers[] wins over
// ignoredTasks: a task that is both implemented and listed as ignorable is a
// mistake in the tables, and running the code is the safer half of it.
func taskPolicy(task string) policy {
	if _, ok := handlers[task]; ok {
		return policyHandle
	}
	if _, ok := ignoredTasks[task]; ok {
		return policyIgnore
	}
	return policyPark
}
