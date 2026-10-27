#!/bin/sh
# bench.sh — what does a worker cost?
#
# Measures the upstream node worker (the api image running dist/WorkerEntrypoint.js)
# against worker-lite, on the same host, against an isolated NATS and valkey, with the
# same burst of jobs. The database is the instance's own: a copy is impossible while
# the api holds connections to it (template copy needs an idle source), and the tasks
# this burst uses are read-only no-ops.
#
# Why isolated NATS/valkey: the real queue must not be touched. Each run gets a fresh
# broker, because durable consumers survive a container restart and a second worker
# would otherwise hit "filtered consumer not unique".
#
# Usage (on the docker host, e.g. CT 150):
#   sh bench/bench.sh [--seconds 45] [--jobs 40] [--task NAME] [--out FILE]
#
# Prints a markdown table. Exit code is non-zero when a worker fails to start.

set -eu

SECONDS_PER_RUN=45
JOBS=40
TASK=clearAuthenticatedReporterEmails
OUT=""
NET=${NET:-fluxer_fluxer}
API_CONTAINER=${API_CONTAINER:-fluxer-api-1}
GO_IMAGE=${GO_IMAGE:-fluxer-worker-lite:dev}
NATS_IMAGE=${NATS_IMAGE:-nats:2.14-alpine}
VALKEY_IMAGE=${VALKEY_IMAGE:-valkey/valkey:9.1-alpine}
NATS_BOX=${NATS_BOX:-natsio/nats-box:latest}
SNOWFLAKES_CONTAINER=${SNOWFLAKES_CONTAINER:-fluxer-snowflakes-1}

while [ $# -gt 0 ]; do
	case "$1" in
		--seconds) SECONDS_PER_RUN=$2; shift 2 ;;
		--jobs) JOBS=$2; shift 2 ;;
		--task) TASK=$2; shift 2 ;;
		--out) OUT=$2; shift 2 ;;
		-h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
		*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done

say() { printf '%s\n' "$*" >&2; }

cleanup() {
	docker rm -f bench-worker bench-nats bench-valkey bench-snowflakes >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

infra_up() {
	cleanup
	docker run -d --name bench-nats --network "$NET" --network-alias bench-nats "$NATS_IMAGE" -js >/dev/null
	docker run -d --name bench-valkey --network "$NET" --network-alias bench-valkey \
		"$VALKEY_IMAGE" valkey-server --save '' --appendonly no >/dev/null
	# The upstream worker resolves snowflakes over NATS at startup, so the bench broker
	# needs that service too. It is stateless and tiny.
	start_snowflakes
	# Wait for both to answer before starting a worker against them.
	i=0
	while [ $i -lt 30 ]; do
		if docker run --rm --network "$NET" "$NATS_BOX" nats --server nats://bench-nats:4222 stream ls >/dev/null 2>&1; then
			break
		fi
		i=$((i + 1)); sleep 1
	done
	if [ $i -ge 30 ]; then say "bench NATS did not come up"; exit 1; fi
}

# node_env builds an env file from the api container and overrides the three endpoints
# that must point at the bench broker and cache.
node_env() {
	out=$1
	docker inspect "$API_CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' \
		| grep -vE '^(FLUXER_NATS_URL|FLUXER_NATS_JETSTREAM_URL|FLUXER_KV_URL)=' > "$out"
	{
		echo "FLUXER_NATS_URL=nats://bench-nats:4222"
		echo "FLUXER_NATS_JETSTREAM_URL=nats://bench-nats:4222"
		echo "FLUXER_KV_URL=redis://bench-valkey:6379/0"
	} >> "$out"
	chmod 600 "$out"
}

# go_args echoes the env flags worker-lite needs, reusing the api's database settings.
go_args() {
	docker inspect "$API_CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' \
		| grep -E '^(FLUXER_POSTGRES_|FLUXER_S3_|PG)' > "$1"
	{
		echo "NATS_URL=nats://bench-nats:4222"
		echo "VALKEY_ADDR=bench-valkey:6379"
		echo "STREAM=JOBS"
		echo "JOB_PREFIX=jobs."
		echo "DLQ_STREAM=JOBS_DLQ"
		echo "DLQ_PREFIX=dlq."
		echo "PARK_STREAM=JOBS_PARKED"
		echo "PARK_PREFIX=parked."
		echo "STATUS_ADDR=:9098"
		echo "MAX_RSS_MB=96"
	} >> "$1"
	# worker-lite reads PG*, so translate the api's FLUXER_POSTGRES_* names.
	{
		echo "PGHOST=$(env_value "$1" FLUXER_POSTGRES_HOST)"
		echo "PGPORT=$(env_value "$1" FLUXER_POSTGRES_PORT)"
		echo "PGDATABASE=$(env_value "$1" FLUXER_POSTGRES_DATABASE)"
		echo "PGUSER=$(env_value "$1" FLUXER_POSTGRES_USERNAME)"
		echo "PGPASSWORD=$(env_value "$1" FLUXER_POSTGRES_PASSWORD)"
	} >> "$1"
	chmod 600 "$1"
}

env_value() { sed -n "s/^$2=//p" "$1" | head -1; }

start_snowflakes() {
	docker inspect "$SNOWFLAKES_CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' \
		| grep -vE '^FLUXER_(NATS_URL|NATS_JETSTREAM_URL|SVC_NATS_URL)=' > /tmp/bench-snowflakes.env
	{
		echo "FLUXER_NATS_URL=nats://bench-nats:4222"
		echo "FLUXER_NATS_JETSTREAM_URL=nats://bench-nats:4222"
		echo "FLUXER_SVC_NATS_URL=nats://bench-nats:4222"
	} >> /tmp/bench-snowflakes.env
	docker run -d --name bench-snowflakes --network "$NET" --env-file /tmp/bench-snowflakes.env \
		"$(docker inspect "$SNOWFLAKES_CONTAINER" --format '{{.Config.Image}}')" >/dev/null
}

# mem_mib prints one memory sample of a container, in MiB with a decimal point.
mem_mib() {
	raw=$(docker stats --no-stream --format '{{.MemUsage}}' "$1" 2>/dev/null | awk '{print $1}')
	case "$raw" in
		*GiB) awk -v v="$(echo "$raw" | tr -d 'GiB')" 'BEGIN {printf "%.1f", v*1024}' ;;
		*MiB) awk -v v="$(echo "$raw" | tr -d 'MiB')" 'BEGIN {printf "%.1f", v}' ;;
		*KiB) awk -v v="$(echo "$raw" | tr -d 'KiB')" 'BEGIN {printf "%.1f", v/1024}' ;;
		*) echo 0 ;;
	esac
}

is_up() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]; }

wait_ready() {
	# Either worker logs when its lanes are consuming; wait for that line.
	i=0
	while [ $i -lt 90 ]; do
		if docker logs "$1" 2>&1 | grep -qE 'worker-lite start|Worker runtime configuration resolved'; then
			return 0
		fi
		if ! is_up "$1"; then return 1; fi
		i=$((i + 1)); sleep 1
	done
	return 1
}

# sampling_start begins a background sampler writing "epoch mib" lines.
sampling_start() {
	SAMPLES=$(mktemp)
	(
		while :; do
			echo "$(date +%s) $(mem_mib bench-worker)" >> "$SAMPLES"
			sleep 1
		done
	) &
	SAMPLER=$!
}

sampling_stop() {
	kill "$SAMPLER" >/dev/null 2>&1 || true
	wait "$SAMPLER" 2>/dev/null || true
}

# stats_before prints "min median" of the samples taken up to epoch $1.
stats_before() {
	awk -v cut="$1" '$1 < cut {print $2}' "$SAMPLES" | sort -n \
		| awk '{a[NR]=$1} END {if (NR == 0) {print "0.0 0.0"; exit} printf "%.1f %.1f", a[1], a[int((NR+1)/2)]}'
}

peak_all() {
	sort -n -k2 "$SAMPLES" | awk 'END {printf "%.1f", $2}'
}

# publish_jobs enqueues the burst from a single nats-box container: one container per
# message took longer than the burst window and the samples missed the work.
publish_jobs() {
	docker run --rm --network "$NET" -e JOBS="$JOBS" -e TASK="$TASK" "$NATS_BOX" \
		sh -c 'i=0; while [ "$i" -lt "$JOBS" ]; do nats --server nats://bench-nats:4222 pub "jobs.$TASK" "{}" >/dev/null 2>&1; i=$((i+1)); done' \
		>/dev/null 2>&1 || true
}

run_node() {
	say "== node worker: starting"
	infra_up
	node_env /tmp/bench-node.env
	docker run -d --name bench-worker --network "$NET" --env-file /tmp/bench-node.env \
		"$NODE_IMAGE" node dist/WorkerEntrypoint.js >/dev/null
	if ! wait_ready bench-worker; then
		say "node worker did not start"; docker logs --tail 20 bench-worker >&2; return 1
	fi
	sampling_start
	sleep 10
	mark=$(date +%s)
	idle=$(stats_before "$mark")
	publish_jobs
	sleep "$SECONDS_PER_RUN"
	sampling_stop
	peak=$(peak_all)
	handled=$(docker logs bench-worker 2>&1 | grep -c "\"taskType\":\"$TASK\"" || true)
	size=$(docker image inspect "$NODE_IMAGE" --format '{{.Size}}')
	docker rm -f bench-worker >/dev/null 2>&1 || true
	printf 'node:%s:%s:%s:%s:%s\n' "$idle" "$peak" "$handled" "$size"
}

run_go() {
	say "== worker-lite: starting"
	infra_up
	go_args /tmp/bench-go.env
	docker run -d --name bench-worker --network "$NET" --env-file /tmp/bench-go.env \
		-p 127.0.0.1:9098:9098 "$GO_IMAGE" >/dev/null
	if ! wait_ready bench-worker; then
		say "worker-lite did not start"; docker logs --tail 20 bench-worker >&2; return 1
	fi
	sampling_start
	sleep 10
	mark=$(date +%s)
	idle=$(stats_before "$mark")
	publish_jobs
	sleep "$SECONDS_PER_RUN"
	sampling_stop
	peak=$(peak_all)
	handled=0
	if command -v curl >/dev/null 2>&1; then
		counted=$(curl -s --max-time 5 http://127.0.0.1:9098/status | sed -n 's/.*"handled":[ ]*\([0-9]*\).*/\1/p' | head -1)
		[ -n "${counted:-}" ] && handled=$counted
	fi
	size=$(docker image inspect "$GO_IMAGE" --format '{{.Size}}')
	docker rm -f bench-worker >/dev/null 2>&1 || true
	printf 'go:%s:%s:%s:%s\n' "$idle" "$peak" "$handled" "$size"
}

NODE_IMAGE=$(docker inspect "$API_CONTAINER" --format '{{.Config.Image}}')
say "node image: $NODE_IMAGE"
say "go image:   $GO_IMAGE"
say "burst:      $JOBS x $TASK"

node_row=$(run_node || true)
go_row=$(run_go || true)

report() {
	printf '| implementation | image | idle MiB (min/med/max) | burst MiB (min/med/max) | jobs seen |\n'
	printf '|---|---|---|---|---|\n'
	printf '%s\n' "$go_row" | while IFS=: read -r _ idle peak handled size; do
		[ -n "${idle:-}" ] || continue
		printf '| worker-lite (Go) | %s MiB | %s / - | %s | %s |\n' "$(hsize "$size")" "$idle" "$peak" "$handled"
	done
	printf '%s\n' "$node_row" | while IFS=: read -r _ idle peak handled size; do
		[ -n "${idle:-}" ] || continue
		printf '| node worker (upstream) | %s MiB | %s / - | %s | %s |\n' "$(hsize "$size")" "$idle" "$peak" "$handled"
	done
}

hsize() { awk -v b="$1" 'BEGIN {printf "%.0f", b/1048576}'; }

if [ -n "$OUT" ]; then
	report | tee "$OUT"
else
	report
fi
