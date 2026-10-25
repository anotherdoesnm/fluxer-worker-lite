#!/bin/sh
# Deploy worker-lite against the api container that is running right now.
#
# Why a script and not a compose service: the worker has to know which api image
# it is serving, so the deploy reads the digest and BUILD_VERSION off the live
# api container and hands them to the worker (API_DIGEST, API_BUILD_VERSION).
# worker-lite compares them with the release its task tables were generated from
# (specRelease in cmd/worker/version.go) and says so in the log.
#
# Run inside CT 150, from a checkout that has the sources at /root/wl-src and the
# image built as fluxer-worker-lite:dev:
#
#   docker build -t fluxer-worker-lite:dev /root/wl-src
#   sh deploy.sh
#
# Restart policy stays "no" on purpose: a worker that died must look dead in
# `docker ps`, not quietly be restarted by the daemon.
set -e

API_CONTAINER=${API_CONTAINER:-fluxer-api-1}
NETWORK=${NETWORK:-fluxer_fluxer}
IMAGE=${IMAGE:-fluxer-worker-lite:dev}
ENV_FILE=${ENV_FILE:-/srv/fluxer/.env}
STATUS_PORT=${STATUS_PORT:-9099}

api_image=$(docker inspect "$API_CONTAINER" --format '{{.Config.Image}}')
api_digest=${api_image#*@}
api_build=$(docker exec "$API_CONTAINER" printenv BUILD_VERSION 2>/dev/null || true)
# extractEmbeds asks svc.messages to build the MESSAGE_UPDATE payload, and that
# call carries the instance's media settings. Read them from the api container
# instead of duplicating secrets into yet another file.
apienv() { docker exec "$API_CONTAINER" printenv "$1" 2>/dev/null || true; }
media_endpoint=$(apienv FLUXER_MEDIA_PROXY_PUBLIC_ENDPOINT)
media_proxy_secret=$(apienv FLUXER_MEDIA_PROXY_SECRET_KEY)
attachment_secret=$(apienv FLUXER_MEDIA_PROXY_ATTACHMENT_URL_SECRETS_BASE64 | cut -d, -f1)
instance_name=$(apienv FLUXER_APP_PRODUCT_NAME)
# The cache purge cron is gated on the API's own adapter setting, so read it from
# the API rather than guessing: unset means the default adapter, which is none.
cache_purge_adapter=$(apienv FLUXER_CACHE_PURGE_ADAPTER)
base_domain=$(apienv FLUXER_BASE_DOMAIN)
pg_password=$(grep -m1 '^POSTGRES_PASSWORD=' "$ENV_FILE" | cut -d= -f2-)

echo "api image:   $api_image"
echo "api build:   ${api_build:-unknown}"

docker rm -f fluxer-worker-lite >/dev/null 2>&1 || true
docker run -d --name fluxer-worker-lite --network "$NETWORK" --restart no \
	-p "127.0.0.1:${STATUS_PORT}:${STATUS_PORT}" \
	-e NATS_URL=nats://nats:4222 \
	-e STREAM=JOBS -e JOB_PREFIX=jobs. \
	-e DLQ_STREAM=JOBS_DLQ -e DLQ_PREFIX=dlq. \
	-e PARK_STREAM=JOBS_PARKED -e PARK_PREFIX=parked. \
	-e API_DIGEST="$api_digest" \
	-e API_BUILD_VERSION="$api_build" \
	-e FLUXER_BASE_DOMAIN="$base_domain" \
	-e FLUXER_MEDIA_ENDPOINT="$media_endpoint" \
	-e FLUXER_MEDIA_PROXY_SECRET_KEY="$media_proxy_secret" \
	-e FLUXER_ATTACHMENT_URL_SECRET="$attachment_secret" \
	-e FLUXER_INSTANCE_NAME="$instance_name" \
	-e CACHE_PURGE_ADAPTER="$cache_purge_adapter" \
	-e BUNDLE_CHECK_SEC="${BUNDLE_CHECK_SEC:-21600}" \
	-e STATUS_ADDR=":${STATUS_PORT}" \
	-e STATS_SEC="${STATS_SEC:-30}" \
	-e MAX_RSS_MB="${MAX_RSS_MB:-96}" \
	-e LOG_TASKS="${LOG_TASKS:-0}" \
	-e PGHOST="${PGHOST:-192.168.1.10}" -e PGPORT="${PGPORT:-5432}" \
	-e PGDATABASE="${PGDATABASE:-fluxer_infra}" -e PGUSER="${PGUSER:-fluxer_infra}" \
	-e PGPASSWORD="$pg_password" \
	"$IMAGE" >/dev/null

sleep 4
docker logs --tail 12 fluxer-worker-lite
