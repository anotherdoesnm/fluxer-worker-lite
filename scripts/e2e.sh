#!/bin/sh
# End-to-end run of the audit-log batch, from a machine that can actually reach the
# instance. It does not run in GitHub Actions: the runners are hosted by GitHub and
# have no route into the LAN, and the instance is a 1 GiB LXC that sleeps with
# onboot: 0. A check that can only ever skip is worse than no check.
#
#   FLUXER_BOT_TOKEN=<id.secret> scripts/e2e.sh
#
# Environment:
#   FLUXER_BASE_URL   default http://192.168.1.109:8080 (the instance on the LAN)
#   FLUXER_E2E_GUILD  default the "1GB RAM Fluxer Testing" guild
set -eu

BASE_URL=${FLUXER_BASE_URL:-http://192.168.1.109:8080}

if [ -z "${FLUXER_BOT_TOKEN:-}" ]; then
    echo "e2e: FLUXER_BOT_TOKEN is not set (the bot's id.secret)" >&2
    exit 1
fi

# A 2xx-4xx answer means the api is up: unauthenticated /users/@me answers 401.
code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' "$BASE_URL/api/v1/users/@me" || echo 000)
case "$code" in
    2??|3??|4??) echo "e2e: instance answers with $code at $BASE_URL" ;;
    *)
        echo "e2e: instance is not answering at $BASE_URL ($code)." >&2
        echo "     start it first: pct start 150   (it has onboot: 0 on purpose)" >&2
        exit 1
        ;;
esac

FLUXER_BASE_URL=$BASE_URL exec go run ./cmd/e2e
