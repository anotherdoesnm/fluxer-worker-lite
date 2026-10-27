#!/bin/sh
# publish.sh — tag the built image with the spec release and either push it to a
# registry or write a loadable tarball.
#
#   sh publish.sh ghcr.io/you/fluxer-worker-lite        # push (needs registry login)
#   sh publish.sh --tar ./worker-lite.tar.gz            # docker load on the target
#
# The registry path must have package-write permission for the user you are logged in
# as. A token with only repository scope answers "unauthorized: reqPackageAccess" on
# push, which is a permissions problem, not a build problem.

set -eu

IMAGE=${IMAGE:-fluxer-worker-lite:dev}
RELEASE=$(sed -n 's/^[[:space:]]*specRelease = "\(.*\)"/\1/p' cmd/worker/version.go)
if [ -z "$RELEASE" ]; then
	echo "publish.sh: cannot read specRelease from cmd/worker/version.go" >&2
	exit 1
fi

if [ "${1:-}" = "--tar" ]; then
	out=${2:-worker-lite-$RELEASE.tar.gz}
	docker save "$IMAGE" | gzip -1 > "$out"
	echo "wrote $out"
	echo "load it with: gunzip -c $out | docker load"
	exit 0
fi

target=${1:?usage: publish.sh <registry/image> | --tar [file]}
docker tag "$IMAGE" "$target:$RELEASE"
docker tag "$IMAGE" "$target:latest"
docker push "$target:$RELEASE"
docker push "$target:latest"
echo "pushed $target:$RELEASE and :latest"
