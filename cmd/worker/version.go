package main

// Which API release this worker was built against, and whether the API it is
// talking to is still that release.
//
// worker-lite does not import the API: it re-implements the contract (job
// subjects, the KV table, the ledger). That contract moved in both releases we
// have seen — 2026.1025.35621 added 13 tasks and a retiredTasks list — so a
// worker running against a newer API is not "slightly behind", it is running
// against a table it never read.
//
// Two checks, both cheap:
//
//  1. At startup: compare the digest and build number recorded below with the
//     ones the deploy script passes in (API_DIGEST, API_BUILD_VERSION), which it
//     reads off the running api container. Same digest means the task tables in
//     spec.go were generated from exactly this image.
//
//  2. Periodically, if BUNDLE_CHECK_SEC is set: fetch the published compose
//     bundle and compare its fluxer-api digest with the running one. A
//     difference is "a new release exists", not "we are broken" — the upgrade
//     stays a decision, but it stops being a thing someone has to remember.

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// specRelease is the self-hosting release whose dist was read to generate
	// spec.go. Update it together with the task tables, never on its own. The
	// published bundle carries no separate release number, so this is the api
	// BUILD_VERSION of the image the tables were read from.
	specRelease = "2026.1025.144746"
	// specAPIDigest is the fluxer-api image that release ships.
	specAPIDigest = "sha256:408429273dc8912d752d3e4582bc859a4e2dfe6c79f6ccfc3eea7e9cbcd7e538"
	// specAPIBuild is the BUILD_VERSION baked into that image.
	specAPIBuild = "2026.1025.144746"

	defaultBundleURL = "https://fluxer.dev/compose/latest"
)

var apiMismatchLogged atomic.Bool

type apiIdentity struct {
	Build  string
	Digest string
}

func runningAPI() apiIdentity {
	return apiIdentity{
		Build:  env("API_BUILD_VERSION", ""),
		Digest: env("API_DIGEST", ""),
	}
}

func shortDigest(d string) string {
	d = strings.TrimPrefix(d, "sha256:")
	if len(d) > 12 {
		return d[:12]
	}
	return d
}

// reportAPIMatch logs once whether the running API is the one spec.go was
// generated from. A missing identity is not an error (the worker must run
// standalone in tests), but an unequal one is loud, because from then on the
// lane tables are an assumption rather than a fact.
func reportAPIMatch() {
	api := runningAPI()
	switch {
	case api.Digest == "" && api.Build == "":
		log.Printf("api identity not provided (API_DIGEST/API_BUILD_VERSION unset); spec is for release %s build %s, cannot verify", specRelease, specAPIBuild)
	case api.Digest != "" && api.Digest != specAPIDigest:
		apiMismatchLogged.Store(true)
		log.Printf("WARN running api is %s (%s) but spec.go was generated from release %s (%s) — regenerate the task tables",
			shortDigest(api.Digest), api.Build, specRelease, shortDigest(specAPIDigest))
	case api.Build != "" && api.Build != specAPIBuild:
		log.Printf("note running api build %s differs from the spec build %s, same image digest — task tables are current", api.Build, specAPIBuild)
	default:
		log.Printf("api matches the spec: release %s, image %s, build %s", specRelease, shortDigest(specAPIDigest), specAPIBuild)
	}
}

// bundleDigests downloads the published compose bundle and returns the image
// digest of the given service. The bundle is a tar.gz holding docker-compose.yml
// with every image pinned by digest.
func bundleDigests(ctx context.Context, url, service string) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bundle: HTTP %d", resp.StatusCode)
	}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return "", err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return "", fmt.Errorf("bundle: %s not found", "docker-compose.yml")
		}
		if err != nil {
			return "", err
		}
		if !strings.HasSuffix(hdr.Name, "docker-compose.yml") {
			continue
		}
		body, err := io.ReadAll(io.LimitReader(tr, 4<<20))
		if err != nil {
			return "", err
		}
		return apiDigestFromCompose(string(body), service), nil
	}
}

// apiDigestFromCompose finds `ghcr.io/fluxerapp/fluxer-<service>@sha256:…` in the
// compose file. The worker service runs the api image, so both matches are equal.
func apiDigestFromCompose(compose, service string) string {
	// The image reference is ghcr.io/fluxerapp/fluxer-<service>@sha256:…
	// and the worker service reuses the api image.
	marker := "fluxer-" + service + "@sha256:"
	found := ""
	for idx := 0; ; {
		i := strings.Index(compose[idx:], marker)
		if i < 0 {
			break
		}
		start := idx + i + len("fluxer-"+service+"@")
		rest := compose[start:]
		end := strings.IndexAny(rest, " \n\t\"'")
		if end < 0 {
			end = len(rest)
		}
		found = rest[:end]
		idx = start
	}
	return found
}

// watchBundle reports a published release newer than the running one, once.
func watchBundle(ctx context.Context, every time.Duration) {
	url := env("BUNDLE_URL", defaultBundleURL)
	var reported string
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		api := runningAPI()
		published, err := bundleDigests(ctx, url, "api")
		switch {
		case err != nil:
			log.Printf("bundle check: %v (will retry)", err)
		case published == "":
			log.Printf("bundle check: no fluxer-api digest found at %s", url)
		case api.Digest != "" && published != api.Digest && reported != published:
			reported = published
			log.Printf("ALERT new release: published fluxer-api is %s, running %s — upgrade when convenient",
				shortDigest(published), shortDigest(api.Digest))
		case published == api.Digest && reported != published:
			reported = published
			log.Printf("bundle check: published release matches the running api (%s)", shortDigest(published))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// apiLabel is the short form used in the stats line, so a log alone says which
// API this worker was serving.
func apiLabel() string {
	api := runningAPI()
	switch {
	case api.Build != "":
		return api.Build
	case api.Digest != "":
		return shortDigest(api.Digest)
	}
	return "unknown"
}
