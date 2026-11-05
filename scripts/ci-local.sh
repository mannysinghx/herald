#!/usr/bin/env bash
# Local CI, run before every push. It tests the committed code from a clean checkout, in Linux,
# because that is where the service runs. Usage:
#   scripts/ci-local.sh            # HEAD, in a golang:1.26 container
#   scripts/ci-local.sh --here     # the working tree, on this machine (for a quick check)
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "${1:-}" != "--here" ] && [ "${1:-}" != "--inside" ]; then
  commit=$(git rev-parse HEAD)
  echo "CI for $commit in a clean checkout, in Linux"
  git archive --format=tar "$commit" | docker run --rm -i golang:1.26 bash -c \
    'mkdir -p /src && cd /src && tar xf - && apt-get update -qq >/dev/null && apt-get install -y -qq lsof >/dev/null && scripts/ci-local.sh --inside'
  echo "CI passed for $commit"
  exit 0
fi

echo "== gofmt"; test -z "$(gofmt -l .)" || { gofmt -l .; exit 1; }
echo "== vet"; go vet ./...
echo "== tests, race detector"; go test -race -count=1 ./...
echo "== build, static"; CGO_ENABLED=0 go build -o /dev/null .
echo "== dependency audit"; go run golang.org/x/vuln/cmd/govulncheck@latest ./...
echo "== conformance against the recorded ntfy transcripts"; scripts/conformance.sh
echo "== all checks passed"
