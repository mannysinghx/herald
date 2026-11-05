# Progress

## Where it stopped

- Branch `main`, local only: no remote, nothing pushed, nothing deployed. The last commit is whatever `git log -1` shows.
- Nothing uncommitted is expected.

## Done and verified

| Piece | Proof | Result |
|---|---|---|
| Investigation of ntfy | `docs/NTFY_REVERSE_ENGINEERING.md` | Source read at server `59f9931`, Android `d9e11ba`, iOS `96e659c`; live read-only probes |
| Publish, subscribe, replay, scheduling, updates, attachments, tokens, rate limits, upstream forward | `go test -race ./...` | Pass |
| Same wire behavior as ntfy | `REF=<ntfy binary> scripts/conformance.sh` | Identical: 128 protocol, 21 auth, 1 limits steps |
| Real ntfy CLI against Herald | By hand, 2026-11-05 | Publish, delay, file, rejected anonymous, poll, live subscribe all worked |
| Built-in page | Browser, 2026-11-05 | Live receive and send work; HTML in a message stays text |
| Load | `tools/loadtest`, 2,000 subscriptions, 5,000 messages, one run on this Mac | 500,000 of 500,000 delivered |
| Local CI (`scripts/ci-local.sh`, Linux container, clean checkout of `4276628`) | gofmt, vet, race tests, static build, govulncheck, conformance | Pass; no known vulnerabilities |
| Docker image (`docker build`, 32.7 MB) | Ran it as non-root with a volume: publish, token add, restart | Message survived the restart |


## In flight

Nothing.

## Open findings and decisions

| Item | Severity | Who decides |
|---|---|---|
| Official Android and iOS apps have not been run against Herald. Every URL they call is implemented, but delivery on a real phone is unverified. | High, this is the product's main use | Needs a device; owner |
| iOS instant delivery needs either the ntfy.sh upstream (it sees message IDs, topic hashes, timing) or our own app with an Apple Developer account | Decision | Owner |
| Scope for push: official apps only, or our own Firebase and apps | Decision | Owner |
| Where to deploy. Railway is available. It needs a persistent volume for `/data`, plus `-behind-proxy` once the proxy's `X-Forwarded-For` behavior is confirmed (`internal/server/limits.go`, `clientIP`) | Decision | Owner |
| Name ("Herald" is a placeholder) and license (none yet) | Low | Owner |
| Rate limit counters are in memory and reset on restart (`internal/server/limits.go`) | Low | Accepted for now |
| The app's browser pane launcher could not start Herald from this volume; a shell process could. Not diagnosed. | Low, tooling only | Nobody needs to act |

## Next steps, in order

Nothing below has been requested yet, so none of it should start without the owner asking.

1. Test on a real phone: deploy somewhere reachable, point the official Android app at it, send a message with the app in the background.
2. Decide the iOS route, then test it the same way.
3. Deploy with a persistent volume, and create real tokens.
4. Then, as needed: Web Push (so browsers alert with the tab closed), email, metrics.

## How to resume

```bash
git log --oneline
```

```bash
scripts/ci-local.sh --here
```

CI on a clean checkout of `HEAD`, in Linux (needs Docker):

```bash
scripts/ci-local.sh
```

To re-run the comparison with ntfy, rebuild the reference outside this repo. The server embeds its web assets, so
create placeholders first:

```bash
git clone https://github.com/binwiederhier/ntfy.git /tmp/ntfy-src && cd /tmp/ntfy-src && git checkout 59f9931 && mkdir -p server/site server/docs && touch server/site/app.html server/docs/index.html && go build -tags 'nopayments,nofirebase,nowebpush' -o /tmp/ntfy-ref .
```

```bash
REF=/tmp/ntfy-ref scripts/conformance.sh
```

That rewrites `tools/conformance/golden/`, the only input the tests depend on, and it is committed. The
ntfy source clones used during the investigation were in a session scratchpad and are not needed to resume.
