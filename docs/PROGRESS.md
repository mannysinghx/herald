# Progress

## Where it stopped

- Branch `main`, published at https://github.com/mannysinghx/herald (public, Apache-2.0). The last commit is whatever `git log -1` shows.
- **Deployed** to Railway: https://herald-production-ec87.up.railway.app
- **A push to `main` does NOT deploy yet.** The service's source is set to the repo, but it has no deploy trigger: creating one fails with "no one in the project has access to it", because Railway's GitHub app has not been given this repo. The live service runs `737255f`; `35c5bf6` only changed `.dockerignore` and is not deployed.
- Railway project `Herald` (`a6441bb1-8adb-42ba-acb3-85555bc93dfb`), environment `production`, service `herald` (`8627fcb8-a225-4ccf-80b6-ba310c639b78`), volume `herald-data` at `/data`.
- Variables: `PORT=8080`, `RAILWAY_RUN_UID=0` (Railway volumes are root-owned), `HERALD_BASE_URL`, `HERALD_ATTACHMENT_DIR=/data/attachments`, `HERALD_BEHIND_PROXY=true`, `HERALD_PROXY_HEADER=X-Real-IP`, `HERALD_VERBOSE=false`. Access is deny by default.
- **The live server has no tokens**, so nothing can publish or subscribe until the owner creates one (command below).
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

| Live on Railway | Through the public URL, 2026-11-05 | Token publish and poll, wrong token 401, other topic 403, anonymous 403; JSON and SSE arrive in about 200 ms, unbuffered; WebSocket; attachment round trip; scheduled send; messages and tokens survive a redeploy |
| Live rate limit | 75 requests from one client | 62 allowed, 13 limited; spoofed `X-Forwarded-For` or `X-Real-IP` did not escape it; a client on another IP was unaffected |
| Published repo | Fresh clone of `github.com/mannysinghx/herald`, 2026-11-05 | Tests pass, `go install` works, GitHub detects Apache-2.0, no secrets or personal IP in history by regex scan |
| Page errors | Browser against a locked local copy, 14 cases | Bad topics, missing and wrong tokens, and tokens without permission all get a specific message; valid topic goes live and shows sent messages. Not re-run in a browser on the live URL; there the new page was confirmed served and the same responses checked with curl. |

## In flight

Nothing.

## Open findings and decisions

| Item | Severity | Who decides |
|---|---|---|
| Official Android and iOS apps have not been run against Herald. Every URL they call is implemented, but delivery on a real phone is unverified. | High, this is the product's main use | Needs a device; owner |
| iOS instant delivery needs either the ntfy.sh upstream (it sees message IDs, topic hashes, timing) or our own app with an Apple Developer account | Decision | Owner |
| Scope for push: official apps only, or our own Firebase and apps | Decision | Owner |
| Railway may cap how long one HTTP stream stays open; not measured here. Clients reconnect with `since=`, so a cap should cost a reconnect rather than messages, but this is unverified on Railway. | Low | Measure if streams drop |
| Auto-deploy: the owner must add `mannysinghx/herald` to Railway's GitHub app (https://github.com/settings/installations). Then create the trigger and prove it with a push to a watched path: the watch patterns are `main.go`, `internal/**`, `go.mod`, `go.sum`, `Dockerfile`, `.dockerignore`. | High, blocks auto-deploy | Owner |
| Deployment `de7d0b07` built `737255f`, a docs-only commit, at 20:03 UTC with no trigger registered. Cause unknown; it may have been started from the dashboard. | Low | Recheck after the trigger exists |
| Project name: created as `Herald`, assuming that is what "same name" meant, not the folder name `NotificationNTFY`. Renaming is free; the generated URL would not change. | Low | Owner |
| Rate limit counters are in memory and reset on restart (`internal/server/limits.go`) | Low | Accepted for now |
| The app's browser pane launcher could not start Herald from this volume; a shell process could. Not diagnosed. | Low, tooling only | Nobody needs to act |

## Next steps, in order

Nothing below has been requested yet, so none of it should start without the owner asking.

1. Owner creates a token (see below).
2. Test on a real phone: point the official Android app at the live URL, send a message with the app in the background.
3. Decide the iOS route, then test it the same way.
4. Then, as needed: Web Push (so browsers alert with the tab closed), email, metrics.

## How to resume

Create a token on the live server. The secret is printed once, in your terminal only:

```bash
railway ssh --project a6441bb1-8adb-42ba-acb3-85555bc93dfb --environment production --service herald -- herald token add -db /data/herald.db -name owner -allow '*:rw'
```

Until auto-deploy works, deploy the working tree directly, after running local CI:

```bash
railway up --project a6441bb1-8adb-42ba-acb3-85555bc93dfb --environment production --service herald --detach -m "Deploy"
```

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
