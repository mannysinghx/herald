# How ntfy.sh works, and what we built from it

Investigated 2026-11-05. "neft.sh" in the request does not resolve; the target was confirmed as **ntfy.sh**.

ntfy is open source (dual Apache-2.0 / GPLv2), so nothing had to be decompiled. "Reverse engineering" here
means reading the published source, running it, and recording what it does on the wire.

| Evidence | Where |
|---|---|
| Server source | `github.com/binwiederhier/ntfy` at `59f9931` (2026-11-03) |
| Android app source | `github.com/binwiederhier/ntfy-android` at `d9e11ba` (2026-10-18) |
| iOS app source | `github.com/binwiederhier/ntfy-ios` at `96e659c` (2026-06-26) |
| Live service | Read-only GETs to `ntfy.sh/v1/{health,config,stats}`. No messages were published to it. |
| Reference binary | Built from that server source, run on localhost, driven by `tools/conformance` |

## The short version

ntfy is **an HTTP pub/sub server with a small message database**. A topic is just a name in a URL. Anyone who
knows the name can publish or subscribe unless access control is on, and on the public server the name is
effectively the password.

The server is the easy half. **Getting a notification onto a locked phone is the hard half**, and that part
depends on Google and Apple, not on ntfy. See [Reaching a phone](#reaching-a-phone).

The public service answered, at the time of writing: 1.41 billion messages all time, about 97 per second,
behind nginx, one active node (`{"healthy":true,"role":"active"}`).

## Architecture

```
 publisher                     server                                subscribers
 ---------                     ------                                -----------
 curl -d hi /topic  --->  auth, rate limit                     +-->  /topic/json   (NDJSON stream)
                          parse headers or query params        +-->  /topic/sse    (Server-Sent Events)
                          body is message or attachment?       +-->  /topic/raw    (text lines)
                          fan out to open connections  --------+-->  /topic/ws     (WebSocket)
                          write to SQLite or Postgres
                          fire side effects in goroutines:     ---> Firebase (Android, iOS via APNs)
                                                               ---> Web Push (browsers)
                                                               ---> SMTP, Twilio calls
                                                               ---> upstream server (wakes iOS)
```

The core fits in a few files: `server/server.go` (routing and publish), `server/topic.go` (fan-out),
`message/` (cache), `visitor/` and `limit/` (rate limits). `dispatch()` in `server.go` is the single point
every published message passes through.

### The rules that matter

**Publishing.** Every option works as a header or a query parameter, with aliases (`X-Title`, `Title`, `t`), so
that webhooks which cannot set headers still work. `PUT`/`POST /<topic>`, `POST /` with JSON, and
`GET /<topic>/publish` all reach the same handler.

**Message or attachment.** Decided by looking at the body: a filename makes it an attachment, an external URL
makes it a message, and otherwise valid UTF-8 under 4096 bytes is a message and anything else is an attachment.
A body of exactly 4096 bytes becomes an attachment (`Peek` sets `LimitReached` when `read == limit`).

**Subscribing.** The stream opens with an `open` event, then replays history if asked, then delivers live with
a `keepalive` every 45 seconds. WebSockets use ping frames instead of keepalive events. In SSE only non-message
events get an `event:` line, because a named event would not fire a browser's `onmessage`.

**Catching up.** `since=` takes `all`, `none`, `latest`, a message ID, a unix time, or a duration. Clients store
the last ID they saw and reconnect with it. An unknown ID replays everything still stored, and clients dedupe by
ID. Messages live 12 hours by default.

**Updates are events.** Updating or deleting a notification publishes a new message with the same `sequence_id`
(`message_delete`, `message_clear`). Nothing is removed from history, so offline clients learn about it on replay.

**Scheduling.** A delayed message is stored with `published=0` and a future `time`. A loop sends what is due
every 10 seconds. Allowed range: 10 seconds to 3 days.

**Access control.** Off unless `auth-file` is set. Users, tokens, and per-topic rules with wildcards. A browser
WebSocket cannot set headers, so `?auth=` carries the base64url of the `Authorization` header.

**Rate limits**, per IP (/32 or /64) or per user: 60 request burst refilling 1 per 5 seconds, 17,280 messages a
day, 30 open subscriptions, 100 MB of attachments, 500 MB of daily attachment traffic. Download traffic is charged
to the *uploader*, so one upload shared widely cannot burn other people's quota.

**Clustering** is new and marked experimental in the code: a mesh between nodes plus a one-second "tail" that
reads what was written, to catch what the mesh dropped.

## Reaching a phone

Phones kill background connections to save battery. This is the actual problem to solve, and each platform differs.

| Platform | How ntfy delivers | Evidence |
|---|---|---|
| **Android, ntfy.sh** | Firebase (FCM) data message to an FCM topic named after the ntfy topic | `server_firebase.go`, `MainViewModel.kt:45` |
| **Android, self-hosted** | App keeps a foreground service with a long-lived `/json` or `/ws` connection. No Google involved. Costs some battery. | `SubscriberService.kt`, `docs/config.md:1627-1628` |
| **iOS** | APNs is the only way in, and ntfy reaches it through FCM. No background sockets are allowed. | `AppDelegate.swift`, `docs/config.md:1646` |
| **iOS, self-hosted** | Your server sends `ntfy.sh` only the message ID and `sha256(topic URL)`. ntfy.sh wakes the phone, and the app then fetches `/topic/json?poll=1&id=<id>` from your server. | `forwardPollRequest` in `server.go`, `ApiService.swift:26`, `Helpers.swift:16` |
| **Browser** | Web Push with VAPID keys, to an allow-list of push services (the list exists to stop SSRF, GHSA-w9hq-5jg7-q4j7) | `server_webpush.go` |

Two details worth keeping if we build push ourselves:

- If a topic is not anonymously readable, ntfy sends FCM a contentless `poll_request` instead of the message, so
  Google never sees protected content.
- Android and iOS need different FCM payloads. iOS must carry a visible `alert` with `mutable-content`, truncated
  to 100 characters, or APNs may drop it; the app's notification service extension (`ntfyNSE`) can then rewrite it.

**Consequence.** Without shipping our own apps, Android can use the official ntfy app pointed at our server. iOS
can use the official app too, but only by forwarding to ntfy.sh, which then learns message IDs, topic hashes and
timing (not content or topic names). Instant iOS delivery with no third party needs our own iOS app, an Apple
Developer account, and our own APNs setup.

## What we built: Herald

A new implementation of the core in Go, about 3,000 lines plus tests, speaking the same wire protocol. It is not
a fork. It reuses the protocol, the error codes and the default limits, because clients depend on them.

### Verified

| Check | Result |
|---|---|
| **Differential test** against the reference ntfy (`scripts/conformance.sh`) | Transcripts identical: 128 protocol steps, 21 auth steps, 1 rate-limit step |
| **The diff can fail** | Removing one `TrimSpace` made the protocol scenario report a difference |
| **Real ntfy CLI** against Herald | Publish with token, delay and file upload, anonymous rejected, poll, and a live `subscribe` that ran its command with the right fields |
| **Race and property tests** (`go test -race`) | A client reconnecting mid-stream during 4 concurrent publishers saw 1,600 of 1,600 messages exactly once, in order |
| **Browser** | The built-in page received live, sent, and rendered an HTML injection attempt as text |

The transcripts remove what is random (IDs, timestamps) and ignore documentation links and repeated IDs within a stream.

**Load test**, one run, on this Mac with client and server on the same machine: 2,000 subscriptions on 20 topics,
5,000 messages, 16 publishers. Treat these as a rough comparison, not capacity figures.

| | Herald | ntfy (no Firebase, payments or Web Push) |
|---|---|---|
| Publish throughput | 3,283/s | 1,252/s |
| Deliveries | 500,000 of 500,000 | 500,000 of 500,000 |
| Delivery latency p50 / p99 | 5.0 ms / 12.2 ms | 0.35 ms / 2.0 ms |
| Memory after run | 135 MB | 176 MB |

ntfy's latency is lower because it delivers before writing to the database. Herald writes first, which is the
trade-off described below.

### Where we deliberately differ

| Topic | ntfy | Herald | Why |
|---|---|---|---|
| **Order of work** | Fan out, then store | Store, then fan out | A client that reconnects right after a message can always find it. Costs latency, as measured above. |
| **Delivery** | At least once. Observed: a replayed message arrives again, after newer ones, from the tail. | Exactly once per connection on one node | Register first, replay second, drop by position. Clients dedupe anyway, so this is tidiness rather than a fix. |
| **Replay cursor** | `since=<id>` also returns earlier messages from the same second. Observed: `a, b, c` in one second, `since=b` returns `a c`. | Returns `c` | Each message gets a delivery position when it goes out, so scheduled messages still sort correctly. |
| **Slow subscriber** | A goroutine per send, chained, unbounded while the client stalls | Fixed 256-message buffer, then disconnect; the client reconnects with `since=` | Bounded memory. A stuck client cannot affect anyone else. |
| **Default access** | Open | `deny` | A self-hosted service should not start out public. Pass `-default-access rw` for ntfy's behavior. |
| **Revocation** | Reacts to access changes through its user manager | Open streams re-check at each keepalive, so within 45 seconds | Simple, and it also catches removal by the CLI. |
| **Scheduler tick** | 10 seconds | 1 second | Delayed messages arrive closer to their time. |
| **Bad `?auth=`** | 500 | 401 | Not a server error. |
| **Scale** | SQLite or Postgres, experimental cluster | One node, SQLite | Enough until it is not; see the gaps below. |

### Not built

Requests for the first three get an explicit error rather than silently doing nothing.

| Feature | Status | What it would take |
|---|---|---|
| Email (`X-Email`) | Returns 40001 | SMTP settings, per-visitor limit. Small. |
| Phone calls (`X-Call`) | Returns 40032 | Twilio account. Small, costs per call. |
| Message templates | Returns 40043 | Go templates with ntfy's sandboxing. Medium; it has had a security advisory. |
| **Firebase / APNs push** | Not built | Firebase project, signed apps. **Large**, see above. |
| **Web Push** | Not built | VAPID keys, subscription table, service worker. Medium. Today the page only alerts while its tab is open. |
| Natural-language delay ("tomorrow, 10am") | Returns 40004 | One dependency. Small. |
| Accounts, tiers, billing, reservations | Not built | Tokens with topic rules cover self-hosting. Only needed for a public paid service. |
| Full web app, Matrix gateway, SMTP-to-topic | Not built | Unneeded so far. |
| `RateLimit` response headers, Prometheus metrics | Not built | Small. |
| UnifiedPush subscriber-based rate limiting | Not built | UnifiedPush publishing works but is limited by publisher IP. |
| Multiple nodes | Not built | Postgres, plus a mesh or tail. Large. |

Message counts and rate limits reset on restart, because they are in memory.

**Running behind Railway:** the right-most `X-Forwarded-For` entry there is a changing edge address, not the
client, so set `-proxy-header X-Real-IP`. Railway overwrites that header, and a client-sent one was ignored in testing.

**Not verified:** the official Android and iOS apps were never run against Herald, because no device or emulator
was used. I read their source and every URL they call is implemented, but real delivery on a phone is untested.
The upstream forward was tested against a fake upstream, not against ntfy.sh.

## Licensing note

Herald is released under Apache-2.0. ntfy's license permits this kind of reuse. Herald has its own code but mirrors
ntfy's protocol, error codes and default limits, and it was written with the source open, so `NOTICE` credits ntfy.
This is an engineering summary, not legal advice.
