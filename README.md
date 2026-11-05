# Herald

A self-hosted notification service. Publish with a single HTTP request; subscribe from a script, a browser, or
the ntfy apps. One binary and one SQLite file. It speaks the [ntfy](https://ntfy.sh) wire protocol, verified
against ntfy itself.

How ntfy works and how this differs: [docs/NTFY_REVERSE_ENGINEERING.md](docs/NTFY_REVERSE_ENGINEERING.md).
Current state and next steps: [docs/PROGRESS.md](docs/PROGRESS.md).

## Run it

Needs Go 1.26.

```bash
go install github.com/mannysinghx/herald@latest
```

Or use Docker. Keep `/data` on a volume, or tokens and message history are lost with the container.

```bash
docker build -t herald . && docker run -p 8080:8080 -v herald-data:/data herald
```

Access is denied by default, so create a token first. It is shown once; only its hash is stored.

```bash
herald token add -name laptop -allow 'alerts-*:rw,logs:ro'
```

```bash
herald serve -listen :8080 -base-url https://notify.example.com -attachment-dir ./attachments
```

## Use it

```bash
curl -H "Authorization: Bearer $TOKEN" -d "Backup finished" http://localhost:8080/alerts-backup
```

```bash
curl -H "Authorization: Bearer $TOKEN" -H "Title: Disk almost full" -H "Priority: urgent" -H "Tags: warning" -d "db-1 at 91%" http://localhost:8080/alerts-disk
```

```bash
curl -N -H "Authorization: Bearer $TOKEN" http://localhost:8080/alerts-disk/json
```

Open `http://localhost:8080/alerts-disk` for a small live page.

| To do this | Use |
|---|---|
| Publish | `PUT` or `POST /<topic>`, `POST /` with JSON, `GET /<topic>/publish?message=...` |
| Options | Headers or query parameters: `Title`, `Priority`, `Tags`, `Click`, `Icon`, `Actions`, `Attach`, `Filename`, `Markdown`, `Delay`, `Cache`, `X-Sequence-ID` |
| Subscribe | `GET /<topic>/json`, `/sse`, `/raw`, `/ws`; several topics as `/a,b/json` |
| Catch up | `?since=all`, `latest`, `none`, a message ID, a unix time, or a duration like `10m` |
| Fetch once | `?poll=1`, plus `&scheduled=1` to include messages not sent yet |
| Filter | `?priority=4,5`, `?tags=a,b`, `?title=`, `?message=`, `?id=` |
| Update or remove | `PUT /<topic>/<sequence-id>`, `PUT /<topic>/<sequence-id>/clear`, `DELETE /<topic>/<sequence-id>` |
| Authenticate | `Authorization: Bearer <token>`, Basic auth with the token as password, or `?auth=<base64url of the header>` |

## Phones

- **Android:** in the official ntfy app, add a subscription and enter your server URL. The app keeps its own
  connection open, with no Google services involved.
- **iOS:** the official app needs a push from a server connected to Apple. Run with `-upstream-base-url https://ntfy.sh`,
  which sends ntfy.sh each message's ID and a hash of its topic URL, but no content.

Neither app has been tested against Herald on a real device yet.

## Settings

Each flag also has an environment variable, such as `HERALD_BASE_URL`. `PORT` sets the listen port.

| Flag | Default | Meaning |
|---|---|---|
| `-listen` | `:8080` | Listen address |
| `-base-url` | | Public URL; needed for attachments and iOS |
| `-db` | `herald.db` | SQLite file |
| `-attachment-dir` | off | Attachment directory; needs `-base-url` |
| `-default-access` | `deny` | Access with no matching token rule: `deny`, `ro`, `wo`, `rw` |
| `-behind-proxy` | off | Rate limit by the address in `-proxy-header`. Only set this behind a proxy that writes that header, or clients can pick their own limit bucket. |
| `-proxy-header` | `X-Forwarded-For` | Header holding the client address. Use `X-Real-IP` on Railway. |
| `-verbose` | off | Debug logging, including the client address headers of each request |
| `-cache-duration` | `12h` | How long messages stay available for replay |
| `-keepalive` | `45s` | Keepalive interval, and how quickly a revoked token loses its open streams |
| `-upstream-base-url`, `-upstream-token` | off | Forward poll requests, to wake iOS devices |
| `-rate-limit-exempt` | | CIDRs without rate limits |

Tokens: `herald token add -name N -allow RULES`, `herald token list`, `herald token rm NAME_OR_ID`. The first
rule matching a topic applies, so put specific rules before broad ones. Topics no rule matches fall back to
`-default-access`.

With a host that restarts containers, keep the database on a persistent volume, or replay history and tokens are
lost. The Docker image stores it in `/data`.

## Deploy to Railway

Railway builds the included Dockerfile. Give the service a volume at `/data` and set these variables, plus
`HERALD_BASE_URL` once it has a domain:

| Variable | Value | Why |
|---|---|---|
| `RAILWAY_RUN_UID` | `0` | Railway volumes are owned by root, and the image runs as a regular user |
| `HERALD_BEHIND_PROXY` | `true` | Rate limit clients rather than the proxy |
| `HERALD_PROXY_HEADER` | `X-Real-IP` | On Railway, the last `X-Forwarded-For` entry is a changing edge address, not the client |
| `HERALD_ATTACHMENT_DIR` | `/data/attachments` | Optional: enables attachments |

Create a token from your terminal:

```bash
railway ssh --service herald -- herald token add -db /data/herald.db -name me -allow '*:rw'
```

## Test it

```bash
go test -race ./...
```

Full local CI on a clean checkout of `HEAD`, in Linux. Run it before every push.

```bash
scripts/ci-local.sh
```

To re-record the ntfy transcripts in `tools/conformance/golden`, build ntfy (see the progress doc) and run:

```bash
REF=/path/to/ntfy scripts/conformance.sh
```

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE). Herald is an independent implementation of the ntfy protocol,
and is not affiliated with or endorsed by the [ntfy](https://github.com/binwiederhier/ntfy) project. If you want the
full feature set, including push through Firebase, email and a complete web app, use ntfy itself.
