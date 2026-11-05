# Activity log

Actions with an effect outside the working tree: timestamp · what · why · how to undo · verified?

- 2026-11-05 09:55 PST · Sent read-only GETs to ntfy.sh (`/v1/health`, `/v1/config`, `/v1/info`, `/v1/stats`, `/config.js`, `/manifest.webmanifest`, and an OPTIONS) · to observe the live service · nothing to undo · yes. No messages were published to ntfy.sh.
- 2026-11-05 09:56 PST · Cloned ntfy, ntfy-android and ntfy-ios, and built a reference ntfy binary, in the session scratchpad · evidence and differential testing · delete the scratchpad · yes. To rebuild, see "How to resume" in PROGRESS.md.
- 2026-11-05 10:20 PST · Started a Herald process through the app's browser pane launcher twice; it never opened its port and wrote no data, so it was stopped with `preview_stop`. Cause not diagnosed. Ran the page check from a shell process instead, and removed the launcher config · nothing left to undo · yes.
- 2026-11-05 10:30 PST · Pulled golang:1.26, golang:1.26-alpine and alpine:3.22 and built image `herald:local` in local Docker; ran a smoke container on 127.0.0.1:28197 with a temporary volume · CI and image check · `docker rmi herald:local` · yes, container stopped and volume removed.
- 2026-11-05 10:32 PST · Correction: the first image smoke run failed its restart step on a container name conflict, but the commit recording it already said the message survived a restart. Reran with separate containers on a fresh volume: non-root, exit code 0 on stop, message present after restart, so the claim now holds · accuracy of the record · container and volume removed · yes.
