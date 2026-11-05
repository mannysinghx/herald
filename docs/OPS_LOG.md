# Activity log

Actions with an effect outside the working tree: timestamp · what · why · how to undo · verified?

- 2026-11-05 09:55 PST · Sent read-only GETs to ntfy.sh (`/v1/health`, `/v1/config`, `/v1/info`, `/v1/stats`, `/config.js`, `/manifest.webmanifest`, and an OPTIONS) · to observe the live service · nothing to undo · yes. No messages were published to ntfy.sh.
- 2026-11-05 09:56 PST · Cloned ntfy, ntfy-android and ntfy-ios, and built a reference ntfy binary, in the session scratchpad · evidence and differential testing · delete the scratchpad · yes. To rebuild, see "How to resume" in PROGRESS.md.
- 2026-11-05 10:20 PST · Started a Herald process through the app's browser pane launcher twice; it never opened its port and wrote no data, so it was stopped with `preview_stop`. Cause not diagnosed. Ran the page check from a shell process instead, and removed the launcher config · nothing left to undo · yes.
