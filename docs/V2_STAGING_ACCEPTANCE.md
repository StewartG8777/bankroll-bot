# Bankroll V2 rollout safety

## Required staging isolation
Run a separate staging deployment with a separate Discord bot token, guild, backup channel, Stripe test keys, Monzo test/sandbox arrangements, Google OAuth credentials, Drive root and data directory. Never reuse production customer records or payment webhooks in staging. The code must not be promoted until the staging environment is verified.

## Acceptance checks
- All owner-only commands (/storage, /system, /orders, /customer, /file, /backup, /recovery, /logs, /performance, /settings) register and enforce permissions.
- Simulate two concurrent writes to the same JSON record; verify both survive a restart and Drive is authoritative.
- Test failed and delayed Drive writes; ensure no delivery is acknowledged without durable customer file and record.
- Test webhook duplicates and failed payment retries without charging or delivering twice.
- Upload a large file, restart with an empty local cache, download from Drive, and compare SHA-256.
- Test one-time, expired, interrupted and concurrent downloads and download-history retention.
- Run backup and recovery using separate staging credentials. Corrupt one backup object and verify restore refuses it.
- Verify seven daily, four weekly and three monthly retained recovery points without deleting the only good snapshot.
- Verify ShareX custom uploader returns a real Bankroll image URL and ShareX copies it automatically when its After Upload task is set to Copy URL to clipboard.
- Record baseline memory, CPU, event-loop delay and startup duration before claiming performance improvement.
- Verify recovery and rollback with the previous working deployment before production cutover.

## ShareX setup
Import `sharex/Bankroll.sxcu` into ShareX custom uploaders. Replace the host and token placeholders locally. In ShareX, choose this custom image uploader and enable **After upload tasks → Copy URL to clipboard**. The Bankroll endpoint returns JSON `{ "url": "https://.../s/..." }`; ShareX resolves this with `$json:url$`. Do not commit a real token.
