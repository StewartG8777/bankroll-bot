# Drive-first authoritative storage — STAGING ONLY

This branch is NOT deployed. Production Render and main must remain unchanged
until live Google Drive/Discord OAuth, migration, restart and payment-retry
verification have passed in an isolated staging service.

## Existing behavior retained
- BANKROLL retains all eight folders (VAULT, CUSTOMER DELIVERIES, DATABASE,
  BACKUPS, PAYMENT RECORDS, LOGS, SYSTEM, TEMPORARY PROCESSING).
- Stripe/Monzo transaction logic, invoice and delivery business logic is not
  restructured; webhook 2xx acknowledgments and Discord replies are delayed
  until storage confirms the generation.
- Existing independent Discord backups and legacy Google Drive backup uploads
  remain available; primary startup does not silently import an older backup
  over a verified primary snapshot.
- SHA-256 and byte-size checks are required on every restored primary object.
  Changed objects are uploaded to the existing folders; unchanged objects
  reuse their immutable content IDs.

## New startup mode
The npm start script invokes primary-bootstrap.js. Without
BANKROLL_DRIVE_PRIMARY=1 it starts the existing bot as before. Primary mode
refuses to load index.js directly, logs into Discord only when it must recover
encrypted OAuth credentials, resolves existing BANKROLL folders without
creating duplicates, and **refuses to start** if no complete verified primary
snapshot can be restored.

The first migration must have a verified original source before enabling
primary storage. The explicit, one-time value
BANKROLL_DRIVE_INITIALIZE=I_HAVE_VERIFIED_BACKUPS permits seed from a
NONEMPTY, VALID and verified local database, not an empty Render cache.
Never enable this until the original data and independent recovery points
have been inspected. Remove the initialization variable immediately after
the first generation is committed. If an authenticated primary pointer exists
but is damaged, bootstrap refuses to overwrite it with local data.

BANKROLL_BACKUP_ENCRYPTION_KEY must remain unchanged. Configure exactly one
bot writer. The local .drive-primary-writer.lock is not a distributed
multi-instance lock; two separate Render instances must never write to the
same primary Drive generation concurrently.

## Consistency restrictions
All writes made through atomicWrite(), explicit non-payment database
commits and the existing scheduled-backup path mark the authoritative
snapshot dirty. A single in-process queue serializes generations.
Payment webhook 2xx responses and Discord reply/update/follow-up success
must wait until that queue confirms the latest committed generation;
temporary Drive failures surface as retryable/visible errors instead of
silent success. The independent backup uploader is left intact.

Legacy startup recovery is skipped only in primary mode. Payment
reconciliation remains enabled. Snapshot creation includes all allowed
customer files and JSON records; OAuth tokens and .env remain excluded.

## Tests still required before production
Run Actions CI, a staged cutover using a copy of real filenames with
synthetic/customer-redacted values, restart with an empty local cache, retry
failed payments/webhooks idempotently, corrupt/miss Drive objects, test
token rotation recovery, verify vault paths after DATA_DIR relocation, test
Drive downtime, and audit outbound acknowledgments made outside the shared
reply/webhook paths. The snapshot queue is currently in-process, so
multi-instance deployment must remain prohibited.

Do NOT copy this staging startup command or set primary mode on the live
Render service until all of these conditions are validated.
