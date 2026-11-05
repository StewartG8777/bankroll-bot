# Google Drive primary storage — staged integration

## Preserved
The BANKROLL root and eight existing folder names remain VAULT, CUSTOMER
DELIVERIES, DATABASE, BACKUPS, PAYMENT RECORDS, LOGS, SYSTEM, TEMPORARY
PROCESSING. Existing Stripe/Monzo/invoice/delivery logic is unchanged.
Existing Discord + Google Drive backup uploads remain active.
Render still starts with node index.js.

## Added in PR #6
- Immutable Drive snapshot engine with SHA-256 and size verification and
  routing to existing folders. Snapshot references are stored in BACKUPS.
- A serialized storage queue; maintenance mode waits for queued snapshots
  and rejects new queued operations.
- Owner-only commands: /drive snapshot, /drive generations, and read-only
  /drive rollback-check generation:<uuid>.
- Automated mocked Drive end-to-end recovery, corruption and rollback tests.
- Offline-only verified rollback. Verify the target first:
      node scripts/offline-drive-rollback.js verify <generation>
  Stop **all** Bankroll bot instances, Discord commands, Stripe/Monzo webhook
  listeners and other writers. Run a one-off maintenance process with the same
  DATA_DIR and Google OAuth files:
      BANKROLL_MAINTENANCE_MODE=1 node scripts/offline-drive-rollback.js rollback <generation> --i-confirm-bot-is-stopped
  This checks file hashes, records a safety generation, restores the selected
  generation and only then promotes its current pointer. Never run it with
  the live bot active. The maintenance lock covers other offline utilities,
  NOT the existing live application writes. Reconcile newer payments before
  reopening services.

## IMPORTANT: full primary cutover is not yet enabled
Index.js still reads local state on startup and existing app mutations do
not all go through this new queue. The requested unchanged startup means
this PR cannot yet make Drive the authoritative store across Render restarts.
A reliable cutover needs startup recovery BEFORE loading application data
and durable commits for every persistent write, tested in staging with
network failures, empty storage and payment reconciliation. This cannot
be solved safely by a merge switch alone.
