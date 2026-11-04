# Google Drive primary-storage migration

## Safety checkpoint

Before any rollout, keep the protected branch
\`backup/bankroll-before-drive-storage-2026-11-04\` unchanged. The production
branch and Render service must not be switched until the migration has passed
staging verification.

## Storage engine foundation

\`systems/google-drive-primary-storage.js\` implements immutable, content-addressed
Drive snapshots, a single current-generation pointer, SHA-256/size verification,
safe path validation, and staged restore (all files are downloaded and verified
before local files are replaced). It excludes OAuth credentials, environment
files, temporary uploads, logs, and dependency directories.

## Required integration sequence

1. At process bootstrap, establish Google OAuth and resolve the five Bankroll
   Drive folders before loading any application JSON into memory.
2. Restore the current Drive generation into a clean staging data directory.
   Verify every object and the manifest before atomically promoting the staged
   directory to \`DATA_DIR\`.
3. Only then import/start the existing application module and enable payment
   webhooks, Monzo polling, Stripe reconciliation, and Discord commands.
4. Route every persistent mutation through a serialized durable commit queue.
   A financial mutation must not be acknowledged until its Drive commit has
   been confirmed. On Drive outage, fail closed for financial writes rather
   than silently accepting local-only changes.
5. Preserve Discord backups as an independent recovery channel. Never delete
   or overwrite the existing Drive backup layout during migration.
6. Add an owner-only rollback operation that pins a chosen immutable generation,
   stages and verifies it, then restores it under a maintenance lock. Keep the
   previous generation and Discord recovery point until a post-restore health
   check succeeds.

## Important rollout status

This module is a storage-engine foundation, not yet the live primary-storage
cutover. The existing \`index.js\` loads many JSON files at module evaluation
time and performs direct synchronous writes. A correct cutover must move the
application startup behind the bootstrap barrier and route all persistent
writes through the durable commit queue. Do not set \`DATA_DIR\` or deploy this
module alone expecting Drive to become primary.
