# Bankroll Google Drive layout

Bankroll manages one root folder named `BANKROLL`. All managed folders are direct children of that root:

| Folder | Contents |
| --- | --- |
| `VAULT` | Original vault files and files referenced by vault entries |
| `CUSTOMER DELIVERIES` | Files attached directly to delivery/download records rather than linked to a vault item |
| `DATABASE` | General application JSON records |
| `BACKUPS` | Versioned recovery manifests, the current manifest pointer, and mass-backup ZIP exports |
| `PAYMENT RECORDS` | Monzo ledger/review data, Stripe balance history/snapshots, and webhook idempotency records |
| `LOGS` | Notification history and download history |
| `SYSTEM` | Non-secret runtime and storage status snapshots |
| `TEMPORARY PROCESSING` | Reserved for temporary work; excluded from backups and retention |

## Setup and migration

1. Connect the OAuth account with `/drive connect`.
2. Run `/drive setup` to create and verify the complete hierarchy.
3. Run `/drive backup` to create the first verified recovery point.
4. Run `/drive status` to check all eight folders.

The resolver scopes every child-folder lookup to its BANKROLL parent. It never selects an unrelated same-named folder elsewhere in Drive. If a unique legacy folder is found directly in the same My Drive root, it is moved under BANKROLL without changing its Drive file IDs. Ambiguous duplicates stop setup instead of guessing.

## Backup and recovery

Recovery manifests use `bankroll-drive-layout-v3`. The restore and retention code remains compatible with existing `bankroll-drive-layout-v2` manifests. Database/payment/log records, vault files, customer delivery files, and the system snapshot are checksummed and referenced by Drive file ID. Restore verifies SHA-256 and byte size before atomically placing a missing file locally. Existing non-empty local files are not overwritten.

Retention keeps the newest recovery point for each of the latest 7 UTC days, 4 UTC weeks, and 3 UTC months. It deletes only Bankroll-marked content-addressed objects that are not referenced by retained, validated manifests. Unknown and legacy Drive files are preserved.

Credential files, OAuth tokens, environment files, and private keys are not copied into the SYSTEM snapshot. The temporary-processing folder is not a backup destination.

## Storage boundary

The bot still uses its configured local data directory as its active filesystem/cache because existing Discord delivery and payment paths use Node filesystem streams. Google Drive is the durable, checksummed backup and recovery store; this layout change does not silently claim to make Drive a mounted transactional filesystem or change payment acknowledgement semantics.
