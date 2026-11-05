# Bankroll backup and recovery procedure

## Existing automated protection
- After startup recovery, the bot creates a new backup and schedules another every six hours, even if no files change.
- Discord backup snapshots are separate from Google Drive recovery manifests.
- Google Drive retention selects the latest recovery points for seven UTC days, four UTC weeks, and three UTC months. It must not delete objects referenced by retained manifests.
- Recovery reads the newest Drive manifest and verifies **every referenced database, vault and customer delivery object** against its recorded SHA-256 and byte size before restoring any missing local file. Missing objects, corrupt bytes, and duplicate manifest paths or file IDs stop the restore.

## Safe rollback procedure (operator-controlled)
1. Stop the bot and prevent new payments, invoices and file deliveries while recovery is in progress.
2. Preserve the existing data directory and the current Drive manifests. Do not delete or overwrite the last known-good copy.
3. Use a separate staging environment and staging data directory. Never point a staging process at the production Discord backup channel or Drive root.
4. Restore the selected recovery snapshot into staging. Check the recovery manifest's timestamp and all SHA-256 and size results; review customer, vault and invoice references.
5. Verify that Monzo and Stripe records are preserved independently; never overwrite payment records from a non-payment database recovery.
6. Only after successful verification, arrange the production rollback in a maintenance window, preserving the previous working deployment and a current backup.
7. Start the bot, check delivery/download access and monitor errors. If verification fails, keep the production instance stopped and return to the previous deployment and saved data.

**Limitations:** The automated Drive restore currently selects the newest recovery point and restores only missing files. It is not an arbitrary-point, transactional production rollback. Do not describe a rollback as verified until the staging exercise above has passed.
