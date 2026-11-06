# Single-instance deployment gate (staging)
The primary database migration must remain OFF in production until staging tests and operational checks pass.

Single-writer mode is only appropriate when exactly one Render service/process can write the BANKROLL Drive folders. In Render, configure one service instance and disable any separate staging, cron, worker, old deployment, or local bot process using the same Drive root. Verify that overlapping old/new deployments cannot both write; use a maintenance window and stop the old instance before starting the new primary writer.

The local `.drive-primary-writer.lock` uses atomic exclusive creation to block another process on the SAME filesystem. It deliberately refuses to remove an existing lock automatically, because doing so could permit simultaneous writers. If a crash leaves a stale lock, verify the prior writer is fully stopped before manually clearing it. The lock cannot protect across separate disks, Render deployments, or machines.

Only after manually verifying single-writer deployment should the operator set `BANKROLL_PRIMARY_SINGLE_INSTANCE_CONFIRMED=I_HAVE_VERIFIED_ONE_INSTANCE`. This variable records an operational check; it does not enforce Render scaling and is not a distributed lock.

Do not enable `BANKROLL_DRIVE_PRIMARY=1` until the separate cold-recovery, financial-data, payment-sandbox and rollback tests are approved. Preserve independent backups and keep primary migration PR unmerged until validated.
