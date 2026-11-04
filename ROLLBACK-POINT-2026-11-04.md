# Bankroll rollback point — 2026-11-04

This branch is the protected pre-migration code checkpoint.

- Protected branch: `backup/bankroll-before-drive-storage-2026-11-04`
- Original application entry point: `index.js`
- Live Render service: `https://bankroll-bot-2.onrender.com`

## Restore the code checkpoint

Do not run a storage migration against this branch. To restore the application code to this checkpoint, deploy this branch in Render (Settings / Build & Deploy / Branch), or locally run:

```bash
git fetch origin
git switch backup/bankroll-before-drive-storage-2026-11-04
```

To move the deployment back to the previous branch after testing, set Render's branch back to its original production branch and redeploy the last known-good commit.

This checkpoint protects the repository code only. It is not a snapshot of Render's ephemeral `/tmp/bankroll-data`, live invoices, payment records, or Google Drive contents. Keep the existing Discord and Drive backups intact.
