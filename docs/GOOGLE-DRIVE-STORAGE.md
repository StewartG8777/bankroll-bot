# Google Drive primary-storage work (PR #6)

This branch is rebased onto the current bot without replacing live payment logic, changing `node index.js`, or altering the existing BANKROLL folder layout. The new snapshot engine is **not activated in production**.

Folders remain: VAULT, CUSTOMER DELIVERIES, DATABASE, BACKUPS, PAYMENT RECORDS, LOGS, SYSTEM, TEMPORARY PROCESSING.

The new engine writes content-addressed data objects to the appropriate existing folder, then immutable manifests and one current pointer under BACKUPS. It performs SHA-256/size checks before restoration. Current Google Drive and Discord backup uploads continue unchanged.

**Rollout blocker:** a reliable primary storage cutover requires restoring authoritative data before `index.js` loads it and routing every persistent mutation through a durable serialized queue. The user requested leaving current startup and payment behavior unchanged. Therefore this PR provides opt-in storage infrastructure and tests, **not an enabled primary cutover**. Never set a production primary toggle or assume local state is restart-safe until integration and staging drills pass.
