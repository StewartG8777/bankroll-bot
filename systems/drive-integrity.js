'use strict';
const crypto = require('node:crypto');
async function auditRecoveryManifest(drive, manifest, { verifyContent = true } = {}) {
  const entries = [...(manifest.databaseFiles || []), ...(manifest.vaultFiles || []), ...(manifest.customerDeliveryFiles || [])];
  const seenPaths = new Map(), seenIds = new Map(), issues = [];
  let verified = 0;
  for (const entry of entries) {
    const name = String(entry.path || entry.name || '');
    if (!name || !entry.fileId || !/^[a-f0-9]{64}$/i.test(String(entry.sha256 || '')) ||
        !Number.isSafeInteger(Number(entry.size)) || Number(entry.size) < 0) {
      issues.push({ path: name, reason: 'invalid-manifest-entry' }); continue;
    }
    if (seenPaths.has(name)) issues.push({ path: name, reason: 'duplicate-path' });
    else seenPaths.set(name, entry);
    if (seenIds.has(entry.fileId) && seenIds.get(entry.fileId) !== name)
      issues.push({ path: name, reason: 'duplicate-file-id' });
    seenIds.set(entry.fileId, name);
    if (!verifyContent) continue;
    try {
      const response = await drive.files.get({ fileId: entry.fileId, alt: 'media' }, { responseType: 'stream' });
      const hash = crypto.createHash('sha256'); let bytes = 0;
      for await (const chunk of response.data) { bytes += chunk.length; hash.update(chunk); }
      if (bytes !== Number(entry.size)) issues.push({ path: name, reason: 'size-mismatch' });
      else if (hash.digest('hex').toLowerCase() !== entry.sha256.toLowerCase())
        issues.push({ path: name, reason: 'sha256-mismatch' });
      else verified++;
    } catch (error) { issues.push({ path: name, reason: 'missing-or-unreadable', detail: String(error.message || error) }); }
  }
  return { total: entries.length, verified, issues, ok: issues.length === 0 };
}
module.exports = { auditRecoveryManifest };
