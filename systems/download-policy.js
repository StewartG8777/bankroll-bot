'use strict';
// Centralised eligibility checks for hosted customer downloads.
function validateDownload({ record, token, active, safePath, now = Date.now() }) {
  if (!/^[a-f0-9]{48,64}$/i.test(String(token || ''))) return 'malformed';
  if (!record) return 'missing';
  if (!safePath) return 'unsafe-path';
  if (active) return 'in-progress';
  if (record.used && (!record.multiUse || !!record.vaultKey)) return 'used';
  if (record.expiresAt != null && now > Number(record.expiresAt)) return 'expired';
  return null;
}
function shouldConsumeDownload(record) {
  return !record.multiUse || !!record.vaultKey;
}
module.exports = { validateDownload, shouldConsumeDownload };
