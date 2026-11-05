'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { Readable } = require('node:stream');
const { auditRecoveryManifest } = require('../systems/drive-integrity');
const body = Buffer.from('known-good-backup');
const sha256 = crypto.createHash('sha256').update(body).digest('hex');
const entry = (name, id = name) => ({ name, fileId: id, size: body.length, sha256 });
const drive = (data = body) => ({ files: { get: async () => ({ data: Readable.from([data]) }) } });
test('valid recovery point verifies all bytes', async () => {
  const result = await auditRecoveryManifest(drive(), { databaseFiles: [entry('vault.json')] });
  assert.equal(result.ok, true); assert.equal(result.verified, 1);
});
test('corrupted bytes and missing objects fail verification', async () => {
  const bad = await auditRecoveryManifest(drive(Buffer.from('corrupted')), { databaseFiles: [entry('vault.json')] });
  assert.equal(bad.ok, false);
  const missing = await auditRecoveryManifest({ files: { get: async () => { throw Error('404'); } } }, { vaultFiles: [entry('vault/a')] });
  assert.equal(missing.issues[0].reason, 'missing-or-unreadable');
});
test('duplicate paths and duplicate file IDs are reported', async () => {
  const result = await auditRecoveryManifest(drive(), { vaultFiles: [entry('a','same'), entry('a','other'), entry('b','same')] }, { verifyContent: false });
  assert.equal(result.ok, false);
  assert.ok(result.issues.some(i => i.reason === 'duplicate-path'));
  assert.ok(result.issues.some(i => i.reason === 'duplicate-file-id'));
});
