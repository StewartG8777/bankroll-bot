'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');
const { safeRelativePath, isAllowedRelativePath, walkFiles, FORMAT } = require('../systems/google-drive-primary-storage');

test('primary snapshot format is versioned', () => {
  assert.equal(FORMAT, 'bankroll-primary-snapshot-v1');
});

test('rejects traversal and absolute paths', () => {
  for (const value of ['../vault.json', '/etc/passwd', 'C:\\\\secret', 'vault/../../x', '']) {
    assert.equal(safeRelativePath(value), null, value);
  }
});

test('allows safe Bankroll data paths and excludes secrets/temp folders', () => {
  assert.equal(isAllowedRelativePath('vault/catalog.json'), true);
  assert.equal(isAllowedRelativePath('files/customer.bin'), true);
  assert.equal(isAllowedRelativePath('google-oauth.json'), false);
  assert.equal(isAllowedRelativePath('google-drive-token.json'), false);
  assert.equal(isAllowedRelativePath('.env'), false);
  assert.equal(isAllowedRelativePath('tmp/upload.part'), false);
  assert.equal(isAllowedRelativePath('node_modules/pkg/index.js'), false);
});

test('walkFiles includes data files but excludes temporary and OAuth files', async () => {
  const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'bankroll-drive-test-'));
  try {
    await fs.promises.mkdir(path.join(root, 'vault'), { recursive: true });
    await fs.promises.mkdir(path.join(root, 'tmp'), { recursive: true });
    await fs.promises.writeFile(path.join(root, 'deliveries.json'), '{}');
    await fs.promises.writeFile(path.join(root, 'google-oauth.json'), '{"secret":true}');
    await fs.promises.writeFile(path.join(root, 'google-drive-token.json'), '{"refresh_token":"secret"}');
    await fs.promises.writeFile(path.join(root, 'vault', 'file.bin'), Buffer.from([1, 2, 3]));
    await fs.promises.writeFile(path.join(root, 'tmp', 'upload.part'), 'temp');
    const files = await walkFiles(root);
    assert.deepEqual(files.map((f) => f.relative).sort(), ['deliveries.json', 'vault/file.bin']);
  } finally {
    await fs.promises.rm(root, { recursive: true, force: true });
  }
});
