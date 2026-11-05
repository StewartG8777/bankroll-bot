'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { atomicWriteFile, isSafeDatabaseName } = require('../systems/atomic-storage');
test('atomic writer replaces complete file without leaving temporary files', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bankroll-atomic-'));
  try {
    const target = path.join(dir, 'data.json');
    atomicWriteFile(target, '{"v":1}');
    atomicWriteFile(target, '{"v":2}');
    assert.deepEqual(JSON.parse(fs.readFileSync(target, 'utf8')), { v: 2 });
    assert.deepEqual(fs.readdirSync(dir), ['data.json']);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
test('database path guard rejects directory traversal', () => {
  assert.equal(isSafeDatabaseName('vault.json'), true);
  assert.equal(isSafeDatabaseName('../vault.json'), false);
  assert.equal(isSafeDatabaseName('vault.json/other'), false);
});
