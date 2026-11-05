'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { validateDownload, shouldConsumeDownload } = require('../systems/download-policy');
const token = 'a'.repeat(48);
const valid = { used: false, multiUse: false, expiresAt: null };
const check = (record, overrides = {}) => validateDownload({ record, token, active: false, safePath: true, now: 1000, ...overrides });
test('unused one-time link is allowed and must be consumed', () => {
  assert.equal(check(valid), null);
  assert.equal(shouldConsumeDownload(valid), true);
  assert.equal(check({ ...valid, used: true }), 'used');
});
test('expired and malformed links are rejected', () => {
  assert.equal(check({ ...valid, expiresAt: 999 }), 'expired');
  assert.equal(check(valid, { token: 'bad' }), 'malformed');
  assert.equal(check(null), 'missing');
  assert.equal(check(valid, { safePath: false }), 'unsafe-path');
});
test('concurrent transfers blocked; interrupted transfer can be retried', () => {
  assert.equal(check(valid, { active: true }), 'in-progress');
  const interrupted = { ...valid, used: true };
  assert.equal(check(interrupted), 'used');
  interrupted.used = false; // endpoint resets after close/error
  assert.equal(check(interrupted), null);
});
test('multi-use links remain usable unless vault-linked', () => {
  assert.equal(shouldConsumeDownload({ multiUse: true, vaultKey: null }), false);
  assert.equal(check({ ...valid, used: true, multiUse: true }), null);
  assert.equal(shouldConsumeDownload({ multiUse: true, vaultKey: 'item' }), true);
  assert.equal(check({ ...valid, used: true, multiUse: true, vaultKey: 'item' }), 'used');
});
