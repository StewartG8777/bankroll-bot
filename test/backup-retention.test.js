'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { isManagedContentAddressedBackup } = require('../systems/backup-retention');

test('recognises content-addressed database backups', () => {
  assert.equal(isManagedContentAddressedBackup('0123456789abcdef01234567_payments.json'), true);
});

test('recognises content-addressed vault backups', () => {
  assert.equal(isManagedContentAddressedBackup('0123456789abcdef_0123456789abcdef01234567_customer-file.zip'), true);
});

test('preserves legacy and unrecognised objects', () => {
  for (const name of ['bankroll-drive-manifest.json', 'payments.json', 'random-file.zip', '']) {
    assert.equal(isManagedContentAddressedBackup(name), false);
  }
});
