'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  GOOGLE_DRIVE_ROOT_NAME,
  GOOGLE_DRIVE_FOLDER_NAMES,
  GOOGLE_DRIVE_CONTENT_FOLDER_NAMES,
  googleDriveFolderForDatabaseFile,
} = require('../systems/drive-layout');

test('Bankroll Drive layout contains the complete eight-folder structure', () => {
  assert.equal(GOOGLE_DRIVE_ROOT_NAME, 'BANKROLL');
  assert.deepEqual([...GOOGLE_DRIVE_FOLDER_NAMES], [
    'VAULT',
    'CUSTOMER DELIVERIES',
    'DATABASE',
    'BACKUPS',
    'PAYMENT RECORDS',
    'LOGS',
    'SYSTEM',
    'TEMPORARY PROCESSING',
  ]);
});

test('all persistent content folders are included in the backup scope', () => {
  assert.deepEqual([...GOOGLE_DRIVE_CONTENT_FOLDER_NAMES], [
    'VAULT',
    'CUSTOMER DELIVERIES',
    'DATABASE',
    'PAYMENT RECORDS',
    'LOGS',
    'SYSTEM',
  ]);
  assert.equal(GOOGLE_DRIVE_CONTENT_FOLDER_NAMES.includes('TEMPORARY PROCESSING'), false);
});

test('payment ledgers and webhook records are routed to PAYMENT RECORDS', () => {
  for (const name of [
    'monzo_payments.json',
    'monzo_review.json',
    'stripe_balance_history.json',
    'stripe_balance_snapshots.json',
    'webhook_events.json',
  ]) assert.equal(googleDriveFolderForDatabaseFile(name), 'PAYMENT RECORDS');
});

test('notification and download history are routed to LOGS', () => {
  assert.equal(googleDriveFolderForDatabaseFile('notifications.json'), 'LOGS');
  assert.equal(googleDriveFolderForDatabaseFile('download_history.json'), 'LOGS');
});

test('other application records remain in DATABASE', () => {
  assert.equal(googleDriveFolderForDatabaseFile('vault.json'), 'DATABASE');
  assert.equal(googleDriveFolderForDatabaseFile('deliveries.json'), 'DATABASE');
});
