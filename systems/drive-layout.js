'use strict';

const GOOGLE_DRIVE_ROOT_NAME = 'BANKROLL';
const GOOGLE_DRIVE_FOLDER_NAMES = Object.freeze([
  'VAULT',
  'CUSTOMER DELIVERIES',
  'DATABASE',
  'BACKUPS',
  'PAYMENT RECORDS',
  'LOGS',
  'SYSTEM',
  'TEMPORARY PROCESSING',
]);
const GOOGLE_DRIVE_CONTENT_FOLDER_NAMES = Object.freeze([
  'VAULT',
  'CUSTOMER DELIVERIES',
  'DATABASE',
  'PAYMENT RECORDS',
  'LOGS',
  'SYSTEM',
]);
const PAYMENT_RECORD_FILES = new Set([
  'monzo_payments.json',
  'monzo_review.json',
  'stripe_balance_history.json',
  'stripe_balance_snapshots.json',
  'webhook_events.json',
]);
const LOG_FILES = new Set([
  'notifications.json',
  'download_history.json',
]);

function googleDriveFolderForDatabaseFile(name) {
  const normalized = String(name || '').trim().toLowerCase();
  if (PAYMENT_RECORD_FILES.has(normalized)) return 'PAYMENT RECORDS';
  if (LOG_FILES.has(normalized)) return 'LOGS';
  return 'DATABASE';
}

module.exports = {
  GOOGLE_DRIVE_ROOT_NAME,
  GOOGLE_DRIVE_FOLDER_NAMES,
  GOOGLE_DRIVE_CONTENT_FOLDER_NAMES,
  googleDriveFolderForDatabaseFile,
};
