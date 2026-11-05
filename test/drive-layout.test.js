'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { GOOGLE_DRIVE_ROOT_NAME, GOOGLE_DRIVE_FOLDER_NAMES, GOOGLE_DRIVE_CONTENT_FOLDER_NAMES, googleDriveFolderForDatabaseFile } = require('../systems/drive-layout');
test('Drive root and eight folders', () => {assert.equal(GOOGLE_DRIVE_ROOT_NAME,'BANKROLL');assert.equal(GOOGLE_DRIVE_FOLDER_NAMES.length,8);assert.equal(new Set(GOOGLE_DRIVE_FOLDER_NAMES).size,8);for(const name of ['VAULT','CUSTOMER DELIVERIES','DATABASE','BACKUPS','PAYMENT RECORDS','LOGS','SYSTEM','TEMPORARY PROCESSING'])assert.ok(GOOGLE_DRIVE_FOLDER_NAMES.includes(name));});
test('content folders are valid',()=>{for(const name of GOOGLE_DRIVE_CONTENT_FOLDER_NAMES)assert.ok(GOOGLE_DRIVE_FOLDER_NAMES.includes(name));});
test('payment records route correctly',()=>{for(const name of ['monzo_payments.json','monzo_review.json','stripe_ledger.json','stripe_balance_history.json','stripe_balance_snapshots.json','webhook_events.json'])assert.equal(googleDriveFolderForDatabaseFile(name),'PAYMENT RECORDS');});
test('logs route correctly',()=>{for(const name of ['notifications.json','download_history.json'])assert.equal(googleDriveFolderForDatabaseFile(name),'LOGS');});
test('other records route to database',()=>{assert.equal(googleDriveFolderForDatabaseFile('orders.json'),'DATABASE');assert.equal(googleDriveFolderForDatabaseFile('customers.json'),'DATABASE');});
