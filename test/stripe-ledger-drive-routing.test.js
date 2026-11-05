'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {googleDriveFolderForDatabaseFile}=require('../systems/drive-layout');
const {destinationFor}=require('../systems/drive-object-store');
test('Stripe payment ledger remains in BANKROLL/PAYMENT RECORDS',()=>{
  assert.equal(googleDriveFolderForDatabaseFile('stripe_ledger.json'),'PAYMENT RECORDS');
});
test('Stripe ledger is never sent through non-payment object APIs',()=>{
  assert.throws(()=>destinationFor('stripe_ledger.json'),/Payment files/);
});
