'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
test('both independent backup sets include Stripe history ledger',()=>{
  const source=fs.readFileSync(path.join(__dirname,'../index.js'),'utf8');
  const target="'stripe_ledger.json', 'stripe_balance_history.json', 'stripe_balance_snapshots.json'";
  assert.ok(source.split(target).length>=3,'Stripe ledger must be in both Drive and Discord backup sets');
});
