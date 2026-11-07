'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {verifyCustomerPayments}=require('../systems/primary-customer-provider-verification');
const stripe=(id,customer,amount)=>({id,status:'succeeded',amount_received:amount,currency:'gbp',metadata:{agent_id:'a',customer_id:customer}});
test('detects independent customer attribution mismatch despite equal aggregate Stripe amount',()=>{
 const r=verifyCustomerPayments({stripeSaved:{pi:{id:'pi',customerId:'alice',amount:5000,currency:'gbp'}},
 stripeProvider:[stripe('pi','bob',5000)],stripeHistoryComplete:true});
 assert.equal(r.status,'ISSUES');
 assert.equal(r.issues.filter(x=>x.type==='customer-stripe-total-mismatch').length,2);
});
test('valid Stripe customer gross totals are independently confirmed',()=>{
 const r=verifyCustomerPayments({stripeSaved:{pi:{id:'pi',customerId:'alice',amount:5000,currency:'gbp'}},
 stripeProvider:[stripe('pi','alice',5000)],stripeHistoryComplete:true});
 assert.equal(r.issues.length,0);
 assert.equal(r.totals.alice.gbp.providerStripeGrossMinor,5000);
 assert.equal(r.customerBalanceVerified,false);
});
test('Monzo transaction amount is verified but customer attribution is not assumed',()=>{
 const r=verifyCustomerPayments({monzoSaved:{one:{status:'paid',txId:'tx',customerId:'alice',amount:50,currency:'gbp'}},
 monzoProvider:[{id:'tx',amount:5000,currency:'GBP'}]});
 assert.equal(r.issues.length,0);
 assert.equal(r.totals.alice.gbp.verifiedMonzoGrossMinor,5000);
 assert.equal(r.customerIdentityIndependentlyVerified.monzo,false);
});
test('missing Monzo provider transaction remains unverified',()=>{
 const r=verifyCustomerPayments({monzoSaved:{one:{status:'paid',txId:'tx',customerId:'alice',amount:50,currency:'gbp'}}});
 assert.equal(r.issues[0].type,'unverified-monzo-transaction');
});

test('outgoing Monzo transfer cannot verify a paid invoice',()=>{
 const {reconcileMonzoRecords}=require('../systems/primary-provider-reconciliation');
 const result=reconcileMonzoRecords({invoice:{status:'paid',txId:'tx',amount:50,currency:'gbp'}},[{id:'tx',amount:-5000,currency:'GBP'}]);
 assert.ok(result.issues.some(issue=>issue.type==='amount-mismatch'));
});
