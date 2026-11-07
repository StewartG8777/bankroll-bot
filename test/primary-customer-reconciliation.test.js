'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {customerPaymentTotals}=require('../systems/primary-customer-reconciliation');
test('reconciles per-customer Stripe refunds and deduplicated Monzo paid records',()=>{
 const result=customerPaymentTotals({
  stripePayments:{pi_1:{id:'pi_1',customerId:'c1',amount:10000,refunded:2000,currency:'gbp'}},
  monzoPayments:{
   invoice:{status:'paid',txId:'tx_1',customerId:'c1',amount:50,currency:'gbp'},
   monzo_import_tx_1:{status:'paid',txId:'tx_1',customerId:'c1',amount:50,currency:'gbp',source:'historical transaction'}
  }
 });
 assert.equal(result.complete,true);
 assert.deepEqual(result.totals.c1.gbp,{
  stripeGrossMinor:10000,stripeRefundedMinor:2000,monzoGrossMinor:5000,netReceivedMinor:13000
 });
});
test('unattributed payments do not invent customer balances',()=>{
 const result=customerPaymentTotals({stripePayments:{pi:{id:'pi',amount:100,currency:'gbp'}}});
 assert.equal(result.complete,false);
 assert.deepEqual(result.totals,{});
 assert.equal(result.issues[0].type,'unattributed-payment');
});

test('boolean Stripe refund flags cannot be misreported as a 1p refund',()=>{
 const result=customerPaymentTotals({stripePayments:{pi:{id:'pi',customerId:'alice',amount:10000,refunded:true,currency:'gbp'}}});
 assert.equal(result.complete,false);
 assert.deepEqual(result.issues,[{type:'invalid-stripe-refund',id:'pi'}]);
 assert.deepEqual(result.totals,{});
});
