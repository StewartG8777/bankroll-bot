'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {scanStripeHistory}=require('../systems/primary-stripe-history');
const pi=(id)=>({id,status:'succeeded',amount_received:100,currency:'gbp',metadata:{agent_id:'agent',customer_id:'customer'}});
test('scans all Stripe pages and finds payments absent from local ledger',async()=>{
 const calls=[];
 const stripe={paymentIntents:{list:async args=>{
  calls.push(args);
  return calls.length===1?{data:[pi('pi_a')],has_more:true}:{data:[pi('pi_b')],has_more:false};
 }}};
 const saved={pi_a:{amount:100,currency:'gbp',customerId:'customer'}};
 const result=await scanStripeHistory({stripe,saved});
 assert.equal(result.status,'MISMATCH');
 assert.deepEqual(result.issues,[{type:'missing-local-payment',id:'pi_b'}]);
 assert.equal(calls[1].starting_after,'pi_a');
});
test('failed Stripe pagination is not classified as a confirmed mismatch',async()=>{
 const stripe={paymentIntents:{list:async()=>{throw Error('provider unavailable');}}};
 await assert.rejects(scanStripeHistory({stripe,saved:{}}),/provider unavailable/);
});
test('ignores payments outside existing bot agent-scoped ledger',async()=>{
 const stripe={paymentIntents:{list:async()=>({data:[{...pi('pi_other'),metadata:{}}],has_more:false})}};
 const result=await scanStripeHistory({stripe,saved:{}});
 assert.equal(result.status,'VERIFIED');
});
