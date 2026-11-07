'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {verifyProviders}=require('../systems/primary-provider-verifier');

async function withRecoveredLedgers(run){
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-provider-'));
 try{
  await fs.writeFile(path.join(dir,'stripe_ledger.json'),JSON.stringify({
   payments:{pi_1:{id:'pi_1',amount:100,currency:'gbp',customerId:'customer'}}
  }));
  await fs.writeFile(path.join(dir,'monzo_payments.json'),'{}');
  return await run(dir);
 }finally{await fs.rm(dir,{recursive:true,force:true});}
}
const payment=(amount=100)=>({
 id:'pi_1',status:'succeeded',amount_received:amount,currency:'gbp',
 metadata:{agent_id:'agent',customer_id:'customer'}
});
const stripePage=(data,has_more=false)=>({
 paymentIntents:{list:async()=>({data,has_more})}
});

test('complete Stripe history with unavailable Monzo remains incomplete overall',async()=>{
 await withRecoveredLedgers(async dataDir=>{
  const result=await verifyProviders({dataDir,stripe:stripePage([payment()])});
  assert.equal(result.results.stripe.status,'VERIFIED');
  assert.equal(result.results.monzo.status,'INCOMPLETE');
  assert.equal(result.fullyVerified,false);
  assert.equal(result.confirmedMismatch,false);
 });
});

test('confirmed Stripe amount mismatch is detected during paginated verification',async()=>{
 await withRecoveredLedgers(async dataDir=>{
  const result=await verifyProviders({dataDir,stripe:stripePage([payment(200)])});
  assert.equal(result.results.stripe.status,'MISMATCH');
  assert.equal(result.confirmedMismatch,true);
  assert.deepEqual(result.results.stripe.issues,[{type:'amount-mismatch',id:'pi_1'}]);
 });
});

test('Stripe outage remains INCOMPLETE and is not a confirmed mismatch',async()=>{
 await withRecoveredLedgers(async dataDir=>{
  const stripe={paymentIntents:{list:async()=>{throw Error('offline');}}};
  const result=await verifyProviders({dataDir,stripe});
  assert.equal(result.results.stripe.status,'INCOMPLETE');
  assert.equal(result.confirmedMismatch,false);
  assert.equal(result.fullyVerified,false);
 });
});

test('nonterminal Stripe scan error cannot be reported as fully verified',async()=>{
 await withRecoveredLedgers(async dataDir=>{
  const stripe=stripePage([payment()],true);
  const result=await verifyProviders({dataDir,stripe});
  assert.equal(result.results.stripe.status,'INCOMPLETE');
  assert.equal(result.fullyVerified,false);
  assert.equal(result.confirmedMismatch,false);
 });
});
