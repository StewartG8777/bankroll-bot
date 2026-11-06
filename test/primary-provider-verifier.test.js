'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {verifyProviders}=require('../systems/primary-provider-verifier');
test('partial provider verification is not incorrectly reported as fully verified',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-provider-'));
 try{
  await fs.writeFile(path.join(dir,'stripe_ledger.json'),JSON.stringify({payments:{pi_1:{id:'pi_1',amount:100,currency:'gbp'}}}));
  await fs.writeFile(path.join(dir,'monzo_payments.json'),'{}');
  const stripe={paymentIntents:{retrieve:async id=>({id,status:'succeeded',amount_received:100,currency:'gbp'})}};
  const result=await verifyProviders({dataDir:dir,stripe});
  assert.equal(result.results.stripe.status,'PARTIAL');
  assert.equal(result.results.monzo.status,'INCOMPLETE');
  assert.equal(result.fullyVerified,false);
  const mismatch=await verifyProviders({dataDir:dir,stripe:{paymentIntents:{retrieve:async id=>({id,status:'succeeded',amount_received:200,currency:'gbp'})}}});
  assert.equal(mismatch.confirmedMismatch,true);
  const outage=await verifyProviders({dataDir:dir,stripe:{paymentIntents:{retrieve:async()=>{throw Error('offline');}}}});
  assert.equal(outage.results.stripe.status,'INCOMPLETE');
  assert.equal(outage.confirmedMismatch,false);
 }finally{await fs.rm(dir,{recursive:true,force:true});}
});
