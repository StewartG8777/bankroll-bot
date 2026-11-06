'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {auditRecoveredAccounting}=require('../systems/primary-accounting-audit');
test('recovered payment totals count Stripe once and deduplicate Monzo transaction IDs',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-accounting-'));
 try{
  const write=(name,obj)=>fs.writeFile(path.join(dir,name),JSON.stringify(obj));
  await write('stripe_ledger.json',{payments:{pi_1:{id:'pi_1',amount:1234,currency:'gbp'}}});
  await write('monzo_payments.json',{invoice:{status:'paid',txId:'tx_1',amount:12,currency:'GBP'},monzo_import_tx_1:{status:'paid',txId:'tx_1',amount:12,currency:'GBP',source:'historical transaction'}});
  await write('deliveries.json',{order1:{}});
  await write('vault.json',{customer1:{}});
  let audit=await auditRecoveredAccounting({dataDir:dir});
  assert.equal(audit.passed,true);
  assert.equal(audit.totals.stripeMinorUnitsByCurrency.gbp,1234);
  assert.equal(audit.totals.monzoPaidTransactions,1);
  assert.equal(audit.totals.monzoMajorUnitsByCurrency.gbp,12);
  await write('stripe_ledger.json',{payments:{pi_1:{id:'pi_1',amount:-1,currency:'gbp'}}});
  audit=await auditRecoveredAccounting({dataDir:dir});
  assert.equal(audit.passed,false);
 }finally{await fs.rm(dir,{recursive:true,force:true});}
});
