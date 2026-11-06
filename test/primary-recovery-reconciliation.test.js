'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const crypto=require('node:crypto');
const {reconcileRecoveredData}=require('../systems/primary-recovery-reconciliation');
const {REQUIRED}=require('../systems/primary-ledger-audit');
test('recovery preserves every ledger and delivery object byte-for-byte',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-reconcile-'));
 try{
  const files=[];
  for(const name of [...REQUIRED,'files/customer-delivery.bin','vault/customer.bin']){
   const bytes=Buffer.from(name.endsWith('.json')?JSON.stringify({customer:{balance:12345,delivered:2}}):'customer-payload');
   const dest=path.join(dir,name);
   await fs.mkdir(path.dirname(dest),{recursive:true});
   await fs.writeFile(dest,bytes);
   files.push({path:name,size:bytes.length,sha256:crypto.createHash('sha256').update(bytes).digest('hex')});
  }
  const store={currentManifest:async()=>({manifest:{files},pointer:{generation:'verified'}})};
  let result=await reconcileRecoveredData({store,dataDir:dir});
  assert.equal(result.failed,0);
  assert.equal(result.passed,8);
  assert.equal(result.accountingBalancesReconciled,false);
  await fs.writeFile(path.join(dir,'deliveries.json'),'{"customer":{"delivered":3}}');
  result=await reconcileRecoveredData({store,dataDir:dir});
  assert.equal(result.failed,1);
  assert.equal(result.results.find(x=>x.name==='deliveries.json').status,'FAIL');
  await fs.rm(path.join(dir,'files/customer-delivery.bin'));
  result=await reconcileRecoveredData({store,dataDir:dir});
  assert.equal(result.failed,2);
 }finally{await fs.rm(dir,{recursive:true,force:true});}
});
