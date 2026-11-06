'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const crypto=require('node:crypto');
const {auditRecoveredLedgers,REQUIRED}=require('../systems/primary-ledger-audit');
test('all six recovered ledgers must match Drive manifest bytes, not merely valid JSON',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-ledger-audit-'));
 try{
  const files=[];
  for(const name of REQUIRED){
   const data=Buffer.from('{}');
   await fs.writeFile(path.join(dir,name),data);
   files.push({path:name,size:data.length,sha256:crypto.createHash('sha256').update(data).digest('hex')});
  }
  const store={currentManifest:async()=>({manifest:{files},pointer:{generation:'test'}})};
  let result=await auditRecoveredLedgers({store,dataDir:dir});
  assert.equal(result.passed,6);assert.equal(result.failed,0);assert.equal(result.accountingReconciled,false);
  await fs.writeFile(path.join(dir,'stripe_ledger.json'),'{"valid":"but changed"}');
  result=await auditRecoveredLedgers({store,dataDir:dir});
  assert.equal(result.failed,1);
  assert.equal(result.results.find(x=>x.name==='stripe_ledger.json').status,'FAIL');
  await fs.rm(path.join(dir,'monzo_payments.json'));
  result=await auditRecoveredLedgers({store,dataDir:dir});
  assert.equal(result.failed,2);
 }finally{await fs.rm(dir,{recursive:true,force:true});}
});
