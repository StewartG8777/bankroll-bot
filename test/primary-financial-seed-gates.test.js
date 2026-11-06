'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {auditSeedFiles,preparePrimaryData,SEED_REQUIRED}=require('../systems/primary-startup');
test('primary seed refuses missing financial ledger and invalid JSON',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-seed-'));
 try{
  for(const file of SEED_REQUIRED)await fs.writeFile(path.join(dir,file),'{}');
  await auditSeedFiles(dir);
  await fs.rm(path.join(dir,'monzo_payments.json'));
  await assert.rejects(auditSeedFiles(dir),/monzo_payments.json/);
  await fs.writeFile(path.join(dir,'monzo_payments.json'),'not json');
  await assert.rejects(auditSeedFiles(dir),/invalid JSON.*monzo_payments.json/);
 }finally{await fs.rm(dir,{recursive:true,force:true});}
});
test('primary startup refuses missing Drive pointer unless explicitly allowed to seed',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-startup-'));
 let seeded=false;
 const store={
  currentManifest:async()=>{throw Error('No current primary snapshot pointer exists');},
  snapshot:async()=>{seeded=true;}
 };
 try{
  await assert.rejects(preparePrimaryData({store,dataDir:dir,allowSeed:false}),/blocked/);
  assert.equal(seeded,false);
 }finally{await fs.rm(dir,{recursive:true,force:true});}
});
