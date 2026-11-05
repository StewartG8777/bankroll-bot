'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),os=require('node:os');
const path=require('node:path'),fs=require('node:fs/promises');
const {preparePrimaryData}=require('../systems/primary-startup');
async function dir(t) {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-primary-start-'));
  t.after(()=>fs.rm(root,{recursive:true,force:true}));
  return root;
}
test('refuses missing Drive current pointer by default, never seeds empty state',async(t)=>{
  const dataDir=await dir(t);
  const store={currentManifest:async()=>{throw Error('No current primary snapshot pointer exists');},snapshot:async()=>{throw Error('must not seed');}};
  await assert.rejects(preparePrimaryData({store,dataDir}),/blocked/);
  await assert.rejects(preparePrimaryData({store,dataDir,allowSeed:true}),/empty or unverified/);
});
test('valid current generation restores before stale local files are quarantined',async(t)=>{
  const dataDir=await dir(t);let applied=false;
  await fs.writeFile(path.join(dataDir,'old.json'),'stale');
  const store={
    currentManifest:async()=>({manifest:{files:[{path:'orders.json'}]}}),
    restore:async()=>{
      applied=true; await fs.writeFile(path.join(dataDir,'orders.json'),'{"x":1}');
      return {generation:'verified',files:1};
    }
  };
  const result=await preparePrimaryData({store,dataDir,logger:{warn(){}}});
  assert.equal(applied,true);assert.equal(result.generation,'verified');
  assert.deepEqual(result.quarantined,['old.json']);
  await assert.rejects(fs.access(path.join(dataDir,'old.json')));
  assert.equal(await fs.readFile(path.join(dataDir,'orders.json'),'utf8'),'{"x":1}');
});
test('integrity failure prevents startup and does not quarantine data',async(t)=>{
  const dataDir=await dir(t);
  await fs.writeFile(path.join(dataDir,'vault.json'),'current');
  const store={
    currentManifest:async()=>({manifest:{files:[{path:'vault.json'}]}}),
    restore:async()=>{throw Error('Snapshot checksum mismatch');}
  };
  await assert.rejects(preparePrimaryData({store,dataDir}),/checksum/);
  assert.equal(await fs.readFile(path.join(dataDir,'vault.json'),'utf8'),'current');
});
test('one-time seed requires existing ledger and ends in verified restore',async(t)=>{
  const dataDir=await dir(t);await fs.writeFile(path.join(dataDir,'vault.json'),'{"vault":1}');
  let ready=false,committed=false;
  const store={
    currentManifest:async()=>{if(!ready)throw Error('No current primary snapshot pointer exists');return {manifest:{files:[{path:'vault.json'}]}};},
    snapshot:async()=>{ready=true;committed=true;},
    restore:async()=>{assert.ok(committed);return {generation:'seed',files:1};}
  };
  const result=await preparePrimaryData({store,dataDir,allowSeed:true});
  assert.equal(result.seeded,true);assert.equal(result.generation,'seed');
});
