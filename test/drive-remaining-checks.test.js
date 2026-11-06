'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {runRemainingChecks}=require('../systems/drive-remaining-checks');
test('missing ledgers fail and all four unsafe checks remain NOT RUN',async()=>{
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-gaps-'));
 try{
  const result=await runRemainingChecks({dataDir:dir});
  assert.equal(result.failures,1);
  assert.equal(result.notRun,4);
 }finally{await fs.rm(dir,{recursive:true,force:true});}
});
