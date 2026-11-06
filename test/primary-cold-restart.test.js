'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
test('fresh process restores six financial ledgers and vault bytes from Drive with empty cache',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-cold-drive-'));
 const objects=path.join(root,'drive'),seed=path.join(root,'seed'),empty=path.join(root,'empty');
 await fs.mkdir(objects);await fs.mkdir(empty);
 const worker=path.join(__dirname,'primary-cold-restart-worker.js');
 const run=(mode,cache)=>spawnSync(process.execPath,[worker,mode],{env:{...process.env,TEST_DRIVE_OBJECTS:objects,TEST_EMPTY_CACHE:cache},encoding:'utf8',timeout:30000});
 try{
  const first=run('seed',seed);
  assert.equal(first.status,0,first.stderr);
  const second=run('restore',empty);
  assert.equal(second.status,0,second.stderr);
  const result=JSON.parse(second.stdout.trim().split('\n').at(-1));
  assert.equal(result.verified,6);
  assert.ok(result.restored>=7);
 }finally{await fs.rm(root,{recursive:true,force:true});}
});
