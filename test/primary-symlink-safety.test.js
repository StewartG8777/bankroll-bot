'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
const {walkFiles,isAllowedRelativePath}=require('../systems/google-drive-primary-storage');
test('snapshot skips symlinked folders and excludes recovery cache/lock files',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-symlink-'));
  t.after(()=>fs.rm(root,{recursive:true,force:true}));
  await fs.mkdir(path.join(root,'vault'),{recursive:true});
  await fs.writeFile(path.join(root,'vault','car.txt'),'car');
  await fs.symlink(os.tmpdir(),path.join(root,'external'));
  const files=await walkFiles(root);
  assert.deepEqual(files.map(x=>x.relative),['vault/car.txt']);
  for(const rel of ['.drive-primary-writer.lock','.drive-cache-quarantine-x/old.json','.drive-restore-x/file.json']){
    assert.equal(isAllowedRelativePath(rel),false,rel);
  }
});
