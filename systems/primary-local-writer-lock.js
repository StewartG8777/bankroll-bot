'use strict';
// Local process exclusion for a deliberately single-instance deployment.
// This is NOT a distributed lease. Separate machines/disks are not protected.
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
function acquireLocalWriterLock(dataDir){
 const lockPath=path.join(path.resolve(dataDir),'.drive-primary-writer.lock');
 const token=crypto.randomUUID();
 let fd;
 try{
  fd=fs.openSync(lockPath,'wx',0o600);
  fs.writeFileSync(fd,JSON.stringify({pid:process.pid,token,startedAt:new Date().toISOString()}));
  fs.fsyncSync(fd);
 }catch(error){
  if(fd!==undefined)try{fs.closeSync(fd);}catch{}
  if(error.code==='EEXIST')throw Error('Primary writer lock exists. Refusing second writer. Do not delete until the previous writer is confirmed stopped.');
  throw error;
 }
 let released=false;
 return {
  path:lockPath,
  release(){
   if(released)return;
   released=true;
   try{
    // Never delete another process's replacement lock.
    const saved=JSON.parse(fs.readFileSync(lockPath,'utf8'));
    if(saved.token===token)fs.unlinkSync(lockPath);
   }catch(error){if(error.code!=='ENOENT')throw error;}
   finally{fs.closeSync(fd);}
  }
 };
}
module.exports={acquireLocalWriterLock};
