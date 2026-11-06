'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {Readable}=require('node:stream');
const {runDriveReadinessTest}=require('../systems/drive-readiness-test');
test('isolated readiness uploads two versions, restores and deletes both, without changing financial data',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-readiness-'));
 const files=['vault.json','deliveries.json','downloads.json','monzo_payments.json','stripe_ledger.json','webhook_events.json'];
 const ids=Object.fromEntries(['DATABASE','VAULT','BACKUPS','CUSTOMER DELIVERIES','PAYMENT RECORDS','LOGS'].map(x=>[x,x]));
 const objects=new Map(),deleted=[];
 try{
  for(const name of files)await fs.writeFile(path.join(root,name),'{}');
  const drive={
   about:{get:async()=>({data:{user:{emailAddress:'owner@example.invalid'}}})},
   files:{
    create:async({media})=>{const chunks=[];for await(const chunk of media.body)chunks.push(Buffer.from(chunk));const id='temp-'+(objects.size+1);objects.set(id,Buffer.concat(chunks));return {data:{id}};},
    get:async({fileId,alt})=>alt==='media'?{data:Readable.from([objects.get(fileId)])}:{data:{mimeType:'application/vnd.google-apps.folder',trashed:false}},
    delete:async({fileId})=>{deleted.push(fileId);objects.delete(fileId);}
   }
  };
  const result=await runDriveReadinessTest({drive,folderIds:ids,dataDir:root});
  assert.equal(result.failures,0);
  assert.equal(deleted.length,2);
  assert.equal(objects.size,0);
  assert.match(result.report,/cold recovery and integrity.*SHA-256 verified/);
  assert.match(result.report,/Stripe\/Monzo replay and idempotency.*NOT RUN|NOT RUN.*Stripe\/Monzo replay and idempotency/);
  assert.match(result.report,/NOT CERTIFIED/);
  for(const name of files)assert.equal(await fs.readFile(path.join(root,name),'utf8'),'{}');
 }finally{await fs.rm(root,{recursive:true,force:true});}
});
test('readiness reports missing ledgers without changing them',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-readiness-'));
 try{
  const result=await runDriveReadinessTest({drive:{about:{get:async()=>{throw Error('offline')}},files:{get:async()=>{throw Error('offline')},delete:async()=>{}}},folderIds:{},dataDir:root});
  assert.ok(result.failures>0);
  assert.match(result.report,/Missing:/);
 }finally{await fs.rm(root,{recursive:true,force:true});}
});
