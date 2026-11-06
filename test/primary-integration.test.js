'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs/promises');
const os=require('node:os');
const path=require('node:path');
const {Readable}=require('node:stream');
const {createGoogleDrivePrimaryStorage}=require('../systems/google-drive-primary-storage');
const {preparePrimaryData}=require('../systems/primary-startup');
const {createPrimaryCommitManager}=require('../systems/primary-commit-manager');
const {createSerializedStorageQueue}=require('../systems/serialized-storage-queue');
const {protectInteractionDurability,primaryWebhookGate}=require('../systems/primary-ack-guards');
const FOLDERS=Object.fromEntries(['DATABASE','VAULT','BACKUPS','CUSTOMER DELIVERIES','PAYMENT RECORDS','LOGS'].map(x=>[x,x]));
function fakeDrive(){
 const objects=new Map();let next=0;
 const files={
  create:async({requestBody,media})=>{const chunks=[];for await(const c of media.body)chunks.push(Buffer.from(c));const id='object-'+(++next);objects.set(id,{id,name:requestBody.name,parents:requestBody.parents,bytes:Buffer.concat(chunks)});return {data:{id}};},
  list:async({q})=>{const match=q.match(/name = '([^']+)'/);const contains=q.match(/name contains '([^']+)'/);const parent=q.match(/'([^']+)' in parents/);return {data:{files:[...objects.values()].filter(x=>(!match||x.name===match[1])&&(!contains||x.name.includes(contains[1]))&&(!parent||x.parents.includes(parent[1]))).map(x=>({id:x.id,name:x.name,createdTime:new Date().toISOString()}))}};},
  get:async({fileId})=>{if(!objects.has(fileId))throw Error('Missing object '+fileId);return {data:Readable.from([objects.get(fileId).bytes])};},
  update:async({fileId,media})=>{const chunks=[];for await(const c of media.body)chunks.push(Buffer.from(c));objects.get(fileId).bytes=Buffer.concat(chunks);return {data:{id:fileId}};}
 };
 return {google:{drive:()=>({files})},objects};
}
test('isolated Drive snapshots restore cold startup, reject corruption, and verify rollback without changing current pointer',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-primary-test-'));
 try{
  const {google,objects}=fakeDrive();
  const store=createGoogleDrivePrimaryStorage({google,auth:{},dataDir:root,folderIds:FOLDERS,logger:{info(){},warn(){}}});
  await fs.writeFile(path.join(root,'ledger.json'),'{"balance":1}');
  const first=await store.snapshot();
  await fs.writeFile(path.join(root,'ledger.json'),'{"balance":2}');
  const second=await store.snapshot();
  await fs.writeFile(path.join(root,'ledger.json'),'{"balance":999}');
  const restored=await preparePrimaryData({store,dataDir:root});
  assert.equal(restored.generation,second.generation);
  assert.equal(await fs.readFile(path.join(root,'ledger.json'),'utf8'),'{"balance":2}');
  const verified=await store.verifyGeneration(first.generation);
  assert.equal(verified.generation,first.generation);
  assert.equal((await store.currentManifest()).pointer.generation,second.generation);
  await store.restoreGeneration(first.generation);
  assert.equal(await fs.readFile(path.join(root,'ledger.json'),'utf8'),'{"balance":1}');
  assert.equal((await store.currentManifest()).pointer.generation,second.generation);
  const current=(await store.currentManifest()).manifest.files[0];
  objects.get(current.fileId).bytes=Buffer.from('corrupted');
  await assert.rejects(store.restore(),/checksum|size/i);
  assert.equal(await fs.readFile(path.join(root,'ledger.json'),'utf8'),'{"balance":1}');
 }finally{await fs.rm(root,{recursive:true,force:true});}
});
test('serialized writes preserve order, maintenance blocks new writes, and failed commits refuse durability',async()=>{
 const queue=createSerializedStorageQueue(),order=[];
 await Promise.all([1,2,3].map(n=>queue.enqueue(async()=>{order.push(n);await Promise.resolve();})));
 assert.deepEqual(order,[1,2,3]);
 let release;
 const maintenance=queue.withMaintenance(()=>new Promise(resolve=>{release=resolve;}));
 await Promise.resolve();await assert.rejects(queue.enqueue(()=>{}),/maintenance/i);
 release();await maintenance;
 let fail=true;
 const manager=createPrimaryCommitManager({store:{snapshot:async()=>{if(fail)throw Error('Drive unavailable');}},onError:()=>{}});
 manager.activate();manager.markDirty();
 await assert.rejects(manager.flush(),/Drive unavailable/);
 assert.ok(manager.status().pending>0);
 fail=false;await manager.flush();assert.equal(manager.status().pending,0);
});
test('Discord and payment webhook acknowledgements fail closed on Drive errors',async()=>{
 const manager={flush:async()=>{throw Error('Drive unavailable')},status:()=>({active:true})};
 const sent=[];
 const interaction={reply:async x=>sent.push(x)};
 protectInteractionDurability(interaction,manager,{error(){}});
 await interaction.reply({content:'Payment successful'});
 assert.doesNotMatch(sent[0].content,/Payment successful/);
 const res={statusCode:200,headersSent:false,status(n){this.statusCode=n;return this;},removeHeader(){},send(x){this.body=x;return this;}};
 const middleware=primaryWebhookGate(manager,{error(){}});
 middleware({method:'POST',path:'/monzo-webhook'},res,()=>res.send('OK'));
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(res.statusCode,503);
 assert.match(res.body,/commit failed/i);
});
