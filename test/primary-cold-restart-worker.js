'use strict';
// A fresh Node process with an EMPTY cache restores a Drive generation from
// persisted fake Drive objects. No live Drive credentials or payments required.
const fs=require('node:fs/promises');
const path=require('node:path');
const {Readable}=require('node:stream');
const {createGoogleDrivePrimaryStorage}=require('../systems/google-drive-primary-storage');
const {preparePrimaryData}=require('../systems/primary-startup');
const {auditRecoveredLedgers}=require('../systems/primary-ledger-audit');
const {REQUIRED}=require('../systems/primary-ledger-audit');
const objectsDir=process.env.TEST_DRIVE_OBJECTS;
const cache=process.env.TEST_EMPTY_CACHE;
const FOLDERS=Object.fromEntries(['DATABASE','VAULT','BACKUPS','CUSTOMER DELIVERIES','PAYMENT RECORDS','LOGS'].map(x=>[x,x]));
async function all(){return (await fs.readdir(objectsDir)).filter(x=>x.endsWith('.json'));}
function drive(){
 return {files:{
  create:async({requestBody,media})=>{
   const chunks=[];for await(const c of media.body)chunks.push(Buffer.from(c));
   const id='obj-'+require('node:crypto').randomUUID();
   await fs.writeFile(path.join(objectsDir,id+'.bin'),Buffer.concat(chunks));
   await fs.writeFile(path.join(objectsDir,id+'.json'),JSON.stringify({id,name:requestBody.name,parents:requestBody.parents,createdTime:new Date().toISOString()}));
   return {data:{id}};
  },
  list:async({q})=>{
   const name=q.match(/name = '([^']+)'/),contains=q.match(/name contains '([^']+)'/),parent=q.match(/'([^']+)' in parents/);
   const records=await Promise.all((await all()).map(async x=>JSON.parse(await fs.readFile(path.join(objectsDir,x),'utf8'))));
   return {data:{files:records.filter(x=>(!name||x.name===name[1])&&(!contains||x.name.includes(contains[1]))&&(!parent||x.parents.includes(parent[1])))}};
  },
  get:async({fileId})=>({data:Readable.from([await fs.readFile(path.join(objectsDir,fileId+'.bin'))])}),
  update:async({fileId,media})=>{
   const chunks=[];for await(const c of media.body)chunks.push(Buffer.from(c));
   await fs.writeFile(path.join(objectsDir,fileId+'.bin'),Buffer.concat(chunks));return {data:{id:fileId}};
  }
 }};
}
(async()=>{
 if(!objectsDir||!cache){
  // node --test discovers helper files under test/; only execute in child process.
  if(process.argv[2]==='seed'||process.argv[2]==='restore')throw Error('Test Drive fixture environment is missing');
  return;
 }
 const store=createGoogleDrivePrimaryStorage({google:{drive},auth:{},dataDir:cache,folderIds:FOLDERS,logger:{info(){},warn(){}}});
 if(process.argv[2]==='seed'){
  await fs.mkdir(cache,{recursive:true});
  for(const name of REQUIRED)await fs.writeFile(path.join(cache,name),JSON.stringify({name,marker:'authoritative'}));
  await fs.mkdir(path.join(cache,'vault'),{recursive:true});
  await fs.writeFile(path.join(cache,'vault','sample.bin'),Buffer.from('verified vault payload'));
  await store.snapshot({allowInitialize:true});
 }else{
  if((await fs.readdir(cache)).length)throw Error('Cold restart cache was not empty');
  const restored=await preparePrimaryData({store,dataDir:cache});
  const audited=await auditRecoveredLedgers({store,dataDir:cache});
  if(audited.failed||audited.passed!==6)throw Error('Ledger restore verification failed');
  const payload=await fs.readFile(path.join(cache,'vault','sample.bin'),'utf8');
  if(payload!=='verified vault payload')throw Error('Vault payload mismatch');
  if(restored.restored<7)throw Error('Not all files restored');
  console.log(JSON.stringify({generation:restored.generation,restored:restored.restored,verified:audited.passed}));
 }
})().catch(e=>{console.error(e);process.exitCode=1});
