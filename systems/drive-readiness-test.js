'use strict';
// Read-only production inspection plus isolated disposable Drive recovery exercise.
// Never reads payment amounts into reports or touches authoritative generations.
const crypto=require('node:crypto');
const fs=require('node:fs/promises');
const path=require('node:path');
const {Readable}=require('node:stream');
const {AttachmentBuilder}=require('discord.js');
const REQUIRED=['vault.json','deliveries.json','downloads.json','monzo_payments.json','stripe_ledger.json','webhook_events.json'];
const sha=buffer=>crypto.createHash('sha256').update(buffer).digest('hex');
async function readMedia(drive,id){
 const response=await drive.files.get({fileId:id,alt:'media'},{responseType:'stream'});
 const chunks=[];let length=0;
 for await(const chunk of response.data){
  length+=chunk.length;
  if(length>65536)throw Error('Unexpected test object size');
  chunks.push(Buffer.from(chunk));
 }
 return Buffer.concat(chunks);
}
async function runDriveReadinessTest({drive,folderIds,dataDir,mode='LEGACY'}){
 const checks=[];
 const record=async(name,fn)=>{
  try{checks.push({name,status:'PASS',detail:String(await fn())});}
  catch(error){checks.push({name,status:'FAIL',detail:String(error.message||error).slice(0,180)});}
 };
 await record('Google Drive authentication',async()=>{
  const response=await drive.about.get({fields:'user(emailAddress)'});
  if(!response.data?.user?.emailAddress)throw Error('No connected Google account');
  return 'Drive account authenticated';
 });
 const requiredFolders=['DATABASE','VAULT','BACKUPS','CUSTOMER DELIVERIES','PAYMENT RECORDS','LOGS'];
 await record('Required Drive folders',async()=>{
  for(const name of requiredFolders){
   if(!folderIds[name])throw Error('Missing '+name);
   const result=await drive.files.get({fileId:folderIds[name],fields:'id,mimeType,trashed'});
   if(result.data?.trashed||result.data?.mimeType!=='application/vnd.google-apps.folder')throw Error('Invalid '+name);
  }
  return 'All six folders accessible';
 });
 const created=[];
 let recoveryPassed=false;
 try{
  await record('Isolated Drive cold recovery and integrity',async()=>{
   if(!folderIds.BACKUPS)throw Error('BACKUPS folder unavailable');
   const nonce=crypto.randomUUID();
   const make=async(version)=>{
    const payload=Buffer.from(JSON.stringify({test:'bankroll-readiness',nonce,version}));
    const result=await drive.files.create({
     requestBody:{name:'.bankroll-readiness-'+nonce+'-'+version+'.json',parents:[folderIds.BACKUPS],description:'Disposable readiness test object'},
     media:{mimeType:'application/json',body:Readable.from([payload])},fields:'id'
    });
    const id=result.data?.id;
    if(!id)throw Error('Drive did not return object ID');
    created.push(id);
    return {id,payload};
   };
   const old=await make(1),latest=await make(2);
   const recovered=await readMedia(drive,latest.id);
   if(recovered.length!==latest.payload.length||sha(recovered)!==sha(latest.payload))throw Error('Latest snapshot integrity mismatch');
   if(JSON.parse(recovered.toString('utf8')).version!==2)throw Error('Latest version not recovered');
   recoveryPassed=true;
   return 'Latest isolated test version recovered; SHA-256 verified';
  });
  await record('Isolated rollback generation verification',async()=>{
   if(!recoveryPassed||created.length<2)throw Error('Recovery prerequisite failed');
   const old=await readMedia(drive,created[0]);
   const current=await readMedia(drive,created[1]);
   const older=JSON.parse(old.toString('utf8')),newer=JSON.parse(current.toString('utf8'));
   if(older.version!==1||newer.version!==2||older.nonce!==newer.nonce)throw Error('Version mismatch');
   return 'Older test version verified; current version unchanged';
  });
 }finally{
  await record('Disposable Drive object cleanup',async()=>{
   let failed=0;
   for(const id of created){try{await drive.files.delete({fileId:id});}catch{failed++;}}
   if(failed)throw Error(failed+' test object(s) could not be deleted');
   return created.length+' temporary objects removed';
  });
 }
 await record('Required local database JSON',async()=>{
  if(!dataDir)throw Error('Data directory unavailable');
  const missing=[],invalid=[];
  for(const name of REQUIRED){
   try{
    const stat=await fs.stat(path.join(dataDir,name));
    if(!stat.isFile()||!stat.size||stat.size>32*1024*1024)throw Error('Invalid file');
    const parsed=JSON.parse(await fs.readFile(path.join(dataDir,name),'utf8'));
    if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw Error('Invalid JSON object');
   }catch(error){
    if(error.code==='ENOENT')missing.push(name);else invalid.push(name);
   }
  }
  if(missing.length||invalid.length)throw Error('Missing: '+(missing.join(',')||'none')+'; invalid: '+(invalid.join(',')||'none'));
  return REQUIRED.length+' required files parse as JSON objects (not financial reconciliation)';
 });
 for(const [name,detail] of [
  ['Actual bot cold restart','Requires isolated process restart against authoritative Drive generation'],
  ['Stripe/Monzo replay and idempotency','Requires actual payment-handler integration tests; no payment requests sent'],
  ['Financial ledger reconciliation','Requires audited balances and transaction comparison'],
  ['Cross-instance locking','In-process serialization alone does not protect multiple running instances']
 ])checks.push({name,status:'NOT RUN',detail});
 const failures=checks.filter(x=>x.status==='FAIL').length;
 const notRun=checks.filter(x=>x.status==='NOT RUN').length;
 const report=['BANKROLL DRIVE READINESS TEST',new Date().toISOString(),'Mode: '+mode,'Safety: isolated test objects only; no customer/payment writes','',
  ...checks.map(x=>x.status.padEnd(8)+' '+x.name+' — '+x.detail),'',
  'RESULT: '+failures+' failures; '+notRun+' required checks NOT RUN.',
  'Production readiness: NOT CERTIFIED. Primary mode NOT changed.'].join('\n');
 return {checks,failures,notRun,report,attachment:new AttachmentBuilder(Buffer.from(report),{name:'bankroll-drive-readiness.txt'})};
}
module.exports={runDriveReadinessTest};
