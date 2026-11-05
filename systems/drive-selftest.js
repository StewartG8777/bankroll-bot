'use strict';
// Owner-only, non-destructive Google Drive diagnostic. Never touches live snapshots.
const crypto=require('node:crypto');
const {Readable}=require('node:stream');
const {AttachmentBuilder}=require('discord.js');
async function runDriveSelftest({drive,folderIds,primaryContext}) {
  const results=[];
  const check=async(name,fn)=>{
    try{const detail=await fn();results.push({name,status:'PASS',detail:String(detail||'OK')});}
    catch(e){results.push({name,status:'FAIL',detail:String(e.message||e).slice(0,300)});}
  };
  await check('Google Drive authentication',async()=>{
    const r=await drive.about.get({fields:'user(emailAddress)'});
    return r.data?.user?.emailAddress?'Connected account confirmed':'Drive API responded';
  });
  await check('BANKROLL folder structure',async()=>{
    const required=['DATABASE','VAULT','BACKUPS','CUSTOMER DELIVERIES','PAYMENT RECORDS','LOGS'];
    for(const name of required){
      const id=folderIds[name];
      if(!id)throw Error('Missing folder ID: '+name);
      const r=await drive.files.get({fileId:id,fields:'id,mimeType,trashed'});
      if(r.data?.mimeType!=='application/vnd.google-apps.folder'||r.data?.trashed)throw Error('Invalid folder: '+name);
    }
    return required.length+' required folders verified';
  });
  let uploaded=null;
  const data=crypto.randomBytes(64);
  const hash=crypto.createHash('sha256').update(data).digest('hex');
  try{
    await check('Isolated upload',async()=>{
      if(!folderIds.BACKUPS)throw Error('BACKUPS folder missing');
      const r=await drive.files.create({
        requestBody:{name:'.bankroll-selftest-'+crypto.randomUUID()+'.bin',parents:[folderIds.BACKUPS],description:'Temporary diagnostic; safe to delete'},
        media:{mimeType:'application/octet-stream',body:Readable.from([data])},
        fields:'id'
      });
      uploaded=r.data?.id;
      if(!uploaded)throw Error('No uploaded file ID');
      return 'Temporary object created';
    });
    await check('Download and SHA-256 integrity',async()=>{
      if(!uploaded)throw Error('Upload did not succeed');
      const r=await drive.files.get({fileId:uploaded,alt:'media'},{responseType:'stream'});
      const chunks=[];let size=0;
      for await(const chunk of r.data){size+=chunk.length;if(size>1024)throw Error('Unexpected download size');chunks.push(Buffer.from(chunk));}
      if(size!==data.length||crypto.createHash('sha256').update(Buffer.concat(chunks)).digest('hex')!==hash)throw Error('Downloaded bytes differ');
      return 'Size and SHA-256 match';
    });
  }finally{
    if(uploaded)await check('Temporary object cleanup',async()=>{await drive.files.delete({fileId:uploaded});return 'Deleted';});
    else results.push({name:'Temporary object cleanup',status:'SKIP',detail:'No temporary object created'});
  }
  await check('Primary mode state',async()=>{
    if(!primaryContext)return 'Legacy mode: primary storage not enabled (NOT VALIDATED)';
    const status=primaryContext.manager.status();
    if(!status.active)throw Error('Primary manager is not active');
    const {manifest,pointer}=await primaryContext.store.currentManifest();
    if(!manifest.files?.length||manifest.generation!==pointer.generation)throw Error('Invalid current generation');
    return 'Current manifest verified: '+manifest.files.length+' files';
  });
  for(const name of ['Cold restart recovery','Serialized writes and rollback','Stripe/Monzo webhook idempotency']){
    results.push({name,status:'NOT RUN',detail:'Requires separate isolated integration tests; no live state modified'});
  }
  const failures=results.filter(r=>r.status==='FAIL').length;
  const untested=results.filter(r=>r.status==='NOT RUN').length;
  const report=['BANKROLL DRIVE SELFTEST',new Date().toISOString(),'Mode: '+(primaryContext?'PRIMARY':'LEGACY'),'',
    ...results.map(r=>r.status.padEnd(8)+' '+r.name+' — '+r.detail),'',
    'RESULT: '+(failures?'FAIL':'DIAGNOSTICS PASSED')+'; '+failures+' failures; '+untested+' integration checks not run.',
    'Production readiness: NOT CERTIFIED. Live customer/payment data unchanged.'].join('\n');
  return {results,report,failures,attachment:new AttachmentBuilder(Buffer.from(report),{name:'bankroll-drive-selftest.txt'})};
}
module.exports={runDriveSelftest};
