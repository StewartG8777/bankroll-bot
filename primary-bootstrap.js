#!/usr/bin/env node
'use strict';
// Safe launch barrier for authoritative Google Drive storage. Never load the
// application or open payment webhooks until all current objects are verified.
require('dotenv').config();
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const {Client,GatewayIntentBits}=require('discord.js');
const {google}=require('googleapis');
const {atomicWriteFile}=require('./systems/atomic-storage');
const createGoogleDriveSystem=require('./systems/google-drive');
const {GOOGLE_DRIVE_ROOT_NAME,GOOGLE_DRIVE_FOLDER_NAMES}=require('./systems/drive-layout');
const {createGoogleDrivePrimaryStorage}=require('./systems/google-drive-primary-storage');
const {createPrimaryCommitManager}=require('./systems/primary-commit-manager');
const {installPrimaryContext}=require('./systems/primary-storage-context');
const {preparePrimaryData}=require('./systems/primary-startup');

function writableDir() {
  const requested=path.resolve(process.env.DATA_DIR||path.join(__dirname,'data'));
  for(const candidate of new Set([requested,path.join(__dirname,'data'),path.join('/tmp','bankroll-data')])) {
    try {
      fs.mkdirSync(candidate,{recursive:true});
      const probe=path.join(candidate,'.primary-probe-'+process.pid);
      fs.writeFileSync(probe,'ok',{flag:'wx'});fs.unlinkSync(probe);
      if(candidate!==requested)console.warn('[PrimaryBoot] Falling back from unavailable DATA_DIR:',requested);
      return candidate;
    }catch(error){console.warn('[PrimaryBoot] DATA_DIR candidate unusable:',candidate,error.message);}
  }
  throw Error('No writable data directory for Drive recovery.');
}
function storedAuthFiles(dir) {
  return [path.join(dir,'google-oauth.json'),path.join(dir,'google-drive-token.json')];
}
async function restoreCredentials(dir) {
  const [clientFile,tokenFile]=storedAuthFiles(dir);
  if(fs.existsSync(clientFile)&&fs.existsSync(tokenFile))return;
  if(!process.env.BANKROLL_BACKUP_ENCRYPTION_KEY)throw Error('BANKROLL_BACKUP_ENCRYPTION_KEY is required for Google credential recovery.');
  if(!process.env.DISCORD_TOKEN)throw Error('DISCORD_TOKEN is required to find the encrypted OAuth backup.');
  console.log('[PrimaryBoot] Recovering OAuth credentials from the independent Discord backup.');
  const discord=new Client({intents:[GatewayIntentBits.Guilds]});
  try {
    await discord.login(process.env.DISCORD_TOKEN);
    let channel=null;
    if(process.env.BACKUP_CHANNEL_ID){
      channel=await discord.channels.fetch(process.env.BACKUP_CHANNEL_ID).catch(()=>null);
    }
    if(!channel) {
      for(const guild of discord.guilds.cache.values()){
        const channels=await guild.channels.fetch().catch(()=>null);
        channel=channels?.find(c=>c?.name==='bankroll-backup'&&c?.messages?.fetch)||null;
        if(channel)break;
      }
    }
    if(!channel?.messages?.fetch)throw Error('No accessible #bankroll-backup channel was found.');
    const decrypt=createGoogleDriveSystem({crypto,Buffer}).decryptGoogleAuthBundle;
    let before=null;
    for(let page=0;page<15;page++){
      const messages=await channel.messages.fetch({limit:100,...(before?{before}:{})});
      if(!messages.size)break;
      for(const msg of messages.values()){
        if(msg.author?.id!==discord.user.id)continue;
        const attachment=msg.attachments?.find(a=>a.name==='google-drive-auth.enc');
        if(!attachment)continue;
        try {
          const response=await fetch(attachment.url,{signal:AbortSignal.timeout(20000)});
          if(!response.ok)continue;
          const bytes=Buffer.from(await response.arrayBuffer());
          if(bytes.length>1024*1024)continue;
          const parsed=decrypt(bytes);
          if(!parsed.client||!parsed.token)continue;
          const o=parsed.client.web||parsed.client.installed||parsed.client;
          if(!o.client_id||!o.client_secret||!parsed.token.refresh_token)continue;
          atomicWriteFile(clientFile,JSON.stringify(parsed.client,null,2));
          atomicWriteFile(tokenFile,JSON.stringify(parsed.token,null,2));
          console.log('[PrimaryBoot] OAuth credentials recovered from Discord.');
          return;
        }catch(error){console.warn('[PrimaryBoot] Cannot decrypt one OAuth backup:',error.message);}
      }
      before=messages.last()?.id;
      if(messages.size<100||!before)break;
    }
    throw Error('Could not recover a valid encrypted Google OAuth bundle from Discord.');
  }finally{await discord.destroy().catch(()=>{});}
}
function oauth(dir) {
  const [clientFile,tokenFile]=storedAuthFiles(dir);
  const parsed=JSON.parse(fs.readFileSync(clientFile,'utf8'));
  const cfg=parsed.web||parsed.installed||parsed;
  const token=JSON.parse(fs.readFileSync(tokenFile,'utf8'));
  if(!cfg.client_id||!cfg.client_secret||!(token.refresh_token||token.access_token))throw Error('Google OAuth credentials incomplete.');
  const base=String(process.env.PUBLIC_URL||process.env.RENDER_EXTERNAL_URL||'').replace(/\/+$/,'');
  const auth=new google.auth.OAuth2(cfg.client_id,cfg.client_secret,base+'/google/oauth/callback');
  auth.setCredentials(token);
  auth.on('tokens',fresh=>{
    const previous=(()=>{try{return JSON.parse(fs.readFileSync(tokenFile,'utf8'));}catch{return token;}})();
    const merged={...previous,...fresh};
    if(previous.refresh_token&&!merged.refresh_token)merged.refresh_token=previous.refresh_token;
    atomicWriteFile(tokenFile,JSON.stringify(merged,null,2));
  });
  return auth;
}
async function findOneFolder(drive,name,parent) {
  const escaped=String(name).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
  const q="name = '"+escaped+"' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"+
    (parent?" and '"+String(parent).replace(/'/g,"\\'")+"' in parents":'');
  const result=await drive.files.list({q,fields:'nextPageToken,files(id,name)',pageSize:1000,supportsAllDrives:true,includeItemsFromAllDrives:true});
  if(result.data.files?.length!==1||result.data.nextPageToken)throw Error('Expected exactly one existing Drive folder '+name+'. Run /drive setup in legacy mode and resolve duplicates before migration.');
  return result.data.files[0].id;
}
async function folderIds(drive) {
  const root=await findOneFolder(drive,GOOGLE_DRIVE_ROOT_NAME);
  const ids={};
  for(const name of GOOGLE_DRIVE_FOLDER_NAMES)ids[name]=await findOneFolder(drive,name,root);
  return ids;
}
async function main(){
  if(process.env.BANKROLL_DRIVE_PRIMARY!=='1') {
    // Existing production behavior remains the default until an explicit,
    // tested Render cutover.
    require('./index');
    return;
  }
  // Google Drive does not provide an atomic compare-and-swap lease. Until a
  // genuine distributed fencing mechanism exists, primary mode is limited to
  // a deliberately confirmed single-instance deployment. A local lock alone
  // cannot protect against another Render instance or machine.
  if(process.env.BANKROLL_PRIMARY_SINGLE_INSTANCE_CONFIRMED!=='I_HAVE_VERIFIED_ONE_INSTANCE') {
    throw new Error('Primary mode blocked: verify exactly one bot instance and set BANKROLL_PRIMARY_SINGLE_INSTANCE_CONFIRMED=I_HAVE_VERIFIED_ONE_INSTANCE. This is not distributed locking.');
  }
  const {assessPrimaryDeployment}=require('./systems/primary-deployment-preflight');
  const preflight=assessPrimaryDeployment();
  if(preflight.failed)throw new Error('Primary deployment preflight failed: '+preflight.checks.filter(x=>!x.pass).map(x=>x.name).join(', '));
  const dir=writableDir();
  process.env.DATA_DIR=dir;
  const {acquireLocalWriterLock}=require('./systems/primary-local-writer-lock');
  const writerLock=acquireLocalWriterLock(dir);
  process.on('exit',()=>{try{writerLock.release();}catch(error){console.error('[PrimaryBoot] Lock cleanup failed:',error.message);}});
  await restoreCredentials(dir);
  const auth=oauth(dir);
  const drive=google.drive({version:'v3',auth});
  const ids=await folderIds(drive);
  const store=createGoogleDrivePrimaryStorage({google,auth,dataDir:dir,folderIds:ids});
  const allowSeed=process.env.BANKROLL_DRIVE_INITIALIZE==='I_HAVE_VERIFIED_BACKUPS';
  const result=await preparePrimaryData({store,dataDir:dir,allowSeed});
  // Refuse to open payment/Discord handlers if any recovered required ledger
  // differs from the authoritative manifest. Never seed from partial data.
  const {auditRecoveredLedgers}=require('./systems/primary-ledger-audit');
  const ledgerAudit=await auditRecoveredLedgers({store,dataDir:dir});
  if(ledgerAudit.failed)throw new Error('Primary startup blocked: '+ledgerAudit.failed+' financial ledgers failed authoritative checksum/JSON audit: '+ledgerAudit.results.filter(x=>x.status==='FAIL').map(x=>x.name).join(', '));
  console.log('[PrimaryBoot] Verified',ledgerAudit.passed,'required financial ledgers against authoritative Drive manifest (not an accounting balance audit).');
  const {reconcileRecoveredData}=require('./systems/primary-recovery-reconciliation');
  const recovery=await reconcileRecoveredData({store,dataDir:dir});
  if(recovery.failed)throw new Error('Primary startup blocked: '+recovery.failed+' recovered files differ from authoritative Drive generation: '+recovery.results.filter(x=>x.status==='FAIL').map(x=>x.name).join(', '));
  console.log('[PrimaryBoot] Reconciled',recovery.passed,'recovered files against immutable Drive generation (not an accounting balance audit).');
  const {auditRecoveredAccounting}=require('./systems/primary-accounting-audit');
  const accounting=await auditRecoveredAccounting({dataDir:dir});
  if(!accounting.passed)throw new Error('Primary startup blocked: recovered accounting records failed schema audit: '+accounting.issues.join('; '));
  console.log('[PrimaryBoot] Recovered accounting records validated:',accounting.totals.stripePayments,'Stripe and',accounting.totals.monzoPaidTransactions,'deduplicated paid Monzo records. External provider reconciliation remains pending.');
  const {verifyProviders}=require('./systems/primary-provider-verifier');
  const Stripe=require('stripe');
  const providerAudit=await verifyProviders({
    dataDir:dir,
    stripe:process.env.STRIPE_SECRET_KEY?new Stripe(process.env.STRIPE_SECRET_KEY):null,
    monzoFetch:globalThis.fetch,
    monzoAccountId:process.env.MONZO_ACCOUNT_ID,
    monzoToken:process.env.MONZO_ACCESS_TOKEN
  });
  if(providerAudit.confirmedMismatch)throw new Error('Primary startup blocked: confirmed provider mismatch');
  console.log('[PrimaryBoot] Provider verification states:',Object.fromEntries(
    Object.entries(providerAudit.results).map(([name,state])=>[name,state.status])
  ));
  // Until payment-specific command and webhook gates are independently tested,
  // do not start a process that can accept payments on incomplete verification.
  if(!providerAudit.fullyVerified)throw new Error(
    'Primary startup blocked: provider reconciliation incomplete. Payment operations cannot safely be enabled. '+
    'Keep the existing production deployment active; do not bypass this guard.'
  );
  const {applyPathRebase}=require('./systems/primary-path-rebase');
  const {manifest}=await store.currentManifest();
  const rebased=applyPathRebase({dataDir:dir,manifest});
  if(rebased){
    // Path normalization is itself a persistent change: commit its generation
    // before enabling Stripe, Monzo or any Discord command handlers.
    const committed=await store.snapshot({allowInitialize:false});
    result.generation=committed.generation;
    console.log('[PrimaryBoot] Rebased and committed',rebased,'verified stored file paths.');
  }
  console.log('[PrimaryBoot] Verified Drive generation',result.generation,'restored files:',result.restored,'isolated stale:',result.quarantined.length);
  const manager=createPrimaryCommitManager({
    store,onError:error=>console.error('[PrimaryDrive] Persistence is NOT durable:',error.message)
  });
  installPrimaryContext({store,manager,dir,generation:result.generation});
  process.env.BANKROLL_PRIMARY_BOOTSTRAPPED='1';
  require('./index');
}
if(require.main===module)main().catch(error=>{
  console.error('[PrimaryBoot] BLOCKED: application, payments and Discord were NOT started:',error.stack||error);
  process.exitCode=1;
});
module.exports={writableDir,findOneFolder,folderIds,restoreCredentials,main};
