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
  const dir=writableDir();
  process.env.DATA_DIR=dir;
  const lockPath=path.join(dir,'.drive-primary-writer.lock');
  const lock=fs.openSync(lockPath,'wx',0o600);
  process.on('exit',()=>{try{fs.closeSync(lock);fs.unlinkSync(lockPath);}catch{}});
  await restoreCredentials(dir);
  const auth=oauth(dir);
  const drive=google.drive({version:'v3',auth});
  const ids=await folderIds(drive);
  const store=createGoogleDrivePrimaryStorage({google,auth,dataDir:dir,folderIds:ids});
  const allowSeed=process.env.BANKROLL_DRIVE_INITIALIZE==='I_HAVE_VERIFIED_BACKUPS';
  const result=await preparePrimaryData({store,dataDir:dir,allowSeed});
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
