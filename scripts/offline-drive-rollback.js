#!/usr/bin/env node
'use strict';
// Execute in an isolated maintenance process only after all Bankroll instances stop.
require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const { google } = require('googleapis');
const { GOOGLE_DRIVE_ROOT_NAME, GOOGLE_DRIVE_FOLDER_NAMES } = require('../systems/drive-layout');
const { createGoogleDrivePrimaryStorage } = require('../systems/google-drive-primary-storage');
const { createSerializedStorageQueue } = require('../systems/serialized-storage-queue');
const { verifiedOfflineRollback } = require('../systems/verified-drive-rollback');

const dataDir=path.resolve(process.env.DATA_DIR || path.join(__dirname,'..','data'));
const [action,generation,confirmation]=process.argv.slice(2);
function oauthClient() {
  const parsed=JSON.parse(fs.readFileSync(path.join(dataDir,'google-oauth.json'),'utf8'));
  const config=parsed.web || parsed.installed || parsed;
  const token=JSON.parse(fs.readFileSync(path.join(dataDir,'google-drive-token.json'),'utf8'));
  if(!config.client_id || !config.client_secret || !(token.refresh_token || token.access_token)) throw Error('Google OAuth credentials missing.');
  const base=(process.env.PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/+$/,'');
  const auth=new google.auth.OAuth2(config.client_id,config.client_secret,base+'/google/oauth/callback');
  auth.setCredentials(token);return auth;
}
async function existingFolder(drive,name,parent) {
  if(!/^[A-Z ]+$/.test(name)) throw Error('Invalid folder name.');
  const q="name = '"+name+"' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"+
    (parent?" and '"+parent+"' in parents":'');
  const result=await drive.files.list({q,fields:'nextPageToken,files(id,name)',pageSize:1000,supportsAllDrives:true,includeItemsFromAllDrives:true});
  if(result.data.files?.length!==1 || result.data.nextPageToken) throw Error('Expected exactly one BANKROLL folder '+name+'.');
  return result.data.files[0].id;
}
async function main() {
  if(!['verify','rollback'].includes(action) || !generation) throw Error('Usage: verify <generation> OR rollback <generation> --i-confirm-bot-is-stopped');
  if(!fs.existsSync(dataDir)) throw Error('DATA_DIR unavailable: '+dataDir);
  if(action==='rollback' && (confirmation!=='--i-confirm-bot-is-stopped' || process.env.BANKROLL_MAINTENANCE_MODE!=='1'))
    throw Error('Set BANKROLL_MAINTENANCE_MODE=1 and explicitly confirm every bot/payment writer is stopped.');
  const auth=oauthClient(),drive=google.drive({version:'v3',auth});
  const root=await existingFolder(drive,GOOGLE_DRIVE_ROOT_NAME);
  const folderIds={};
  for(const name of GOOGLE_DRIVE_FOLDER_NAMES) folderIds[name]=await existingFolder(drive,name,root);
  const store=createGoogleDrivePrimaryStorage({google,auth,dataDir,folderIds});
  if(action==='verify') {
    const verified=await store.verifyGeneration(generation);
    console.log('Verified without modifying files:',verified.generation,'files:',verified.files);
    return;
  }
  // Local lock prevents concurrent OFFLINE rollback tools; it cannot stop the live bot.
  if(fs.existsSync(path.join(dataDir,'.drive-primary-writer.lock')))
    throw Error('A primary writer lock exists. Stop all live Bankroll processes and verify they have exited before rollback.');
  const lockPath=path.join(dataDir,'.drive-maintenance.lock'),lock=fs.openSync(lockPath,'wx',0o600);
  try{
    const result=await verifiedOfflineRollback({
      queue:createSerializedStorageQueue(),store,generation,confirm:true,
      assertStopped:async()=>process.env.BANKROLL_MAINTENANCE_MODE==='1'
    });
    console.log('Restored generation:',result.promoted.generation);
    console.log('Safety generation:',result.safetyGeneration);
    console.log('Reconcile Stripe/Monzo payments since the selected generation before restarting.');
  }finally{fs.closeSync(lock);fs.rmSync(lockPath,{force:true});}
}
main().catch(error=>{console.error('Offline rollback failed:',error.message);process.exitCode=1;});
