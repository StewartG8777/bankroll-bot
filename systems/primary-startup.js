'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { walkFiles } = require('./google-drive-primary-storage');
const SEED_REQUIRED = Object.freeze([
  'vault.json','deliveries.json','downloads.json',
  'monzo_payments.json','stripe_ledger.json','webhook_events.json'
]);
async function auditSeedFiles(root) {
  const missing=[],invalid=[];
  for(const name of SEED_REQUIRED) {
    const file=path.join(root,name);
    let raw;
    try {
      const stat=await fs.promises.stat(file);
      if(!stat.isFile()||!stat.size||stat.size>32*1024*1024)throw Error('Invalid file size/type');
      raw=await fs.promises.readFile(file,'utf8');
    }catch(error){missing.push(name);continue;}
    try {
      const parsed=JSON.parse(raw);
      if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw Error('Expected object');
    }catch(error){invalid.push(name);}
  }
  if(missing.length||invalid.length)throw Error(
    'Refusing incomplete financial migration. Missing: '+(missing.join(', ')||'none')+
    '; invalid JSON: '+(invalid.join(', ')||'none')+
    '. Restore and verify ALL current financial ledgers before seeding Drive.'
  );
}

// Never load index.js until a complete, checked Drive generation is restored.
// Initialization can happen only from explicitly trusted EXISTING local state.
async function preparePrimaryData({store,dataDir,allowSeed=false,logger=console}) {
  if (!store || !dataDir) throw new TypeError('Primary storage bootstrap needs a store and DATA_DIR.');
  const root = path.resolve(dataDir);
  let seeded = false;
  try {
    await store.currentManifest();
  } catch (error) {
    if (!/No current primary snapshot pointer exists/.test(String(error.message || error))) throw error;
    if (!allowSeed) throw new Error('No authoritative Google Drive generation. Primary startup is blocked; a verified one-time migration is required.');
    const files = await walkFiles(root);
    if (!files.length) throw new Error('Refusing primary initialization from an empty or unverified local data directory.');
    await auditSeedFiles(root);
    await store.snapshot({allowInitialize:true});
    seeded = true;
  }
  // Download and verify EVERY object before any local file is promoted.
  const restored = await store.restore();
  const {manifest} = await store.currentManifest();
  const expected = new Set(manifest.files.map(entry=>entry.path));
  const existing = await walkFiles(root);
  const stale = existing.filter(item=>!expected.has(item.relative));
  const quarantined = [];
  if (stale.length) {
    const quarantine=path.join(root,'.drive-cache-quarantine-'+crypto.randomUUID());
    for (const item of stale) {
      const destination=path.join(quarantine,item.relative);
      await fs.promises.mkdir(path.dirname(destination),{recursive:true});
      await fs.promises.rename(item.absolute,destination);
      quarantined.push(item.relative);
    }
    logger.warn?.('[PrimaryBoot] Untracked cache files isolated; retained under '+quarantine);
  }
  return {seeded,generation:restored.generation,restored:restored.files,quarantined};
}
module.exports={preparePrimaryData,SEED_REQUIRED,auditSeedFiles};
