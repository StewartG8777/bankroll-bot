'use strict';
// Read-only migration reconciliation: verify that each required financial ledger
// in the authoritative manifest matches the local recovered file byte-for-byte.
// This is a storage reconciliation, NOT a business-accounting balance audit.
const fs=require('node:fs/promises');
const path=require('node:path');
const crypto=require('node:crypto');
const REQUIRED=['vault.json','deliveries.json','downloads.json','monzo_payments.json','stripe_ledger.json','webhook_events.json'];
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
async function auditRecoveredLedgers({store,dataDir}){
 if(!store?.currentManifest||!dataDir)throw TypeError('Drive store and data directory required');
 const {manifest,pointer}=await store.currentManifest();
 const byPath=new Map(manifest.files.map(f=>[f.path,f]));
 const results=[];
 for(const name of REQUIRED){
  const entry=byPath.get(name);
  if(!entry){results.push({name,status:'FAIL',reason:'Missing from authoritative manifest'});continue;}
  try{
   const absolute=path.join(path.resolve(dataDir),name);
   const stat=await fs.stat(absolute);
   if(!stat.isFile()||stat.size>32*1024*1024)throw Error('Invalid file size/type');
   const bytes=await fs.readFile(absolute);
   if(bytes.length!==Number(entry.size)||hash(bytes)!==entry.sha256)throw Error('Recovered file differs from authoritative Drive manifest');
   const parsed=JSON.parse(bytes.toString('utf8'));
   if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw Error('Invalid JSON object');
   results.push({name,status:'PASS',reason:'Manifest size/SHA-256 and JSON verified'});
  }catch(error){results.push({name,status:'FAIL',reason:String(error.message||error)});}
 }
 return {generation:pointer.generation,results,passed:results.filter(x=>x.status==='PASS').length,failed:results.filter(x=>x.status==='FAIL').length,accountingReconciled:false};
}
module.exports={auditRecoveredLedgers,REQUIRED};
