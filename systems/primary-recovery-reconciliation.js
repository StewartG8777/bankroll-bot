'use strict';
// Read-only recovery reconciliation against the authoritative immutable generation.
// Does not infer business balances from undocumented ledger schemas.
const fs=require('node:fs/promises');
const path=require('node:path');
const crypto=require('node:crypto');
const {REQUIRED}=require('./primary-ledger-audit');
const digest=b=>crypto.createHash('sha256').update(b).digest('hex');
async function reconcileRecoveredData({store,dataDir}){
 if(!store?.currentManifest||!dataDir)throw TypeError('Drive store and dataDir required');
 const {manifest,pointer}=await store.currentManifest();
 if(!Array.isArray(manifest.files)||!manifest.files.length)throw Error('Empty recovery manifest');
 const seen=new Set(),results=[];
 for(const entry of manifest.files){
  const name=entry.path;
  if(typeof name!=='string'||name.startsWith('/')||name.includes('\\')||name.split('/').some(p=>!p||p==='.'||p==='..')||seen.has(name)){
   results.push({name,status:'FAIL',reason:'Unsafe or duplicate manifest path'});continue;
  }
  seen.add(name);
  try{
   const bytes=await fs.readFile(path.join(path.resolve(dataDir),name));
   if(bytes.length!==Number(entry.size)||digest(bytes)!==entry.sha256)throw Error('File differs from authoritative generation');
   let recordCount=null;
   if(name.endsWith('.json')){
    const parsed=JSON.parse(bytes.toString('utf8'));
    if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw Error('Invalid JSON object');
    recordCount=Object.keys(parsed).length;
   }
   results.push({name,status:'PASS',recordCount,bytes:bytes.length});
  }catch(error){results.push({name,status:'FAIL',reason:String(error.message||error)});}
 }
 for(const name of REQUIRED)if(!seen.has(name))results.push({name,status:'FAIL',reason:'Required financial ledger absent from manifest'});
 const failed=results.filter(r=>r.status==='FAIL').length;
 return {generation:pointer.generation,passed:results.length-failed,failed,results,allFilesReconciled:failed===0,accountingBalancesReconciled:false};
}
module.exports={reconcileRecoveredData};
