'use strict';
const fs=require('node:fs/promises');
const path=require('node:path');
const {AttachmentBuilder}=require('discord.js');
// Read-only gap assessment. Never exercises actual payment endpoints or changes storage mode.
async function runRemainingChecks({dataDir,primaryEnabled=false}={}){
 const checks=[];
 const check=async(name,fn)=>{
  try{const result=await fn();checks.push({name,status:result.status,detail:result.detail});}
  catch(error){checks.push({name,status:'FAIL',detail:String(error.message||error).slice(0,250)});}
 };
 await check('Financial ledger structure',async()=>{
  const names=['vault.json','deliveries.json','downloads.json','monzo_payments.json','stripe_ledger.json','webhook_events.json'];
  const absent=[],invalid=[];
  for(const name of names){
   try{
    const stat=await fs.stat(path.join(dataDir,name));
    if(!stat.isFile()||stat.size>32*1024*1024)throw Error('Invalid file size/type');
    const json=JSON.parse(await fs.readFile(path.join(dataDir,name),'utf8'));
    if(!json||typeof json!=='object'||Array.isArray(json))throw Error('Not a JSON object');
   }catch(e){(e.code==='ENOENT'?absent:invalid).push(name);}
  }
  if(absent.length||invalid.length)return {status:'FAIL',detail:'Missing: '+(absent.join(',')||'none')+'; invalid: '+(invalid.join(',')||'none')};
  return {status:'PASS',detail:'Six files exist and parse; financial totals NOT reconciled'};
 });
 for(const [name,detail] of [
  ['Actual authoritative Drive restart','Requires separate staging deployment with real primary generation; cannot restart live bot safely from command'],
  ['Stripe/Monzo replay after restart','Requires sandbox payment-handler integration tests; no payment webhook is invoked'],
  ['Financial balance reconciliation','Requires ledger schema and transaction-level accounting audit; syntax alone is insufficient'],
  ['Distributed locking/fencing','Not implemented: local queue and single-instance confirmation are not distributed fencing']
 ])checks.push({name,status:'NOT RUN',detail});
 const failures=checks.filter(x=>x.status==='FAIL').length;
 const passed=checks.filter(x=>x.status==='PASS').length;
 const notRun=checks.filter(x=>x.status==='NOT RUN').length;
 const report=['BANKROLL REMAINING MIGRATION CHECKS',new Date().toISOString(),'Primary mode: '+(primaryEnabled?'ENABLED':'LEGACY'),'Read-only; no payments, Drive writes, or customer changes','',...checks.map(x=>x.status.padEnd(8)+' '+x.name+' — '+x.detail),'',`RESULT: ${passed} passed; ${failures} failed; ${notRun} not run`,'Production readiness: NOT CERTIFIED'].join('\n');
 return {passed,failures,notRun,attachment:new AttachmentBuilder(Buffer.from(report),{name:'bankroll-remaining-checks.txt'})};
}
module.exports={runRemainingChecks};
