'use strict';
const {AttachmentBuilder}=require('discord.js');
const {runDriveReadinessTest}=require('./drive-readiness-test');
const {fork}=require('node:child_process');
const path=require('node:path');
const fs=require('node:fs/promises');
const os=require('node:os');
const crypto=require('node:crypto');

async function runIsolatedProcessRecovery(){
 const directory=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-migration-'));
 try{
  const file=path.join(directory,'generation.json');
  const payload=JSON.stringify({marker:crypto.randomUUID(),version:2});
  await fs.writeFile(file,payload);
  const digest=crypto.createHash('sha256').update(payload).digest('hex');
  const code=`const fs=require('fs'),crypto=require('crypto');const content=fs.readFileSync(process.argv[1],'utf8');if(crypto.createHash('sha256').update(content).digest('hex')!==process.argv[2])process.exit(2);if(JSON.parse(content).version!==2)process.exit(3)`;
  const {spawn}=require('node:child_process');
  await new Promise((resolve,reject)=>{
   const child=spawn(process.execPath,['-e',code,file,digest],{stdio:'ignore',timeout:10000});
   child.on('error',reject);
   child.on('close',(status)=>status===0?resolve():reject(Error('Isolated process exited '+status)));
  });
  return 'Fresh Node process recovered and SHA-256 verified disposable local test data (not authoritative Drive startup)';
 }finally{await fs.rm(directory,{recursive:true,force:true});}
}
async function runMigrationTest({drive,folderIds,dataDir}){
 const base=await runDriveReadinessTest({drive,folderIds,dataDir,mode:'LEGACY'});
 const checks=base.checks.filter(item=>!['Actual bot cold restart','Stripe/Monzo replay and idempotency','Financial ledger reconciliation','Cross-instance locking'].includes(item.name));
 try{checks.push({name:'Fresh-process isolated recovery',status:'PASS',detail:await runIsolatedProcessRecovery()});}
 catch(error){checks.push({name:'Fresh-process isolated recovery',status:'FAIL',detail:String(error.message||error)});}
 for(const [name,detail] of [
  ['Authoritative Drive cold restart','NOT RUN: requires staging primary-mode process and independent Drive generation'],
  ['Stripe/Monzo duplicate webhook replay','NOT RUN: payment handlers not executed; requires sandbox integration tests'],
  ['Financial ledger reconciliation','NOT RUN: JSON syntax is not proof of balanced payment/customer ledgers'],
  ['Distributed writer fencing','NOT RUN: no cross-instance lock in legacy storage; must be implemented before migration']
 ])checks.push({name,status:'NOT RUN',detail});
 const failures=checks.filter(x=>x.status==='FAIL').length;
 const notRun=checks.filter(x=>x.status==='NOT RUN').length;
 const passed=checks.filter(x=>x.status==='PASS').length;
 const report=['BANKROLL MIGRATION TEST',new Date().toISOString(),'Mode: LEGACY','No primary mode activation; no real payments or customer writes','',...checks.map(x=>x.status.padEnd(8)+' '+x.name+' — '+x.detail),'',`RESULT: ${passed} passed, ${failures} failed, ${notRun} not run`,'Production readiness: NOT CERTIFIED'].join('\n');
 return {passed,failures,notRun,report,attachment:new AttachmentBuilder(Buffer.from(report),{name:'bankroll-migration-test.txt'})};
}
module.exports={runMigrationTest,runIsolatedProcessRecovery};
