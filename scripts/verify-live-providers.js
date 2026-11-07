'use strict';
// Explicit opt-in, read-only provider verification on isolated staging data.
// Does not create charges, refunds, webhooks or mutate BANKROLL records.
require('dotenv').config();
const fs=require('node:fs/promises');
const path=require('node:path');
const {verifyProviders}=require('../systems/primary-provider-verifier');
const REQUIRED=['stripe_ledger.json','monzo_payments.json'];
async function validateEnvironment(env=process.env){
 if(env.BANKROLL_LIVE_API_TEST!=='I_AUTHORIZE_READ_ONLY_STAGING_TEST')
  throw Error('Explicit read-only staging test authorization required');
 if(env.BANKROLL_DRIVE_PRIMARY==='1' || env.BANKROLL_PRIMARY_BOOTSTRAPPED==='1')
  throw Error('Refusing to run against a live primary-mode process');
 if(!env.DATA_DIR||!env.STRIPE_SECRET_KEY||!env.MONZO_ACCESS_TOKEN||!env.MONZO_ACCOUNT_ID)
  throw Error('Missing DATA_DIR, Stripe secret, Monzo token or Monzo account');
 if(!env.BANKROLL_MONZO_HISTORY_SINCE||!Number.isFinite(Date.parse(env.BANKROLL_MONZO_HISTORY_SINCE)))
  throw Error('Set BANKROLL_MONZO_HISTORY_SINCE to a valid explicit history start');
 if(!/^sk_(test|live)_/.test(env.STRIPE_SECRET_KEY))
  throw Error('Stripe secret key format invalid');
 if(env.STRIPE_SECRET_KEY.startsWith('sk_live_')&&env.BANKROLL_ALLOW_LIVE_STRIPE_READONLY!=='I_CONFIRM_LIVE_READ_ONLY')
  throw Error('Live Stripe key requires separate explicit read-only confirmation');
 const root=path.resolve(env.DATA_DIR);
 const stat=await fs.stat(root);
 if(!stat.isDirectory())throw Error('DATA_DIR is not a directory');
 for(const name of REQUIRED){
  const value=JSON.parse(await fs.readFile(path.join(root,name),'utf8'));
  if(!value||typeof value!=='object'||Array.isArray(value))throw Error('Invalid staging ledger '+name);
 }
 return root;
}
function summarize(result){
 const safe={providers:{},customerIssues:result.customers?.issues?.length??null,
  customerStatus:result.customers?.status??'UNKNOWN',
  confirmedMismatch:result.confirmedMismatch,fullyVerified:result.fullyVerified};
 for(const [name,record] of Object.entries(result.results||{}))
  safe.providers[name]={status:record.status,verified:record.verified??null,
   issues:record.issues?.length??0,unclassifiedIncoming:record.unclassifiedIncoming?.length??0,
   unseenSavedIds:record.unseenSavedIds?.length??0,completeWindow:record.completeWindow??null,
   reason:record.reason||null};
 return safe;
}
async function main({env=process.env,fetch=globalThis.fetch,Stripe=require('stripe'),log=console.log}={}){
 const dataDir=await validateEnvironment(env);
 const result=await verifyProviders({dataDir,stripe:new Stripe(env.STRIPE_SECRET_KEY,{maxNetworkRetries:2,timeout:20000}),
  monzoFetch:fetch,monzoAccountId:env.MONZO_ACCOUNT_ID,monzoToken:env.MONZO_ACCESS_TOKEN});
 const report=summarize(result);
 log(JSON.stringify(report,null,2));
 if(!result.fullyVerified||result.customers?.status==='ISSUES')process.exitCode=2;
 return report;
}
if(require.main===module)main().catch(error=>{
 console.error('Read-only staging verification failed:',error.message);process.exitCode=1;
});
module.exports={main,validateEnvironment,summarize};
