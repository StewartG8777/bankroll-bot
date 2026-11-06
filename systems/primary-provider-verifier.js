'use strict';
// Read-only provider verification. Never declare a partial provider page complete.
const fs=require('node:fs/promises');
const path=require('node:path');
const {reconcileStripeRecords,reconcileMonzoRecords}=require('./primary-provider-reconciliation');
async function verifyProviders({dataDir,stripe,monzoFetch,monzoAccountId,monzoToken}){
 const stripeSaved=JSON.parse(await fs.readFile(path.join(dataDir,'stripe_ledger.json'),'utf8')).payments;
 const monzoSaved=JSON.parse(await fs.readFile(path.join(dataDir,'monzo_payments.json'),'utf8'));
 const results={stripe:{status:'INCOMPLETE'},monzo:{status:'INCOMPLETE'}};
 if(stripe){
  try{
   const savedIds=Object.keys(stripeSaved);
   const provider=[];
   // Retrieve each known ID individually: a 2000-record list cap is not an audit.
   for(const id of savedIds){
    const pi=await stripe.paymentIntents.retrieve(id);
    provider.push(pi);
   }
   const comparison=reconcileStripeRecords(stripeSaved,provider);
   // Individual lookups validate existing records but cannot discover absent ones.
   results.stripe={status:comparison.issues.length?'MISMATCH':'PARTIAL',issues:comparison.issues,verified:comparison.verifiedProviderRecords,reason:'Existing payment IDs checked; provider-wide missing-payment scan not completed'};
  }catch(error){results.stripe={status:'INCOMPLETE',reason:String(error.message||error)};}
 }
 if(monzoFetch&&monzoToken&&monzoAccountId){
  try{
   const txIds=[...new Set(Object.values(monzoSaved).filter(p=>p?.status==='paid'&&p.txId).map(p=>String(p.txId)))];
   const transactions=[];
   for(const id of txIds){
    const response=await monzoFetch('https://api.monzo.com/transactions/'+encodeURIComponent(id)+'?expand[]=merchant',{
     headers:{Authorization:'Bearer '+monzoToken},signal:AbortSignal.timeout(15000)
    });
    if(!response.ok)throw Error('Monzo verification HTTP '+response.status);
    const body=await response.json();
    if(!body.transaction||String(body.transaction.account_id)!==String(monzoAccountId))throw Error('Monzo transaction account mismatch');
    transactions.push(body.transaction);
   }
   const comparison=reconcileMonzoRecords(monzoSaved,transactions);
   results.monzo={status:comparison.issues.length?'MISMATCH':'PARTIAL',issues:comparison.issues,verified:comparison.verifiedProviderRecords,reason:'Existing transaction IDs checked; provider-wide missing-payment scan not completed'};
  }catch(error){results.monzo={status:'INCOMPLETE',reason:String(error.message||error)};}
 }
 return {results,confirmedMismatch:Object.values(results).some(x=>x.status==='MISMATCH'),fullyVerified:false};
}
module.exports={verifyProviders};
