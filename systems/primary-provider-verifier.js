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
   const {scanStripeHistory}=require('./primary-stripe-history');
   results.stripe=await scanStripeHistory({stripe,saved:stripeSaved});
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
   if(comparison.issues.length){
    results.monzo={status:'MISMATCH',issues:comparison.issues,verified:comparison.verifiedProviderRecords};
   }else if(process.env.BANKROLL_MONZO_HISTORY_SINCE){
    const since=Date.parse(process.env.BANKROLL_MONZO_HISTORY_SINCE);
    const {scanMonzoHistory}=require('./primary-monzo-history');
    const review=JSON.parse(await fs.readFile(path.join(dataDir,'monzo_review.json'),'utf8'));
    const scan=await scanMonzoHistory({fetch:monzoFetch,token:monzoToken,accountId:monzoAccountId,
     saved:monzoSaved,review,since});
    results.monzo={...scan,verifiedSavedTransactions:comparison.verifiedProviderRecords};
   }else{
    results.monzo={status:'PARTIAL',verified:comparison.verifiedProviderRecords,
     reason:'Saved transaction IDs verified; set BANKROLL_MONZO_HISTORY_SINCE to scan account history. Unclassified incoming transfers require review.'};
   }
  }catch(error){results.monzo={status:'INCOMPLETE',reason:String(error.message||error)};}
 }
 return {results,confirmedMismatch:Object.values(results).some(x=>x.status==='MISMATCH'),fullyVerified:Object.values(results).every(x=>x.status==='VERIFIED')};
}
module.exports={verifyProviders};
