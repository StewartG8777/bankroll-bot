'use strict';
// Read-only Monzo history scan. Account transfers are not automatically BANKROLL sales.
// Unknown history coverage or unclassified incoming credits must never be VERIFIED.
const {reconcileMonzoRecords}=require('./primary-provider-reconciliation');
const DAY=86400000;
async function scanMonzoHistory({fetch,token,accountId,saved,review={},since,until=Date.now(),maxRequests=20000}){
 if(!fetch||!token||!accountId)throw Error('Monzo credentials and fetch required');
 if(!Number.isFinite(since)||since<0||!Number.isFinite(until)||until<=since)throw Error('Explicit valid history bounds required');
 let requests=0;const byId=new Map(),unknown=[];
 async function window(start,end){
  if(++requests>maxRequests)throw Error('Monzo history request budget exceeded');
  const params=new URLSearchParams({account_id:accountId,since:new Date(start).toISOString(),before:new Date(end).toISOString(),limit:'100'});
  const response=await fetch('https://api.monzo.com/transactions?'+params,{
   headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(15000)
  });
  if(!response.ok)throw Error('Monzo history HTTP '+response.status);
  const body=await response.json();
  if(!Array.isArray(body.transactions))throw Error('Malformed Monzo history response');
  const batch=body.transactions;
  // 100 is the API page cap; split rather than silently assuming completeness.
  if(batch.length>=100){
   if(end-start<=1000)throw Error('Monzo history saturated at minimum window');
   const mid=Math.floor((start+end)/2);
   await window(start,mid);await window(mid,end);return;
  }
  for(const tx of batch){
   if(!tx||!tx.id||String(tx.account_id)!==String(accountId)||!Number.isSafeInteger(tx.amount))throw Error('Malformed or wrong-account Monzo transaction');
   if(byId.has(String(tx.id))){
    const old=byId.get(String(tx.id));
    if(JSON.stringify(old)!==JSON.stringify(tx))throw Error('Conflicting Monzo transaction '+tx.id);
   }else byId.set(String(tx.id),tx);
  }
 }
 for(let start=since;start<until;start+=DAY)await window(start,Math.min(start+DAY,until));
 const known=new Set(Object.values(saved).filter(x=>x?.status==='paid'&&x.txId).map(x=>String(x.txId)));
 const reviewed=new Set(Object.keys(review?.reviewed||{}));
 for(const tx of byId.values()){
  if(tx.amount<=0||known.has(String(tx.id))||reviewed.has(String(tx.id)))continue;
  unknown.push({id:String(tx.id),amountMinor:tx.amount,currency:tx.currency||null,
   reason:'Incoming account transaction not classified as BANKROLL sale'});
 }
 const comparison=reconcileMonzoRecords(saved,[...byId.values()]);
 // If the scan starts after account inception, earlier transactions are unknown.
 // Even complete account history cannot prove which unreviewed credits are sales.
 const issues=comparison.issues.filter(x=>x.type!=='missing-provider-transaction');
 const missing=Object.values(saved).filter(x=>x?.status==='paid'&&x.txId&&!byId.has(String(x.txId)));
 return {status:issues.length?'MISMATCH':'PARTIAL',issues,
  unclassifiedIncoming:unknown,unseenSavedIds:[...new Set(missing.map(x=>String(x.txId)))],
  verified:comparison.verifiedProviderRecords,requests,completeWindow:true,
  since:new Date(since).toISOString(),until:new Date(until).toISOString(),
  reason:'History window scanned; incoming transfers require sale classification and pre-window saved IDs require individual verification'};
}
module.exports={scanMonzoHistory};
