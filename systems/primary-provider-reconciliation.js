'use strict';
// Provider reconciliation is explicitly read-only. Never mutate ledgers or payments.
// Missing provider access is UNKNOWN, never proof of a mismatch.
function reconcileStripeRecords(saved,provider){
 const issues=[],seen=new Set();
 for(const pi of provider){
  if(!pi||!pi.id||seen.has(pi.id)){issues.push({type:'duplicate-provider-id',id:pi?.id});continue;}
  seen.add(pi.id);
  const record=saved[pi.id];
  if(!record){if(pi.status==='succeeded'&&pi.metadata?.agent_id)issues.push({type:'missing-local-payment',id:pi.id});continue;}
  if(pi.status!=='succeeded')issues.push({type:'status-mismatch',id:pi.id});
  if(Number(record.amount)!==Number(pi.amount_received??pi.amount))issues.push({type:'amount-mismatch',id:pi.id});
  if(String(record.currency).toLowerCase()!==String(pi.currency).toLowerCase())issues.push({type:'currency-mismatch',id:pi.id});
 }
 return {issues,verifiedProviderRecords:seen.size};
}
function reconcileMonzoRecords(saved,transactions){
 const issues=[],byTx=new Map();
 for(const [key,p] of Object.entries(saved)){
  if(p?.status!=='paid'||!p.txId)continue;
  const tx=String(p.txId);
  if(!byTx.has(tx))byTx.set(tx,[]);
  byTx.get(tx).push({key,p});
 }
 const found=new Set();
 for(const tx of transactions){
  if(!tx?.id||found.has(tx.id)){issues.push({type:'duplicate-provider-id',id:tx?.id});continue;}
  found.add(tx.id);
  const matches=byTx.get(String(tx.id))||[];
  for(const {p} of matches){
   // Monzo API amounts are minor units; saved ledger amounts are major units.
   if(!Number.isFinite(Number(p.amount))||Math.round(Number(p.amount)*100)!==Math.abs(Number(tx.amount)))issues.push({type:'amount-mismatch',id:tx.id});
   if(p.currency&&tx.currency&&String(p.currency).toLowerCase()!==String(tx.currency).toLowerCase())issues.push({type:'currency-mismatch',id:tx.id});
  }
 }
 for(const [tx,records] of byTx)if(!found.has(tx))issues.push({type:'missing-provider-transaction',id:tx,localRecords:records.length});
 return {issues,verifiedProviderRecords:found.size};
}
module.exports={reconcileStripeRecords,reconcileMonzoRecords};
