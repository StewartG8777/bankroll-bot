'use strict';
// Provider-to-ledger customer payment verification. Read-only; no invented balances.
// Stripe customer IDs come from independent PaymentIntent metadata. Monzo API
// does not establish BANKROLL customer identity, so attribution stays unverified.
function verifyCustomerPayments({stripeSaved={},stripeProvider=[],monzoSaved={},monzoProvider=[],
 stripeHistoryComplete=false,monzoHistoryComplete=false}){
 const issues=[],totals={};
 const add=(customer,currency,minor,side)=>{
  if(!customer)return;
  const key=String(customer),ccy=String(currency||'').toLowerCase();
  if(!ccy||!Number.isSafeInteger(minor)||minor<0){issues.push({type:'invalid-provider-amount',customer:key});return;}
  const b=(totals[key]??={})[ccy]??={localStripeGrossMinor:0,providerStripeGrossMinor:0,localMonzoGrossMinor:0,verifiedMonzoGrossMinor:0};
  b[side]+=minor;
 };
 for(const [id,p] of Object.entries(stripeSaved)){
  if(!p||!Number.isSafeInteger(p.amount)||p.amount<0){issues.push({type:'invalid-local-stripe',id});continue;}
  add(p.customerId,p.currency,p.amount,'localStripeGrossMinor');
 }
 const seen=new Set();
 for(const pi of stripeProvider){
  if(!pi?.id||seen.has(pi.id)){issues.push({type:'duplicate-provider-stripe',id:pi?.id});continue;}
  seen.add(pi.id);
  if(pi.status!=='succeeded'||!pi.metadata?.agent_id)continue;
  const amount=pi.amount_received??pi.amount;
  add(pi.metadata.customer_id,pi.currency,amount,'providerStripeGrossMinor');
  if(!pi.metadata.customer_id)issues.push({type:'unattributed-provider-stripe',id:pi.id});
 }
 const byMonzoId=new Map();
 for(const [key,p] of Object.entries(monzoSaved)){
  if(p?.status!=='paid'||!p.txId)continue;
  const id=String(p.txId);
  const existing=byMonzoId.get(id);
  const preferred=key==='monzo_import_'+id||String(p.source||'').includes('historical transaction');
  if(!existing||preferred&&!existing.preferred)byMonzoId.set(id,{p,preferred});
 }
 const providerMonzo=new Map();
 for(const tx of monzoProvider){
  if(!tx?.id||providerMonzo.has(String(tx.id))){issues.push({type:'duplicate-provider-monzo',id:tx?.id});continue;}
  providerMonzo.set(String(tx.id),tx);
 }
 for(const [id,{p}] of byMonzoId){
  const amount=Math.round(Number(p.amount)*100);
  if(!Number.isSafeInteger(amount)||amount<0){issues.push({type:'invalid-local-monzo',id});continue;}
  add(p.customerId,p.currency,amount,'localMonzoGrossMinor');
  const tx=providerMonzo.get(id);
  if(!tx){issues.push({type:'unverified-monzo-transaction',id});continue;}
  if(!Number.isSafeInteger(tx.amount)||tx.amount<=0||tx.amount!==amount||
    String(tx.currency||'').toLowerCase()!==String(p.currency||'').toLowerCase()){
   issues.push({type:'monzo-payment-mismatch',id});continue;
  }
  add(p.customerId,tx.currency,tx.amount,'verifiedMonzoGrossMinor');
 }
 if(stripeHistoryComplete){
  for(const [customer,currencies] of Object.entries(totals))for(const [currency,b] of Object.entries(currencies))
   if(b.localStripeGrossMinor!==b.providerStripeGrossMinor)issues.push({type:'customer-stripe-total-mismatch',customer,currency,
    localMinor:b.localStripeGrossMinor,providerMinor:b.providerStripeGrossMinor});
 }
 return {status:issues.length?'ISSUES':'PARTIAL',issues,totals,
  stripeHistoryComplete,monzoHistoryComplete,
  customerIdentityIndependentlyVerified:{stripe:stripeHistoryComplete,monzo:false},
  refundsIndependentlyVerified:false,customerBalanceVerified:false,
  reason:'Monzo account transactions do not independently identify BANKROLL customers; Stripe refunds and stored customer balances are not independently reconciled.'};
}
module.exports={verifyCustomerPayments};
