'use strict';
// Read-only customer takings projection, never a claim about account balances.
// All monetary values are integer minor units, keyed by currency and customer.
function customerPaymentTotals({stripePayments={},monzoPayments={}}){
 const totals={},issues=[];
 const add=(id,currency,minor,provider)=>{
  if(!id){issues.push({type:'unattributed-payment',provider});return;}
  if(!Number.isSafeInteger(minor)){issues.push({type:'invalid-amount',provider});return;}
  const customer=totals[id]??={};
  const bucket=customer[currency]??={stripeGrossMinor:0,stripeRefundedMinor:0,monzoGrossMinor:0,netReceivedMinor:0};
  bucket[provider]+=minor;
 };
 for(const [key,p] of Object.entries(stripePayments)){
  if(!p||p.id!==key||!Number.isSafeInteger(p.amount)||p.amount<0){issues.push({type:'invalid-stripe-record',id:key});continue;}
  const refunded=Number(p.refunded||0);
  if(!Number.isSafeInteger(refunded)||refunded<0||refunded>p.amount){issues.push({type:'invalid-stripe-refund',id:key});continue;}
  const currency=String(p.currency||'').toLowerCase();
  if(!currency){issues.push({type:'missing-currency',provider:'stripe',id:key});continue;}
  add(String(p.customerId||''),currency,p.amount,'stripeGrossMinor');
  if(refunded)add(String(p.customerId||''),currency,refunded,'stripeRefundedMinor');
 }
 const unique=new Map();
 for(const [key,p] of Object.entries(monzoPayments)){
  if(!p||p.status!=='paid')continue;
  const identity=p.txId?'tx:'+String(p.txId):'key:'+key;
  const isImport=key==='monzo_import_'+String(p.txId||'')||String(p.source||'').includes('historical transaction');
  if(!unique.has(identity)||isImport&&!unique.get(identity).isImport)unique.set(identity,{p,key,isImport});
 }
 for(const {p,key} of unique.values()){
  const amount=Number(p.amount);
  const minor=Math.round(amount*100);
  if(!Number.isFinite(amount)||amount<0||!Number.isSafeInteger(minor)||Math.abs(amount*100-minor)>0.000001){issues.push({type:'invalid-monzo-amount',id:key});continue;}
  const currency=String(p.currency||'').toLowerCase();
  if(!currency){issues.push({type:'missing-currency',provider:'monzo',id:key});continue;}
  add(String(p.customerId||''),currency,minor,'monzoGrossMinor');
 }
 for(const currencies of Object.values(totals))for(const bucket of Object.values(currencies)){
  bucket.netReceivedMinor=bucket.stripeGrossMinor-bucket.stripeRefundedMinor+bucket.monzoGrossMinor;
 }
 return {totals,issues,complete:issues.length===0};
}
module.exports={customerPaymentTotals};
