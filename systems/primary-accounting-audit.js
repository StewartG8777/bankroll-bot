'use strict';
// Read-only, schema-aware payment accounting checks. Never contacts payment APIs,
// changes payment status, or invents a balance from a non-accounting vault record.
const fs=require('node:fs/promises');
const path=require('node:path');
async function auditRecoveredAccounting({dataDir}){
 const read=async name=>JSON.parse(await fs.readFile(path.join(dataDir,name),'utf8'));
 const issues=[];
 const stripe=await read('stripe_ledger.json');
 const monzo=await read('monzo_payments.json');
 const deliveries=await read('deliveries.json');
 const vault=await read('vault.json');
 if(!stripe||typeof stripe.payments!=='object'||!stripe.payments||Array.isArray(stripe.payments))issues.push('Stripe payments map missing');
 if(!monzo||typeof monzo!=='object'||Array.isArray(monzo))issues.push('Monzo ledger must be a keyed object');
 if(!deliveries||typeof deliveries!=='object'||Array.isArray(deliveries))issues.push('Deliveries must be a keyed object');
 if(!vault||typeof vault!=='object'||Array.isArray(vault))issues.push('Vault must be a keyed object');
 const totals={stripeMinorUnitsByCurrency:{},monzoMajorUnitsByCurrency:{},stripePayments:0,monzoPaidTransactions:0,deliveryRecords:Object.keys(deliveries||{}).length,vaultRecords:Object.keys(vault||{}).length};
 if(!issues.length){
  for(const [key,p] of Object.entries(stripe.payments)){
   if(!p||typeof p!=='object'||p.id!==key||!Number.isSafeInteger(p.amount)||p.amount<0||typeof p.currency!=='string'||!p.currency)issues.push('Invalid Stripe payment '+key);
   else{totals.stripePayments++;totals.stripeMinorUnitsByCurrency[p.currency]=(totals.stripeMinorUnitsByCurrency[p.currency]||0)+p.amount;}
  }
  const unique=new Map();
  for(const [key,p] of Object.entries(monzo)){
   if(!p||typeof p!=='object'){issues.push('Invalid Monzo record '+key);continue;}
   if(p.status!=='paid')continue;
   if(!Number.isFinite(p.amount)||p.amount<0||!p.currency){issues.push('Invalid paid Monzo amount '+key);continue;}
   const identity=p.txId?'tx:'+String(p.txId):'key:'+key;
   const prior=unique.get(identity);
   const isImport=key==='monzo_import_'+String(p.txId||'')||String(p.source||'').includes('historical transaction');
   if(!prior||isImport&&!prior.isImport)unique.set(identity,{p,isImport});
  }
  for(const {p} of unique.values()){
   totals.monzoPaidTransactions++;
   const currency=String(p.currency).toLowerCase();
   totals.monzoMajorUnitsByCurrency[currency]=(totals.monzoMajorUnitsByCurrency[currency]||0)+p.amount;
  }
 }
 const {customerPaymentTotals}=require('./primary-customer-reconciliation');
 const customerReconciliation=customerPaymentTotals({
  stripePayments:stripe?.payments||{},monzoPayments:monzo||{}
 });
 // Unattributed historical payments are reported as incomplete, not invented
 // customer balances. Keep legacy startup schema behavior unchanged.
 return {passed:issues.length===0,issues,totals,
  customerReconciliation,externalProviderVerified:false,
  customerBalancesRecomputed:false};
}
module.exports={auditRecoveredAccounting};
