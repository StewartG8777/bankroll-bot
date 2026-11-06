'use strict';
// Exhaustive Stripe PaymentIntent scan. No fixed 2000-record cap.
// Never classify a truncated/failed scan as an authoritative mismatch.
async function scanStripeHistory({stripe,saved,maxPages=100000}){
 if(!stripe?.paymentIntents?.list)throw Error('Stripe payment listing unavailable');
 const found=new Map(),issues=[];
 let cursor,complete=false,pages=0;
 while(!complete){
  if(++pages>maxPages)throw Error('Stripe history exceeds configured page safety limit');
  const page=await stripe.paymentIntents.list({limit:100,...(cursor?{starting_after:cursor}:{})});
  if(!page||!Array.isArray(page.data)||typeof page.has_more!=='boolean')throw Error('Malformed Stripe page');
  if(page.has_more&&page.data.length===0)throw Error('Stripe returned empty nonterminal page');
  for(const pi of page.data){
   if(!pi?.id)throw Error('Stripe payment without ID');
   if(found.has(pi.id))throw Error('Duplicate Stripe pagination ID');
   found.set(pi.id,pi);
  }
  complete=!page.has_more;
  if(!complete){
   const next=page.data.at(-1).id;
   if(next===cursor)throw Error('Stripe pagination did not advance');
   cursor=next;
  }
 }
 // Match the bot's existing rule: only succeeded PaymentIntents
 // bearing agent_id are included in its Stripe ledger.
 for(const [id,pi] of found){
  if(pi.status!=='succeeded'||!pi.metadata?.agent_id)continue;
  const local=saved[id];
  if(!local){issues.push({type:'missing-local-payment',id});continue;}
  if(Number(local.amount)!==Number(pi.amount_received??pi.amount))issues.push({type:'amount-mismatch',id});
  if(String(local.currency).toLowerCase()!==String(pi.currency).toLowerCase())issues.push({type:'currency-mismatch',id});
  if(String(local.customerId||'')!==String(pi.metadata?.customer_id||''))issues.push({type:'customer-mismatch',id});
 }
 for(const id of Object.keys(saved)){
  const pi=found.get(id);
  if(!pi)issues.push({type:'missing-provider-payment',id});
  else if(pi.status!=='succeeded')issues.push({type:'status-mismatch',id});
 }
 return {status:issues.length?'MISMATCH':'VERIFIED',issues,verified:found.size,pages,complete:true};
}
module.exports={scanStripeHistory};
