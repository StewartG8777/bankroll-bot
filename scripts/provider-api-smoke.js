'use strict';
// Read-only connectivity smoke test. No payment creation, ledger writes or customer data logging.
const Stripe=require('stripe');
async function main(){
 const key=process.env.STRIPE_SECRET_KEY,token=process.env.MONZO_ACCESS_TOKEN,account=process.env.MONZO_ACCOUNT_ID;
 if(!key||!token||!account)throw Error('Missing required GitHub Actions secrets: STRIPE_SECRET_KEY, MONZO_ACCESS_TOKEN, MONZO_ACCOUNT_ID');
 if(!/^sk_(test|live)_/.test(key))throw Error('Stripe secret key must be sk_test_ or sk_live_');
 const stripe=new Stripe(key,{maxNetworkRetries:1,timeout:15000});
 const page=await stripe.paymentIntents.list({limit:1});
 if(!Array.isArray(page?.data)||typeof page.has_more!=='boolean')throw Error('Invalid Stripe PaymentIntent response');
 console.log('Stripe: authenticated, read-only PaymentIntent listing succeeded ('+(key.startsWith('sk_live_')?'live':'test')+' mode)');
 const url='https://api.monzo.com/transactions?'+new URLSearchParams({account_id:account,limit:'1'});
 const response=await fetch(url,{headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(15000)});
 if(!response.ok)throw Error('Monzo read-only transaction listing failed: HTTP '+response.status);
 const body=await response.json();
 if(!Array.isArray(body.transactions))throw Error('Invalid Monzo transaction listing response');
 if(body.transactions.some(tx=>String(tx.account_id)!==String(account)))throw Error('Monzo returned transactions for unexpected account');
 console.log('Monzo: authenticated, read-only account transaction listing succeeded');
 console.log('Connectivity smoke test passed; this does NOT verify payment matching, historical coverage, refunds or webhook delivery.');
}
main().catch(e=>{console.error('Provider smoke test failed:',e.message);process.exitCode=1;});
