'use strict';
// Opt-in read-only live API probe. Never writes, credits, or changes provider state.
// Run manually in an isolated staging environment with real credentials.
require('dotenv').config();
const {verifyProviders}=require('../systems/primary-provider-verifier');
async function main(){
 if(process.env.BANKROLL_LIVE_API_TEST!=='I_AUTHORIZE_READ_ONLY_STAGING_TEST')throw Error('Live API test requires explicit opt-in');
 if(!process.env.DATA_DIR||!process.env.STRIPE_SECRET_KEY||!process.env.MONZO_ACCESS_TOKEN||!process.env.MONZO_ACCOUNT_ID)
  throw Error('DATA_DIR, STRIPE_SECRET_KEY, MONZO_ACCESS_TOKEN, MONZO_ACCOUNT_ID required');
 const Stripe=require('stripe');
 const result=await verifyProviders({
  dataDir:process.env.DATA_DIR,stripe:new Stripe(process.env.STRIPE_SECRET_KEY),
  monzoFetch:globalThis.fetch,monzoAccountId:process.env.MONZO_ACCOUNT_ID,
  monzoToken:process.env.MONZO_ACCESS_TOKEN
 });
 console.log(JSON.stringify({providers:result.results,customers:result.customers,
  confirmedMismatch:result.confirmedMismatch,fullyVerified:result.fullyVerified},null,2));
 if(!result.fullyVerified||result.customers.status==='ISSUES')process.exitCode=2;
}
if(require.main===module)main().catch(error=>{console.error('Read-only provider probe failed:',error.message);process.exitCode=1;});
module.exports={main};
