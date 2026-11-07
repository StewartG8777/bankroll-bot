'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs/promises'),os=require('node:os'),path=require('node:path');
const {validateEnvironment,summarize}=require('../scripts/verify-live-providers');
async function fixture(t){
 const dir=await fs.mkdtemp(path.join(os.tmpdir(),'bankroll-live-probe-'));
 t.after(()=>fs.rm(dir,{recursive:true,force:true}));
 await fs.writeFile(path.join(dir,'stripe_ledger.json'),'{"payments":{}}');
 await fs.writeFile(path.join(dir,'monzo_payments.json'),'{}');
 return {DATA_DIR:dir,BANKROLL_LIVE_API_TEST:'I_AUTHORIZE_READ_ONLY_STAGING_TEST',
 STRIPE_SECRET_KEY:'sk_test_fake',MONZO_ACCESS_TOKEN:'fake',MONZO_ACCOUNT_ID:'acc_fake',
 BANKROLL_MONZO_HISTORY_SINCE:'2024-01-01T00:00:00Z'};
}
test('read-only API probe requires explicit authorization',async t=>{
 const env=await fixture(t);delete env.BANKROLL_LIVE_API_TEST;
 await assert.rejects(validateEnvironment(env),/authorization/);
});
test('probe rejects live primary mode and missing history boundary',async t=>{
 const env=await fixture(t);env.BANKROLL_DRIVE_PRIMARY='1';
 await assert.rejects(validateEnvironment(env),/primary-mode/);
 delete env.BANKROLL_DRIVE_PRIMARY;delete env.BANKROLL_MONZO_HISTORY_SINCE;
 await assert.rejects(validateEnvironment(env),/HISTORY_SINCE/);
});
test('live Stripe key requires separate read-only confirmation',async t=>{
 const env=await fixture(t);env.STRIPE_SECRET_KEY='sk_live_fake';
 await assert.rejects(validateEnvironment(env),/separate explicit/);
 env.BANKROLL_ALLOW_LIVE_STRIPE_READONLY='I_CONFIRM_LIVE_READ_ONLY';
 assert.equal(await validateEnvironment(env),env.DATA_DIR);
});
test('staging ledgers must be valid JSON',async t=>{
 const env=await fixture(t);
 await fs.writeFile(path.join(env.DATA_DIR,'monzo_payments.json'),'broken');
 await assert.rejects(validateEnvironment(env),SyntaxError);
});
test('report excludes transaction IDs and customer details',()=>{
 const report=summarize({results:{stripe:{status:'MISMATCH',issues:[{id:'secret-pi'}]},
 monzo:{status:'PARTIAL',unclassifiedIncoming:[{id:'secret-tx'}],unseenSavedIds:['secret-tx']}},
 customers:{status:'ISSUES',issues:[{customer:'private'}]},confirmedMismatch:true,fullyVerified:false});
 assert.equal(report.providers.monzo.unclassifiedIncoming,1);
 assert.equal(report.providers.stripe.issues,1);
 assert.doesNotMatch(JSON.stringify(report),/secret-pi|secret-tx|private/);
});
