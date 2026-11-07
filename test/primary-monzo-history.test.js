'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {scanMonzoHistory}=require('../systems/primary-monzo-history');
const day=86400000;
function fakeFetch(transactions){
 return async url=>{
  const u=new URL(url),since=Date.parse(u.searchParams.get('since')),before=Date.parse(u.searchParams.get('before'));
  return {ok:true,json:async()=>({transactions:transactions.filter(x=>x.created>=since&&x.created<before).slice(0,100)})};
 };
}
const tx=(id,amount,created)=>({id,amount,created,account_id:'acc',currency:'GBP'});
test('scans history and reports unmatched incoming credits for review, not confirmed sales',async()=>{
 const result=await scanMonzoHistory({fetch:fakeFetch([tx('known',5000,1000),tx('other',2500,2000),tx('out',-100,3000)]),
 token:'token',accountId:'acc',saved:{one:{status:'paid',txId:'known',amount:50,currency:'gbp'}},since:0,until:day});
 assert.equal(result.status,'PARTIAL');
 assert.deepEqual(result.unclassifiedIncoming.map(x=>x.id),['other']);
 assert.equal(result.unseenSavedIds.length,0);
});
test('splits saturated windows without dropping transactions',async()=>{
 const transactions=Array.from({length:110},(_,i)=>tx('tx'+i,100,1000+i*1000));
 const result=await scanMonzoHistory({fetch:fakeFetch(transactions),token:'token',accountId:'acc',saved:{},since:0,until:day});
 assert.equal(result.unclassifiedIncoming.length,110);
 assert.ok(result.requests>1);
});
test('API failure is not reported as successful scan',async()=>{
 await assert.rejects(()=>scanMonzoHistory({fetch:async()=>({ok:false,status:401}),token:'t',accountId:'acc',saved:{},since:0,until:day}),/401/);
});
