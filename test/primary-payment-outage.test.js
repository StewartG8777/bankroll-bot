'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {paymentOutageGate,isReadOnlyInteraction}=require('../systems/primary-payment-outage');
test('payment webhook outage returns retryable 503 without invoking handler',()=>{
 let next=false,status,body,headers={};
 const res={setHeader:(k,v)=>headers[k]=v,status:n=>{status=n;return res;},send:v=>{body=v;return res;}};
 paymentOutageGate({degraded:true})({path:'/webhook',method:'POST'},res,()=>next=true);
 assert.equal(next,false);assert.equal(status,503);assert.equal(headers['Retry-After'],'300');assert.match(body,/retry/i);
});
test('Monzo webhook blocked but health GET proceeds',()=>{
 const res={setHeader:()=>{},status:()=>res,send:()=>res};let calls=0;
 const gate=paymentOutageGate({degraded:true});
 gate({path:'/monzo-webhook',method:'POST'},res,()=>calls++);
 gate({path:'/healthz',method:'GET'},res,()=>calls++);
 assert.equal(calls,1);
});
test('only explicitly allowed read-only commands proceed in degraded mode',()=>{
 const make=commandName=>({commandName,isChatInputCommand:()=>true});
 assert.equal(isReadOnlyInteraction(make('help')),true);
 assert.equal(isReadOnlyInteraction(make('ping')),true);
 assert.equal(isReadOnlyInteraction(make('payments')),false);
 assert.equal(isReadOnlyInteraction({commandName:'help',isChatInputCommand:()=>false}),false);
});
