'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {protectInteractionDurability,primaryWebhookGate}=require('../systems/primary-ack-guards');
const logger={error(){}};
function response(){
  return {statusCode:200,headersSent:false,body:null,
    status(n){this.statusCode=n;return this;},
    send(v){this.body=v;this.headersSent=true;return this;},
    removeHeader(){},destroy(e){this.destroyed=e;}
  };
}
test('payment success waits for Drive acknowledgement',async()=>{
  let resolve;const manager={status:()=>({active:true}),flush:()=>new Promise(r=>resolve=r)};
  const res=response();
  primaryWebhookGate(manager,logger)({method:'POST',path:'/webhook'},res,()=>res.send('OK'));
  assert.equal(res.body,null);resolve();await new Promise(setImmediate);
  assert.equal(res.statusCode,200);assert.equal(res.body,'OK');
});
test('Drive outage returns retryable 503 instead of 2xx webhook success',async()=>{
  const manager={status:()=>({active:true}),flush:()=>Promise.reject(Error('Drive down'))};
  const res=response();
  primaryWebhookGate(manager,logger)({method:'POST',path:'/monzo-webhook'},res,()=>res.send('OK'));
  await new Promise(setImmediate);
  assert.equal(res.statusCode,503);assert.match(res.body,/retry webhook/);
});
test('primary startup blocks webhooks until recovery active',()=>{
  const res=response();
  primaryWebhookGate({status:()=>({active:false})},logger)({method:'POST',path:'/webhook'},res,()=>{throw Error('should not handle');});
  assert.equal(res.statusCode,503);
});
test('nonpayment endpoints pass unchanged',()=>{
  let called=false;const manager={status:()=>({active:false})};
  primaryWebhookGate(manager,logger)({method:'GET',path:'/health'},response(),()=>{called=true;});
  assert.ok(called);
});
test('Discord command success is replaced by a clear error when Drive commit fails',async()=>{
  let result;
  const interaction={async editReply(payload){result=payload;return payload;}};
  protectInteractionDurability(interaction,{flush:()=>Promise.reject(Error('Drive offline'))},logger);
  await interaction.editReply('Payment recorded');
  assert.match(result.content,/could not confirm/i);
});
test('Discord command success is delayed until Drive confirms the save',async()=>{
  let resolve,output=null;
  const interaction={async reply(payload){output=payload;}};
  protectInteractionDurability(interaction,{flush:()=>new Promise(r=>resolve=r)},logger);
  const pending=interaction.reply('Success');
  assert.equal(output,null);resolve();await pending;
  assert.equal(output,'Success');
});
