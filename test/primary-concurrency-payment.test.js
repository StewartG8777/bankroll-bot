'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {createSerializedStorageQueue}=require('../systems/serialized-storage-queue');
const {createPrimaryCommitManager}=require('../systems/primary-commit-manager');
const {protectInteractionDurability,primaryWebhookGate}=require('../systems/primary-ack-guards');

test('concurrent writes execute strictly in arrival order',async()=>{
 const queue=createSerializedStorageQueue(),trace=[];
 await Promise.all(Array.from({length:25},(_,i)=>queue.enqueue(async()=>{
  trace.push('start-'+i);
  await new Promise(resolve=>setImmediate(resolve));
  trace.push('end-'+i);
 })));
 assert.deepEqual(trace,Array.from({length:25},(_,i)=>['start-'+i,'end-'+i]).flat());
 assert.equal(queue.status().pending,0);
});
test('maintenance drains existing writes, rejects incoming writes and releases lock on failure',async()=>{
 const queue=createSerializedStorageQueue(),trace=[];
 const first=queue.enqueue(async()=>{await new Promise(resolve=>setImmediate(resolve));trace.push('write');});
 const maintenance=queue.withMaintenance(async()=>{trace.push('maintenance');throw Error('rollback failed');});
 await assert.rejects(queue.enqueue(async()=>trace.push('unexpected')),/maintenance/i);
 await first;
 await assert.rejects(maintenance,/rollback failed/);
 await queue.enqueue(async()=>trace.push('recovered'));
 assert.deepEqual(trace,['write','maintenance','recovered']);
 assert.equal(queue.status().maintenance,false);
});
test('concurrent durable flushes coalesce, failed snapshot stays pending, retry commits',async()=>{
 let count=0,fail=true;
 const manager=createPrimaryCommitManager({store:{snapshot:async()=>{count++;await new Promise(resolve=>setImmediate(resolve));if(fail)throw Error('offline');}},onError:()=>{}});
 manager.markDirty();
 await assert.rejects(manager.flush(),/not ready/i);
 manager.activate();
 const results=await Promise.allSettled(Array.from({length:8},()=>manager.flush()));
 assert.ok(results.every(x=>x.status==='rejected'));
 assert.ok(manager.status().pending>0);
 fail=false;
 await Promise.all(Array.from({length:8},()=>manager.flush()));
 assert.equal(manager.status().pending,0);
 assert.ok(count>=2);
});
for(const route of ['/webhook','/monzo-webhook']){
 test(route+' never acknowledges a failed Drive commit as success',async()=>{
  const manager={status:()=>({active:true}),flush:async()=>{throw Error('Drive offline');}};
  const gate=primaryWebhookGate(manager,{error(){}});
  const response={statusCode:200,headersSent:false,status(n){this.statusCode=n;return this;},removeHeader(){},send(v){this.body=v;return this;}};
  gate({method:'POST',path:route},response,()=>response.send('success'));
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(response.statusCode,503);
  assert.match(response.body,/commit failed/i);
 });
}
test('Discord acknowledgement is replaced with error on failed commit',async()=>{
 const sent=[],manager={flush:async()=>{throw Error('offline')}};
 const interaction={reply:async payload=>sent.push(payload)};
 protectInteractionDurability(interaction,manager,{error(){}});
 await interaction.reply({content:'Payment received'});
 assert.equal(sent.length,1);
 assert.doesNotMatch(sent[0].content,/Payment received/);
});
