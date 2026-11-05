'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {createPrimaryCommitManager}=require('../systems/primary-commit-manager');
test('primary startup blocks acknowledgements until storage is active',async()=>{
  const m=createPrimaryCommitManager({store:{snapshot:async()=>{}},onError:()=>{}});
  m.markDirty();
  await assert.rejects(m.flush(),/not ready/);
  m.activate(); await m.flush();
  assert.equal(m.status().pending,0);
});
test('multiple writes including debounced flush are committed in serialized generations',async()=>{
  let inProgress=0,max=0,commits=0;
  const m=createPrimaryCommitManager({
    store:{snapshot:async()=>{max=Math.max(max,++inProgress);await new Promise(r=>setTimeout(r,4));commits++;inProgress--;}},
    onError:()=>{}
  });
  m.activate();m.markDirty();m.markDirty();m.markDirty();
  await Promise.all([m.flush(),m.flush(),m.flush()]);
  assert.equal(max,1);assert.equal(m.status().pending,0);assert.ok(commits>=1);
});
test('flushLocal generated mutation must be durably committed before ack',async()=>{
  let called=0,local=true;
  const m=createPrimaryCommitManager({
    store:{snapshot:async()=>{called++;}},
    flushLocal:()=>{if(local){local=false;m.markDirty();}},onError:()=>{}
  });
  m.activate();m.markDirty();await m.flush();
  assert.ok(called>=1);assert.equal(m.status().committed,2);
});
test('failure does not acknowledge local writes and retries successfully',async()=>{
  let failing=true;
  const m=createPrimaryCommitManager({
    store:{snapshot:async()=>{if(failing)throw Error('Drive unavailable');}},onError:()=>{}
  });
  m.activate();m.markDirty();
  await assert.rejects(m.flush(),/Drive unavailable/);
  assert.equal(m.status().committed,0);
  assert.ok(m.status().lastError);
  failing=false;await m.flush();
  assert.equal(m.status().pending,0);assert.equal(m.status().lastError,null);
});
