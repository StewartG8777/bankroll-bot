'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {verifiedOfflineRollback}=require('../systems/verified-drive-rollback');
function fixture(){
 const calls=[],store={
  verifyGeneration:async()=>{calls.push('verify');return {files:3};},
  snapshot:async()=>{calls.push('safety');return {generation:'safety'};},
  restoreGeneration:async g=>{calls.push('restore:'+g);return {generation:g};},
  promoteGeneration:async g=>{calls.push('promote:'+g);return {generation:g};}
 };
 const queue={withMaintenance:async fn=>{calls.push('maintenance');return fn();}};
 return {calls,store,queue};
}
test('rollback refuses to run while bot is active',async()=>{
 const f=fixture();
 await assert.rejects(verifiedOfflineRollback({...f,generation:'old',confirm:true,assertStopped:async()=>false,
 reconcileAfterRollback:async()=>({safeToPromote:true})}),/stopped/);
 assert.deepEqual(f.calls,[]);
});
test('rollback refuses missing payment reconciliation callback',async()=>{
 const f=fixture();
 await assert.rejects(verifiedOfflineRollback({...f,generation:'old',confirm:true,assertStopped:async()=>true}),/reconciliation callback/);
 assert.deepEqual(f.calls,[]);
});
test('rollback rejected by payment audit restores safety state and does not promote pointer',async()=>{
 const f=fixture();
 await assert.rejects(verifiedOfflineRollback({...f,generation:'old',confirm:true,assertStopped:async()=>true,
 reconcileAfterRollback:async()=>({safeToPromote:false})}),/did not approve/);
 assert.deepEqual(f.calls,['maintenance','verify','safety','restore:old','restore:safety']);
});
test('rollback promotes only after independent reconciliation approval',async()=>{
 const f=fixture();
 await verifiedOfflineRollback({...f,generation:'old',confirm:true,assertStopped:async()=>true,
 reconcileAfterRollback:async()=>({safeToPromote:true})});
 assert.deepEqual(f.calls,['maintenance','verify','safety','restore:old','promote:old']);
});
