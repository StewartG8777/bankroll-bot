'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {runIsolatedProcessRecovery}=require('../systems/drive-migration-test');
test('disposable recovery is validated by a separate Node process',async()=>{
 const detail=await runIsolatedProcessRecovery();
 assert.match(detail,/Fresh Node process recovered/);
});
