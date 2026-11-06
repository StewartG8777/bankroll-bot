'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {assessPrimaryDeployment}=require('../systems/primary-deployment-preflight');
test('primary deployment preflight fails closed without explicit singleton verification',()=>{
 const config={BANKROLL_DRIVE_PRIMARY:'1',DISCORD_TOKEN:'test',BANKROLL_BACKUP_ENCRYPTION_KEY:'test'};
 const result=assessPrimaryDeployment(config);
 assert.equal(result.failed,1);
 assert.equal(result.distributedLockImplemented,false);
});
test('preflight accepts declared single writer but never claims distributed locking',()=>{
 const result=assessPrimaryDeployment({BANKROLL_DRIVE_PRIMARY:'1',BANKROLL_PRIMARY_SINGLE_INSTANCE_CONFIRMED:'I_HAVE_VERIFIED_ONE_INSTANCE',DISCORD_TOKEN:'test',BANKROLL_BACKUP_ENCRYPTION_KEY:'test'});
 assert.equal(result.failed,0);
 assert.equal(result.distributedLockImplemented,false);
});
