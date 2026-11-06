'use strict';
// Production migration preflight. No mutation, no Drive writes, no payments.
// Verifies that an operator explicitly configured a single-instance deployment;
// Google Drive does NOT offer an atomic CAS lock through ordinary file APIs.
function assessPrimaryDeployment(env=process.env){
 const checks=[
  {name:'Primary feature flag',pass:env.BANKROLL_DRIVE_PRIMARY==='1',detail:'BANKROLL_DRIVE_PRIMARY must equal 1'},
  {name:'Single-instance deployment acknowledgement',pass:env.BANKROLL_PRIMARY_SINGLE_INSTANCE_CONFIRMED==='I_HAVE_VERIFIED_ONE_INSTANCE',detail:'Operator must verify exactly one live writer; this is NOT a distributed lock'},
  {name:'Independent backup encryption',pass:!!env.BANKROLL_BACKUP_ENCRYPTION_KEY,detail:'BANKROLL_BACKUP_ENCRYPTION_KEY must be set for credential recovery'},
  {name:'Bot token for credential recovery',pass:!!env.DISCORD_TOKEN,detail:'DISCORD_TOKEN must be available for Discord backup recovery'}
 ];
 return {checks,passed:checks.filter(x=>x.pass).length,failed:checks.filter(x=>!x.pass).length,distributedLockImplemented:false};
}
module.exports={assessPrimaryDeployment};
