'use strict';
// Offline-only, guarded rollback. Keep the running Discord bot and payment
// webhooks STOPPED while this operates on its local DATA_DIR and Drive pointer.
async function verifiedOfflineRollback({ queue, store, generation, flush = async () => {}, confirm = false, reconcileAfterRollback, assertStopped } = {}) {
  if (!queue || !store || !generation) throw new Error('A storage queue, store and generation are required.');
  if (confirm !== true) throw new Error('Rollback requires an explicit confirmation.');
  // Historical rollback can omit newer payments. Require an explicit
  // reconciliation plan before pointer promotion; otherwise refuse.
  if(typeof reconcileAfterRollback!=='function')throw new Error('Rollback requires a post-restore payment reconciliation callback.');
  if (typeof assertStopped !== 'function' || !(await assertStopped())) throw new Error('Rollback refused: the bot must be stopped for maintenance.');
  return queue.withMaintenance(async () => {
    const target = await store.verifyGeneration(generation);
    // Save current local files as an immutable independent safety checkpoint.
    const safety = await store.snapshot({ flush });
    try {
      const applied = await store.restoreGeneration(generation);
      const reconciliation=await reconcileAfterRollback({targetGeneration:generation,safetyGeneration:safety.generation});
      if(reconciliation?.safeToPromote!==true){
        await store.restoreGeneration(safety.generation);
        throw new Error('Post-rollback payment reconciliation did not approve promotion; safety generation restored locally');
      }
      const promoted = await store.promoteGeneration(generation);
      return { restored: applied, promoted, safetyGeneration: safety.generation, verifiedFiles: target.files };
    } catch (error) {
      error.message += ' (Safety generation ' + safety.generation + ' remains available for recovery.)';
      throw error;
    }
  });
}
module.exports = { verifiedOfflineRollback };
