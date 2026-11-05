'use strict';
// One in-process queue for primary snapshot/maintenance operations. This is not
// a cross-instance lock; callers must stop other bot instances for rollback.
function createSerializedStorageQueue() {
  let tail = Promise.resolve();
  let maintenance = false;
  let pending = 0;
  function enqueue(job) {
    if (typeof job !== 'function') return Promise.reject(new TypeError('Storage queue requires a function.'));
    if (maintenance) return Promise.reject(new Error('Storage writes are blocked by maintenance mode.'));
    pending++;
    const result = tail.then(job, job);
    tail = result.then(() => {}, () => {}).finally(() => { pending--; });
    return result;
  }
  function withMaintenance(operation) {
    if (typeof operation !== 'function') return Promise.reject(new TypeError('Maintenance requires a function.'));
    if (maintenance) return Promise.reject(new Error('Maintenance mode is already active.'));
    maintenance = true; // reject new writes immediately, then drain queued writes
    const result = tail.then(operation, operation);
    tail = result.then(() => {}, () => {});
    return result.finally(() => { maintenance = false; });
  }
  return { enqueue, withMaintenance, drain: () => tail, status: () => ({ maintenance, pending }) };
}
module.exports = { createSerializedStorageQueue };
