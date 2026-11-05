'use strict';
/**
 * Serializes per-key writes, remembers failures and permits an awaited barrier.
 * A failed write never counts as committed. Newer writes may supersede failures,
 * but an earlier failed write cannot silently be reported as successful.
 */
function createDriveCommitQueue({ write, onFailure = () => {} }) {
  if (typeof write !== 'function') throw new TypeError('write must be a function');
  const pending = new Map();
  const failed = new Map();
  function enqueue(key, snapshot) {
    const previous = pending.get(key) || Promise.resolve();
    const next = previous.catch(() => {}).then(() => write(key, snapshot));
    pending.set(key, next);
    next.then(() => failed.delete(key), error => {
      failed.set(key, error);
      onFailure(key, error);
    }).finally(() => {
      if (pending.get(key) === next) pending.delete(key);
    });
    return next;
  }
  async function flush() {
    while (pending.size) {
      const batch = [...pending.values()];
      await Promise.allSettled(batch);
    }
    if (failed.size) {
      throw new AggregateError([...failed.values()], 'Uncommitted Google Drive database writes: ' + [...failed.keys()].join(', '));
    }
  }
  return { enqueue, flush, pending, failed };
}
module.exports = { createDriveCommitQueue };
