'use strict';
function createDownloadLifecycle({ record, shouldConsume, persist, onCompleted, onInterrupted }) {
  let state = 'ready';
  return {
    async reserve() {
      if (state !== 'ready') return false;
      if (shouldConsume(record)) {
        record.used = true;
        try { await persist(); } catch (error) { record.used = false; throw error; }
      }
      state = 'reserved';
      return true;
    },
    async completed() {
      if (state !== 'reserved') return false;
      state = 'completed';
      await onCompleted();
      return true;
    },
    async interrupted() {
      if (state !== 'reserved') return false;
      state = 'interrupted';
      if (shouldConsume(record)) record.used = false;
      await onInterrupted();
      return true;
    },
    get state() { return state; },
  };
}
module.exports = { createDownloadLifecycle };
