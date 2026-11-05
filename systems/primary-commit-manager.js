'use strict';
// Coordinates all local persistent mutations into ordered, confirmed Drive
// generations. Call markDirty after a local mutation and await flush() BEFORE
// reporting that mutation to an external caller as durably saved.
function createPrimaryCommitManager({ store, flushLocal = () => {}, onError = console.error } = {}) {
  if (!store || typeof store.snapshot !== 'function') throw new TypeError('A primary Drive snapshot store is required.');
  let active = false, version = 0, committed = 0, inFlight = null, lastError = null;
  let scheduled = false;
  function setFlushLocal(fn) {
    if (typeof fn !== 'function') throw new TypeError('flushLocal must be callable.');
    flushLocal = fn;
  }
  function markDirty() {
    version++;
    if (active && !scheduled) {
      scheduled = true;
      queueMicrotask(() => {
        scheduled = false;
        void flush().catch(error => {
          try { onError(error); } catch {}
        });
      });
    }
    return version;
  }
  async function commit() {
    while (committed < version) {
      // Draining debounced local files may itself create additional mutations.
      await flushLocal();
      const boundary = version;
      try {
        await store.snapshot({ allowInitialize: false });
        committed = boundary;
        lastError = null;
      } catch (error) {
        lastError = error;
        throw error;
      }
    }
  }
  function flush() {
    if (!active) return Promise.reject(new Error('Primary storage is not ready; refusing to acknowledge a write.'));
    if (committed === version && !inFlight) return Promise.resolve();
    if (!inFlight) {
      inFlight = commit().finally(() => { inFlight = null; });
    }
    return inFlight.then(() => committed === version ? undefined : flush());
  }
  function activate() {
    active = true;
    // A pre-ready data mutation must be included before any success response.
    if (version > committed) void flush().catch(error => { try { onError(error); } catch {} });
  }
  function status() {
    return { active, version, committed, pending: version - committed,
      lastError: lastError ? String(lastError.message || lastError) : null };
  }
  return { setFlushLocal, markDirty, flush, activate, status };
}
module.exports = { createPrimaryCommitManager };
