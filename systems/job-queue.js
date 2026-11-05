'use strict';

/** Bounded, in-process queue for non-critical jobs. Does not survive a process crash. */
function createJobQueue({ concurrency = 2, maxPending = 500, retryDelayMs = 1000 } = {}) {
  if (!Number.isInteger(concurrency) || concurrency < 1) throw new RangeError('Invalid concurrency');
  const pending = [];
  const active = new Set();
  let closed = false;
  const stats = { completed: 0, failed: 0, retried: 0 };
  function pump() {
    while (active.size < concurrency && pending.length) {
      const job = pending.shift();
      const work = (async () => {
        for (let attempt = 0; ; attempt++) {
          try {
            const result = await job.task();
            stats.completed++;
            job.resolve(result);
            break;
          } catch (error) {
            if (attempt >= job.retries) {
              stats.failed++;
              job.reject(error);
              break;
            }
            stats.retried++;
            await new Promise(resolve => setTimeout(resolve, retryDelayMs * Math.min(16, 2 ** attempt)));
          }
        }
      })();
      active.add(work);
      work.finally(() => { active.delete(work); pump(); });
    }
  }
  return {
    enqueue(task, { retries = 2 } = {}) {
      if (closed) return Promise.reject(new Error('Job queue is closed'));
      if (typeof task !== 'function') return Promise.reject(new TypeError('Job must be a function'));
      if (pending.length >= maxPending) return Promise.reject(new Error('Job queue is full'));
      return new Promise((resolve, reject) => { pending.push({ task, retries, resolve, reject }); pump(); });
    },
    status() { return { pending: pending.length, active: active.size, ...stats }; },
    close() { closed = true; },
  };
}
module.exports = { createJobQueue };
