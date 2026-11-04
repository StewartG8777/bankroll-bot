'use strict';

const { monitorEventLoopDelay } = require('node:perf_hooks');

const histogram = monitorEventLoopDelay({ resolution: 20 });
histogram.enable();

function finiteMs(value) {
  return Number.isFinite(value) && value > 0 ? value / 1e6 : null;
}

function getPerformanceSnapshot() {
  const memory = process.memoryUsage();
  return {
    capturedAt: Date.now(),
    memory: {
      rss: memory.rss,
      heapTotal: memory.heapTotal,
      heapUsed: memory.heapUsed,
      external: memory.external,
      arrayBuffers: memory.arrayBuffers || 0,
    },
    eventLoop: {
      meanMs: finiteMs(histogram.mean),
      p99Ms: finiteMs(histogram.percentile(99)),
      maxMs: finiteMs(histogram.max),
    },
  };
}

module.exports = { getPerformanceSnapshot };
