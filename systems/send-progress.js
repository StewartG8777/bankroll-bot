'use strict';

function randomSearchGb(durationMinutes) {
  const center = Math.max(2, 2 + ((durationMinutes - 2) * 198) / 118);
  const jitter = 0.82 + Math.random() * 0.36;
  return Math.max(2, Math.round(center * jitter));
}
module.exports = { randomSearchGb };
