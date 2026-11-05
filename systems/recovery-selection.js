'use strict';
function selectRecoveryPoint(points, { id, at } = {}) {
  if (!Array.isArray(points)) throw new TypeError('Recovery points must be an array');
  const eligible = points.filter(p => p && typeof p.id === 'string' && Number.isFinite(Date.parse(p.createdAt)));
  if (id) return eligible.find(p => p.id === id) || null;
  let cutoff = Infinity;
  if (at !== undefined) {
    cutoff = Date.parse(at);
    if (!Number.isFinite(cutoff)) throw new RangeError('Invalid recovery timestamp');
  }
  return eligible.filter(p => Date.parse(p.createdAt) <= cutoff)
    .sort((a,b) => Date.parse(b.createdAt) - Date.parse(a.createdAt))[0] || null;
}
module.exports = { selectRecoveryPoint };
