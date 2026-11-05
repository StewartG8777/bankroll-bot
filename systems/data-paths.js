'use strict';
const path = require('path');
function createDataPathHelpers(DATA_DIR) {
function relData(p) {
  if (!p) return null;
  const rel = path.relative(DATA_DIR, path.resolve(String(p)));
  return rel && !rel.startsWith('..') && !path.isAbsolute(rel) ? rel.split(path.sep).join('/') : null;
}
return { relData };
}
module.exports = { createDataPathHelpers };
