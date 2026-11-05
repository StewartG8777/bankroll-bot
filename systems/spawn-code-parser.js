'use strict';
const path = require('path');
function zipSpawnCodes(zip) {
  const entries = zip.getEntries().filter((e) => !e.isDirectory);
  const found = new Set();
  for (const e of entries) {
    const base = path.posix.basename(e.entryName.replace(/\\/g, '/')).toLowerCase();
    if (!/^vehicles.*\.meta$/.test(base)) continue;
    const text = e.getData().toString('utf8');
    for (const m of text.matchAll(/<modelName>\s*([^<\s]+)\s*<\/modelName>/gi)) found.add(m[1].toLowerCase());
  }
  if (!found.size) {
    for (const e of entries) {
      const m = path.posix.basename(e.entryName.replace(/\\/g, '/')).toLowerCase().match(/^([a-z0-9_]+?)(?:_hi)?\.yft$/);
      if (m) found.add(m[1]);
    }
  }
  return [...found];
}
module.exports = { zipSpawnCodes };
