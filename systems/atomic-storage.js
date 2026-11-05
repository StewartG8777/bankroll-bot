'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
function atomicWriteFile(file, data) {
  const tmp = file + '.tmp-' + process.pid + '-' + crypto.randomBytes(8).toString('hex');
  try {
    fs.writeFileSync(tmp, data, { flag: 'wx', mode: 0o600 });
    fs.renameSync(tmp, file);
  } catch (error) {
    try { fs.unlinkSync(tmp); } catch {}
    throw error;
  }
}
function isSafeDatabaseName(name) { return /^[a-z0-9_.-]+\.json$/i.test(name) && path.basename(name) === name; }
module.exports = { atomicWriteFile, isSafeDatabaseName };
