'use strict';
const { Transform } = require('node:stream');
const crypto = require('node:crypto');
function createIntegrityMeter({ maxBytes = Number.MAX_SAFE_INTEGER } = {}) {
  let bytes = 0;
  const hash = crypto.createHash('sha256');
  const meter = new Transform({
    transform(chunk, _encoding, callback) {
      bytes += chunk.length;
      if (bytes > maxBytes) return callback(new Error('STREAM_SIZE_LIMIT'));
      hash.update(chunk);
      callback(null, chunk);
    }
  });
  meter.result = () => ({ size: bytes, sha256: hash.copy().digest('hex') });
  return meter;
}
module.exports = { createIntegrityMeter };
