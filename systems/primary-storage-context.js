'use strict';
let state = null;
function installPrimaryContext(context) {
  if (state) throw new Error('Primary storage context has already been installed.');
  if (!context?.manager || !context?.store) throw new TypeError('Primary storage manager and store required.');
  state = context;
}
function getPrimaryContext() { return state; }
function requirePrimaryContext() {
  if (!state) throw new Error('Primary mode requires verified Drive bootstrap; do not start index.js directly.');
  return state;
}
module.exports = { installPrimaryContext, getPrimaryContext, requirePrimaryContext };
