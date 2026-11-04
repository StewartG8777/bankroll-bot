'use strict';

// Only files created by Bankroll's content-addressed backup writer may be
// considered by retention. Unknown and legacy Drive objects are preserved.
function isManagedContentAddressedBackup(name) {
  const value = String(name || '');
  const databaseObject = /^[a-f0-9]{24}_[a-z0-9_.-]+\.json$/i.test(value);
  const vaultObject = /^[a-f0-9]{16}_[a-f0-9]{24}_[a-z0-9_.-]+$/i.test(value);
  return databaseObject || vaultObject;
}

module.exports = { isManagedContentAddressedBackup };
