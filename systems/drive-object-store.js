'use strict';

// Direct Google Drive object store for non-payment files. Drive is the source of
// truth; callers may stream data without writing a persistent local copy.
const { Readable } = require('stream');
const path = require('path');
const crypto = require('crypto');
const { googleDriveFolderForDatabaseFile } = require('./drive-layout');

const PAYMENT_FILES = new Set([
  'monzo_payments.json', 'monzo_review.json', 'stripe_balance_history.json',
  'stripe_balance_snapshots.json', 'webhook_events.json',
]);
function destinationFor(relativePath) {
  if (typeof relativePath !== 'string' || !relativePath || relativePath.includes('\\')) throw new Error('Invalid Drive object path');
  const parts = relativePath.split('/');
  if (parts.some(p => !p || p === '.' || p === '..') || relativePath.startsWith('/')) throw new Error('Invalid Drive object path');
  const name = parts[parts.length - 1];
  if (PAYMENT_FILES.has(name.toLowerCase())) throw new Error('Payment files remain on their existing storage path');
  if (parts[0] === 'vault') return 'VAULT';
  if (parts[0] === 'deliveries' || parts[0] === 'files') return 'CUSTOMER DELIVERIES';
  if (parts[0] === 'tmp') return 'TEMPORARY PROCESSING';
  if (parts.length === 1 && name.endsWith('.json')) return googleDriveFolderForDatabaseFile(name);
  throw new Error('Unsupported non-payment Drive object path');
}
function createDriveObjectStore({ drive, folders }) {
  if (!drive?.files || !folders) throw new Error('Drive client and resolved folders are required');
  async function versions(relativePath) {
    const folder = destinationFor(relativePath);
    const parent = folders[folder]?.id;
    if (!parent) throw new Error('Missing Google Drive folder: ' + folder);
    const found = [];
    let pageToken;
    do {
      const response = await drive.files.list({
        q: "'" + parent.replace(/'/g, "\\'") + "' in parents and trashed = false",
        pageSize: 1000, pageToken,
        fields: 'nextPageToken,files(id,name,createdTime,appProperties,size)',
        supportsAllDrives: true, includeItemsFromAllDrives: true,
      });
      found.push(...(response.data.files || []).filter(f => f.appProperties?.bankrollObjectPath === relativePath && f.appProperties?.bankrollObjectVersion === '1'));
      pageToken = response.data.nextPageToken;
    } while (pageToken);
    return found.sort((a,b) => (b.createdTime || '').localeCompare(a.createdTime || '') || b.id.localeCompare(a.id));
  }
  async function put(relativePath, source, { mimeType = 'application/octet-stream' } = {}) {
    const folder = destinationFor(relativePath);
    const parent = folders[folder]?.id;
    if (!parent) throw new Error('Missing Google Drive folder: ' + folder);
    const body = Buffer.isBuffer(source) ? Readable.from([source]) : source;
    if (!body || typeof body.pipe !== 'function') throw new Error('A readable stream or Buffer is required');
    const result = await drive.files.create({
      requestBody: {
        name: crypto.randomUUID() + '_' + path.posix.basename(relativePath),
        parents: [parent],
        appProperties: { bankrollObjectPath: relativePath, bankrollObjectVersion: '1' },
      },
      media: { mimeType, body },
      fields: 'id,name,size,createdTime',
      supportsAllDrives: true,
    });
    if (!result.data?.id) throw new Error('Google Drive did not confirm the uploaded object');
    return result.data;
  }
  async function get(relativePath) {
    const [entry] = await versions(relativePath);
    if (!entry) return null;
    const response = await drive.files.get({ fileId: entry.id, alt: 'media', supportsAllDrives: true }, { responseType: 'stream' });
    return { stream: response.data, fileId: entry.id, size: Number(entry.size || 0) };
  }
  async function remove(relativePath) {
    // Delete all versions only on an explicit request; never touch backups.
    const entries = await versions(relativePath);
    for (const entry of entries) await drive.files.delete({ fileId: entry.id, supportsAllDrives: true });
    return entries.length;
  }
  return { put, get, versions, remove };
}
module.exports = { destinationFor, createDriveObjectStore };
