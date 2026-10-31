// Usage: node patch-index.js [path/to/index.js]   (default ./index.js)
// Makes a backup (index.js.bak), applies 5 fixes, checks syntax. Aborts without writing if any anchor is missing.
const fs = require('fs');
const { execFileSync } = require('child_process');
const file = process.argv[2] || 'index.js';
let src = fs.readFileSync(file, 'utf8');
const crlf = src.includes('\r\n');
src = src.replace(/\r\n/g, '\n');

function must(name, find, replace) {
  if (typeof find === 'string') {
    const n = src.split(find).length - 1;
    if (n !== 1) throw new Error(`Fix "${name}": expected 1 match, found ${n}`);
    src = src.replace(find, () => replace);
  } else {
    const n = (src.match(new RegExp(find.source, find.flags + 'g')) || []).length;
    if (n !== 1) throw new Error(`Fix "${name}": expected 1 match, found ${n}`);
    src = src.replace(find, () => replace);
  }
  console.log('applied:', name);
}

// 1. Stray line that runs before wipedCustomers exists (the indented copy only).
must('remove stray wipedCustomers line',
  /^ {4}try \{ const ids = JSON\.parse\(fs\.readFileSync\(WIPED_CUSTOMERS_FILE, 'utf8'\)\); if \(Array\.isArray\(ids\)\) wipedCustomers = new Set\(ids\.map\(String\)\); \} catch \{\}\n(?=function hasProcessedWebhook)/m,
  '');

// 2. Startup restore runs in the background; the rest of startup continues after it finishes.
must('background startup restore',
  "  await autoRestoreOnStartup();\n",
  "  autoRestoreOnStartup()\n" +
  "    .catch((e) => console.error('Startup restore error:', e.message))\n" +
  "    .then(() => postRestoreStartup())\n" +
  "    .catch((e) => console.error('Post-restore startup error:', e));\n" +
  "};\n\n" +
  "async function postRestoreStartup() {\n");

// 3. Cap the backup-channel history scan (10 pages = 1000 messages).
must('cap history scan', "while (!exhausted) {", "while (!exhausted && pages < 10) {");

// 4. Faster shutdown so the rolling deploy is not held up.
must('faster shutdown', "setTimeout(r, 20000)", "setTimeout(r, 5000)");

// 5. Dead backup links fail fast.
must('backup link timeout',
  "fetch(link.url, { signal: AbortSignal.timeout(120000) })",
  "fetch(link.url, { signal: AbortSignal.timeout(30000) })");

if (crlf) src = src.replace(/\n/g, '\r\n');
fs.copyFileSync(file, file + '.bak');
const tmp = file + '.patched.js';
fs.writeFileSync(tmp, src);
try { execFileSync(process.execPath, ['--check', tmp], { stdio: 'inherit' }); }
catch { fs.unlinkSync(tmp); throw new Error('Patched file failed syntax check. Nothing changed.'); }
fs.renameSync(tmp, file);
console.log('Done. Original saved as ' + file + '.bak');
