'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const {planPathRebase,applyPathRebase}=require('../systems/primary-path-rebase');
function sandbox(t){
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'bankroll-rebase-'));
  fs.mkdirSync(path.join(root,'vault'),{recursive:true});
  fs.mkdirSync(path.join(root,'files'),{recursive:true});
  fs.writeFileSync(path.join(root,'vault','car.bin'),'car');
  fs.writeFileSync(path.join(root,'files','buyer.bin'),'buyer');
  t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
  return root;
}
test('rebases old absolute vault and customer file paths only when verified',t=>{
  const root=sandbox(t);
  fs.writeFileSync(path.join(root,'vault.json'),JSON.stringify({car:{path:'/old/render/data/vault/car.bin',size:3}}));
  fs.writeFileSync(path.join(root,'deliveries.json'),JSON.stringify({order:{files:[{path:'/old/render/data/files/buyer.bin'}]}}));
  const manifest={files:[{path:'vault/car.bin'},{path:'files/buyer.bin'},{path:'vault.json'},{path:'deliveries.json'}]};
  const count=applyPathRebase({dataDir:root,manifest});
  assert.equal(count,2);
  const vault=JSON.parse(fs.readFileSync(path.join(root,'vault.json'),'utf8'));
  const deliveries=JSON.parse(fs.readFileSync(path.join(root,'deliveries.json'),'utf8'));
  assert.equal(vault.car.path,path.join(root,'vault','car.bin'));
  assert.equal(deliveries.order.files[0].path,path.join(root,'files','buyer.bin'));
});
test('refuses to rewrite a path absent from verified manifest',t=>{
  const root=sandbox(t);
  const raw=JSON.stringify({missing:{path:'/old/data/vault/unverified.bin'}});
  fs.writeFileSync(path.join(root,'vault.json'),raw);
  assert.throws(()=>planPathRebase({dataDir:root,manifest:{files:[{path:'vault/car.bin'}]}}),/not uniquely backed/);
  assert.equal(fs.readFileSync(path.join(root,'vault.json'),'utf8'),raw);
});
test('does not rewrite paths that already point to current data directory',t=>{
  const root=sandbox(t);
  const current=path.join(root,'vault','car.bin');
  fs.writeFileSync(path.join(root,'vault.json'),JSON.stringify({car:{path:current}}));
  assert.deepEqual(planPathRebase({dataDir:root,manifest:{files:[{path:'vault/car.bin'}]}}),[]);
});
