'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const {acquireLocalWriterLock}=require('../systems/primary-local-writer-lock');
test('only one writer can hold local lock; release permits next writer',()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'bankroll-writer-'));
 try{
  const first=acquireLocalWriterLock(dir);
  assert.throws(()=>acquireLocalWriterLock(dir),/Refusing second writer/);
  first.release();
  const next=acquireLocalWriterLock(dir);
  next.release();
 }finally{fs.rmSync(dir,{recursive:true,force:true});}
});
test('lock cleanup does not remove a replaced lock',()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'bankroll-writer-'));
 try{
  const first=acquireLocalWriterLock(dir);
  const file=first.path;
  fs.writeFileSync(file,JSON.stringify({token:'another-owner'}));
  first.release();
  assert.equal(JSON.parse(fs.readFileSync(file,'utf8')).token,'another-owner');
 }finally{fs.rmSync(dir,{recursive:true,force:true});}
});
