'use strict';
// Bankroll stores some local delivery/vault paths as absolute DATA_DIR paths.
// A Render redeploy can change that root. Rebase ONLY paths whose byte content
// belongs to the cryptographically verified current Drive manifest.
const fs=require('node:fs'),path=require('node:path');
const {atomicWriteFile}=require('./atomic-storage');
const INDEXES=['vault.json','deliveries.json','downloads.json'];
function relativeStoragePath(raw,verified){
  const parts=String(raw||'').replace(/\\/g,'/').split('/');
  const matches=[];
  for(let i=0;i<parts.length;i++){
    if(!['vault','files','deliveries'].includes(parts[i]))continue;
    const candidate=parts.slice(i).join('/');
    if(verified.has(candidate))matches.push(candidate);
  }
  const unique=[...new Set(matches)];
  if(unique.length!==1)throw Error('Indexed file path is not uniquely backed by current verified Drive generation: '+String(raw).slice(0,180));
  return unique[0];
}
function planPathRebase({dataDir,manifest}){
  if(!dataDir||!Array.isArray(manifest?.files))throw TypeError('Verified Drive manifest and DATA_DIR required.');
  const root=path.resolve(dataDir),verified=new Set(manifest.files.map(x=>x.path));
  const pending=[],seen=new Set();
  for(const name of INDEXES){
    const file=path.join(root,name);
    if(!fs.existsSync(file))continue;
    const parsed=JSON.parse(fs.readFileSync(file,'utf8'));
    let changed=0;
    const visit=(node)=>{
      if(!node||typeof node!=='object')return;
      if(seen.has(node))return;seen.add(node);
      if(typeof node.path==='string'&&!/^https?:\/\//i.test(node.path)){
        const rel=relativeStoragePath(node.path,verified);
        const expected=path.join(root,...rel.split('/'));
        if(!fs.statSync(expected).isFile())throw Error('Expected verified local file missing: '+rel);
        if(path.resolve(node.path)!==path.resolve(expected)){
          node.path=expected;changed++;
        }
      }
      for(const value of Object.values(node))if(value&&typeof value==='object')visit(value);
    };
    visit(parsed);
    if(changed)pending.push({file,content:JSON.stringify(parsed,null,2),count:changed});
  }
  return pending;
}
function applyPathRebase({dataDir,manifest}){
  const changes=planPathRebase({dataDir,manifest});
  for(const change of changes)atomicWriteFile(change.file,change.content);
  return changes.reduce((sum,x)=>sum+x.count,0);
}
module.exports={planPathRebase,applyPathRebase,relativeStoragePath};
