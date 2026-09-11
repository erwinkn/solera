const $ = (s) => document.querySelector(s);
const el = (tag, text, cls) => { const n = document.createElement(tag); if (text !== undefined) n.textContent = text; if (cls) n.className = cls; return n; };
let state, view = 'assets', filter = '', selectedRun, partition = '', refreshing = false, requestKey, requestBody;
let token = sessionStorage.getItem('dorc-token') || '';
const time = (value) => value ? new Date(value * 1000).toLocaleString([], {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';
const tag = (value) => el('span', value.replaceAll('_', ' '), `tag ${value}`);
function button(text, handler, cls) { const b = el('button', text, cls); b.type = 'button'; b.addEventListener('click', handler); return b; }
function error(message) { $('#error').textContent = message; $('#error').hidden = !message; }
async function api(path, options={}) {
  const response = await fetch('/api' + path, {...options, headers:{'Content-Type':'application/json',...(token ? {Authorization:'Bearer '+token}:{}),...options.headers}});
  if (response.status === 401) { if (!$('#login').open) $('#login').showModal(); throw new Error('Authentication required'); }
  const data = await response.json(); if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail)); return data;
}
function empty(title, description) { const n=el('div',undefined,'empty'); n.append(el('h2',title),el('p',description)); return n; }
function table(headers, rows) { const wrapper=el('div',undefined,'table-scroll'), t=el('table'), h=el('tr'); headers.forEach(x=>{const th=el('th',x);th.scope='col';h.append(th)}); const head=el('thead');head.append(h);t.append(head);const body=el('tbody'); rows.forEach(cells=>{const row=el('tr');cells.forEach(value=>{const td=el('td');td.append(value instanceof Node?value:document.createTextNode(value ?? '—'));row.append(td)});body.append(row)});t.append(body);wrapper.append(t);return wrapper; }
function json(title, value, parent) { parent.append(el('h3',title),el('pre',JSON.stringify(value,null,2))); }
function setView(next) { view=next; document.querySelectorAll('nav button').forEach(b=>{b.classList.toggle('active',b.dataset.view===view);b.setAttribute('aria-current',b.dataset.view===view?'page':'false')}); render(); }
function render() {
  if (!state) return;
  const names={assets:['Assets','Data products, dependencies, and their committed state.'],runs:['Runs','Durable requests, attempts, and materialization history.'],automations:['Automations','One scheduling model, with durable evaluation cursors.'],storage:['Storage','Object storage is the source of truth. Local caches are disposable.']};
  $('#heading').textContent=names[view][0]; $('#description').textContent=names[view][1]; $('#breadcrumb').textContent=`WORKSPACE / ${view.toUpperCase()}`;
  $('#materialize').hidden=view==='storage';$('#workspace').textContent=state.storage.namespace;$('#asset-count').textContent=state.assets.length;$('#run-count').textContent=state.runs.length;
  const root=$('#content'); root.replaceChildren();
  if(view==='assets') {
    const tools=el('div',undefined,'toolbar'), search=el('input');search.type='search';search.placeholder='Filter assets…';search.setAttribute('aria-label','Filter assets');search.value=filter;search.addEventListener('input',()=>{filter=search.value;renderAssets()});tools.append(search,el('span',`${state.assets.length} assets · ${new Set(state.assets.map(a=>a.group)).size} groups`,'count'));root.append(tools,el('div',undefined,'asset-list'));
    renderAssets();root.append(el('h3','Dependencies')); const graph=el('div',undefined,'lineage');state.assets.forEach(a=>{const node=el('div',undefined,'lineage-node');node.append(button(a.name,()=>showAsset(a.name),'asset-name'),el('small',a.inputs.length?'← '+a.inputs.join(', '):'Source asset'));graph.append(node)});root.append(graph);
  } else if(view==='runs') {
    if(!state.runs.length)root.append(empty('No materializations yet','Start with sample_quality to exercise incremental, multi-output processing.'));
    else root.append(table(['Request','Targets','Status','Created'],state.runs.map(r=>[button(r.id.slice(0,8),()=>showRun(r.id),'asset-name'),r.targets.join(', '),tag(r.status),time(r.created_at)])));
  } else if(view==='automations') {
    root.append(table(['Automation','Targets','Interval','State'],state.automations.map(a=>[a.name,a.targets.join(', '),`${a.every_seconds}s`,button(a.enabled?'Enabled — pause':'Paused — enable',async()=>{try{await api('/automations/'+encodeURIComponent(a.name),{method:'POST',body:JSON.stringify({enabled:!a.enabled})});await refresh()}catch(e){error(e.message)}})])));
    root.append(el('p','Missed interval ticks are coalesced. Accepting requests and advancing the automation cursor is one durable transaction.','notice'));
  } else {
    const grid=el('dl',undefined,'storage-grid');const entries=[['State engine',state.storage.engine+' 0.16'],['Object store',state.storage.scheme==='file'?'Local filesystem':state.storage.scheme.toUpperCase()],['Namespace',state.storage.namespace],['Last local acknowledgement',String(state.storage.sequence)],['Publication','Object-store durability awaited before acknowledgement'],['Coordinator','One active writer; replacement fences the old writer'],['Definition',state.revision.slice(0,16)]];
    entries.forEach(([k,v])=>grid.append(el('dt',k),el('dd',v)));root.append(grid,el('p','Experimental backend. SlateDB owns the log, compaction, recovery, and writer fencing. Data files are immutable; output references, checkpoints, task completion, and change notifications commit together.','notice'),el('h3','Current boundaries'),el('p','JSON snapshots and local subprocess execution. No cross-destination transactions, historical code bundles, or artifact garbage collection. External side effects may repeat after a crash. The filesystem mode is a development backend, not a multi-host object store.'));
  }
}
function renderAssets() {
 const root=$('.asset-list'); if(!root)return;
 const assets=state.assets.filter(a=>a.name.toLowerCase().includes(filter.toLowerCase()));
 root.replaceChildren(table(['Asset','Group','Update model','Published'],assets.map(a=>{const name=el('div');name.append(button(a.name,()=>showAsset(a.name),'asset-name'),el('span',a.inputs.length?`${a.inputs.length} upstream asset${a.inputs.length>1?'s':''}`:'Source asset','sub'));return [name,a.group,tag(a.incremental?'keyed incremental':a.partitions?'daily partitions':'snapshot'),a.heads.length?el('div',`${a.heads.length} scope${a.heads.length>1?'s':''} · ${time(Math.max(...a.heads.map(h=>h.updated_at)))}`):tag('not_materialized')]})));
}
async function showAsset(name, selectedPartition) {
 selectedRun=null;const asset=state.assets.find(a=>a.name===name);partition=selectedPartition??asset.heads[0]?.partition??'';
 try {
  const data=await api('/assets/'+encodeURIComponent(name)+'?partition='+encodeURIComponent(partition));$('#drawer-title').textContent=name;const root=$('#drawer-content');root.replaceChildren(el('p',asset.description));
  if(asset.partitions&&asset.heads.length){const select=el('select',undefined,'partition-select');select.setAttribute('aria-label','Partition');asset.heads.forEach(h=>{const option=el('option',h.partition);option.value=h.partition;select.append(option)});select.value=partition;select.addEventListener('change',()=>showAsset(name,select.value));root.append(select)}
  root.append(button('Materialize this asset',()=>openRequest(name),'primary'));
  if(!data.head)root.append(empty('Not materialized','Create a materialization to publish this asset.'));
  else {root.append(el('h3','Data preview · first 100 rows'));if(Array.isArray(data.preview)&&data.preview.length&&typeof data.preview[0]==='object'&&data.preview[0]!==null){const columns=[...new Set(data.preview.flatMap(Object.keys))];const preview=table(columns,data.preview.map(r=>columns.map(c=>typeof r[c]==='object'?JSON.stringify(r[c]):String(r[c]??''))));preview.classList.add('data-table');root.append(preview)}else root.append(el('pre',JSON.stringify(data.preview,null,2)));json('Checkpoint',data.checkpoint,root);json('Immutable output',data.head,root);json('Materialization commit',data.commit,root)}
  if(!$('#drawer').open)$('#drawer').showModal();
 }catch(e){error(e.message)}
}
async function runAction(id, action){try{await api('/runs/'+id+'/'+action,{method:'POST'});await showRun(id)}catch(e){error(e.message)}}
async function showRun(id, silent=false) {
 selectedRun=id;
 try{const detail=await api('/runs/'+id);if(selectedRun!==id)return;$('#drawer-title').textContent='Run '+id.slice(0,8);const root=$('#drawer-content');root.replaceChildren(tag(detail.request.status),el('p',detail.request.targets.join(', ')+' · '+time(detail.request.created_at)));
 const actions=el('div',undefined,'actions');if(!['failed','succeeded','canceled'].includes(detail.request.status)){actions.append(button('Cancel request',()=>runAction(id,'cancel')),button(detail.request.paused?'Resume':'Pause',()=>runAction(id,detail.request.paused?'resume':'pause')))}if(detail.request.status==='failed')actions.append(button('Retry failed work',()=>runAction(id,'retry')));root.append(actions,table(['Producer','Partition','Status','Generation'],detail.tasks.map(t=>[t.producer,t.partition||'—',tag(t.status),String(t.generation)])),el('h3','Events'));
 detail.events.forEach(event=>{const n=el('div',undefined,'event');n.append(el('time',time(event.at)),tag(event.kind),el('p',event.message));if(event.data)n.append(el('pre',JSON.stringify(event.data,null,2)));root.append(n)});
 Object.entries(detail.attempts).forEach(([task,attempts])=>attempts.filter(a=>a.logs||a.error).forEach(a=>json('Attempt '+task.slice(0,8)+' / '+a.generation,a.logs||a.error,root)));
 if(!silent&&!$('#drawer').open)$('#drawer').showModal();
 }catch(e){error(e.message)}
}
function openRequest(name) { if($('#drawer').open)$('#drawer').close();requestKey=null;requestBody=null;const list=$('#target-list');list.replaceChildren(el('legend','Targets'));state.assets.forEach(a=>{const label=el('label'),input=el('input');input.type='checkbox';input.value=a.name;input.checked=a.name===name;input.addEventListener('change',updatePartitions);label.append(input,document.createTextNode(a.name));list.append(label)});$('#request-error').textContent='';updatePartitions();$('#request-dialog').showModal(); }
function updatePartitions(){const selected=[...$('#target-list').querySelectorAll('input:checked')].map(i=>i.value);$('#partition-fields').hidden=!selected.some(n=>state.assets.find(a=>a.name===n).partitions)}
$('#request-form').addEventListener('submit',async(event)=>{event.preventDefault();$('#submit-run').disabled=true;try{const targets=[...$('#target-list').querySelectorAll('input:checked')].map(i=>i.value), partitions=[];if(!targets.length)throw new Error('Select at least one asset');if(!$('#partition-fields').hidden){const start=$('#from-date').value,end=$('#to-date').value;if(!start||!end||start>end)throw new Error('Choose a valid inclusive date range');for(let d=new Date(start+'T00:00:00Z'),last=new Date(end+'T00:00:00Z');d<=last;d.setUTCDate(d.getUTCDate()+1)){partitions.push(d.toISOString().slice(0,10));if(partitions.length>1000)throw new Error('Maximum 1,000 partitions')}}const config=JSON.parse($('#config').value);if(!config||Array.isArray(config)||typeof config!=='object')throw new Error('Configuration must be a JSON object');const body=JSON.stringify({targets,partitions,mode:$('#mode').value,config});if(body!==requestBody){requestKey=crypto.randomUUID();requestBody=body}const run=await api('/runs',{method:'POST',headers:{'Idempotency-Key':requestKey},body});$('#request-dialog').close();setView('runs');await refresh();await showRun(run.id)}catch(e){$('#request-error').textContent=e.message}finally{$('#submit-run').disabled=false}});
$('#login-form').addEventListener('submit',async(event)=>{event.preventDefault();token=$('#token').value;try{await api('/state');sessionStorage.setItem('dorc-token',token);$('#token').value='';$('#login').close();await refresh()}catch(e){$('#login-error').textContent=e.message}});
$('#signout').addEventListener('click',()=>{sessionStorage.removeItem('dorc-token');location.reload()});
document.querySelectorAll('nav button').forEach(b=>b.addEventListener('click',()=>setView(b.dataset.view)));
document.querySelectorAll('.close-dialog').forEach(b=>b.addEventListener('click',()=>b.closest('dialog').close()));
$('#drawer').addEventListener('close',()=>{selectedRun=null});$('#materialize').addEventListener('click',()=>openRequest());
async function refresh(){if(refreshing||$('#login').open)return;refreshing=true;try{state=await api('/state');$('#connection').textContent='Connected · '+state.storage.scheme;error('');if(!$('#content').contains(document.activeElement)&&!$('#request-dialog').open)render();if(selectedRun&&$('#drawer').open)await showRun(selectedRun,true)}catch(e){$('#connection').textContent='Disconnected';if(e.message!=='Authentication required')error(e.message)}finally{refreshing=false}}
await refresh();setInterval(refresh,2000);
