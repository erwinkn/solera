"""One-time, exact-match source refinement; removed after verified publication."""
from pathlib import Path


def replace(path, old, new):
    path = Path(path)
    text = path.read_text()
    assert text.count(old) == 1, (str(path), old)
    path.write_text(text.replace(old, new))


replace('src/data_orchestrator/sdk.py',
    '            if a.incremental and (a.incremental.input not in inputs or a.incremental.batch_size < 1):',
    '''            parameters = inspect.signature(a.fn).parameters
            if any(p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD) for p in parameters.values()):
                raise ValueError("Asset parameters must be explicitly named and keyword-bindable")
            if any(name not in parameters for name in inputs):
                raise ValueError("Inputs must reference producer parameters")
            if "ctx" in inputs or "ctx" in self.resources:
                raise ValueError("ctx is reserved for AssetContext")
            if set(inputs) & set(self.resources):
                raise ValueError("Input and resource parameter names must not collide")
            if any(name != "ctx" and name not in inputs and name not in self.resources and p.default is inspect.Parameter.empty for name, p in parameters.items()):
                raise ValueError("Every required producer parameter must have an input or resource binding")
            if a.incremental and (a.incremental.input not in inputs or a.incremental.batch_size < 1):''')
replace('src/data_orchestrator/engine.py',
    '        refs, receipts = {}, {}\n        for asset, write in result["outputs"].items():',
    '        refs, receipts = {}, {}\n        inputs_complete = all(info["ref"]["complete"] for info in claim["input_refs"].values())\n        for asset, write in result["outputs"].items():')
replace('src/data_orchestrator/engine.py', '            kind, complete = write["kind"], True', '            kind, complete = write["kind"], inputs_complete')
replace('src/data_orchestrator/engine.py',
    '                    raise ValueError("Inventory requires rows and a boolean completeness flag")',
    '                    raise ValueError("Inventory requires rows and a boolean completeness flag")\n                complete = complete and inputs_complete')
replace('src/data_orchestrator/engine.py',
    '            and all(h and h.get("scope_complete") for h in claim["baseline"].values())',
    '            and all(h and h.get("scope_complete") for h in claim["baseline"].values())\n            and all(info["ref"]["complete"] for info in claim["input_refs"].values())')
replace('src/data_orchestrator/demo.py', 'row["id"] in ctx.changes["upserted_keys"]', 'str(row["id"]) in ctx.changes["upserted_keys"]')
replace('src/data_orchestrator/web/app.js',
    "let state, view = 'assets', filter = '', selectedRun, partition = '', refreshing = false, requestKey, requestBody;",
    "let state, view = 'assets', filter = '', selectedRun, partition = '', refreshing = false, requestKey, requestBody, selectionEpoch = 0;")
replace('src/data_orchestrator/web/app.js',
    " selectedRun=null;const asset=state.assets.find(a=>a.name===name);",
    " const epoch=++selectionEpoch;selectedRun=null;const asset=state.assets.find(a=>a.name===name);")
replace('src/data_orchestrator/web/app.js',
    "encodeURIComponent(partition));$('#drawer-title').textContent=name;",
    "encodeURIComponent(partition));if(epoch!==selectionEpoch)return;$('#drawer-title').textContent=name;")
replace('src/data_orchestrator/web/app.js',
    "typeof data.preview[0]==='object'&&data.preview[0]!==null",
    "data.preview.every(row=>row!==null&&typeof row==='object'&&!Array.isArray(row))")
replace('src/data_orchestrator/web/app.js',
    "await api('/runs/'+id+'/'+action,{method:'POST'});await showRun(id)",
    "await api('/runs/'+id+'/'+action,{method:'POST'});if(selectedRun===id&&$('#drawer').open)await showRun(id,true)")
replace('src/data_orchestrator/web/app.js',
    " selectedRun=id;\n try{const detail=await api('/runs/'+id);if(selectedRun!==id)return;",
    " const epoch=silent?selectionEpoch:++selectionEpoch;selectedRun=id;\n try{const detail=await api('/runs/'+id);if(selectedRun!==id||epoch!==selectionEpoch)return;")
replace('src/data_orchestrator/web/app.js',
    "$('#drawer').addEventListener('close',()=>{selectedRun=null});",
    "$('#drawer').addEventListener('close',()=>{selectedRun=null;selectionEpoch++});")
replace('src/data_orchestrator/web/app.js',
    "body:JSON.stringify({enabled:!a.enabled})",
    "body:JSON.stringify({enabled:!state.automations.find(current=>current.name===a.name).enabled})")
replace('README.md',
    '`uv.lock` and the browser lock are uploaded with test artifacts until they are committed. The two storage engines are explicitly version-pinned.',
    '`uv.lock` and the browser package lock are committed. CI uses locked installs; the storage engines are explicitly version-pinned.')
replace('README.md', 'uv sync\n', 'uv sync --locked\n')
replace('README.md', 'cd ui && npm install && npx playwright install chromium && npm test', 'cd ui && npm ci && npx playwright install chromium && npm test')
replace('README.md', 'for f in files if f["id"] in changed', 'for f in files if str(f["id"]) in changed')
with Path('docs/architecture.md').open('a') as f:
    f.write('\n## Review regressions\n\nIncomplete-inventory flags are propagated through every output reference, including ordinary snapshot transformations; a downstream keyed consumer must not turn a partial scan into deletions. A no-change skip also requires complete inputs. Manifest protocol files are separate from user stdout. Subprocess log pipes have bounded buffers and an 8 MiB aggregate limit; exceeding it terminates the process group. Registration rejects ambiguous input/resource/context bindings and unsupported signatures.\n\nEngine lifecycle is one-shot: after `stop`, construct a new Engine and call `initialize` before resuming work. That startup reconciles persisted active attempts; a process interrupted at an uncertain commit must not blindly release its scope.\n')
with Path('docs/railway.md').open('a') as f:
    f.write('\n## Attempted validation — September 11, 2026\n\nThe dedicated `data-orchestrator-s3-test` Railway project was created and the application built, but bucket provisioning did not complete: the environment returned no buckets and bucket references resolved without a bucket name. The application correctly failed before S3 tests. **No real Railway S3 validation or public demo is claimed.** Local filesystem and HTTP S3-emulator results are separate.\n\nCleanup of the failed `orchestrator-s3-test` service is staged but requires two-factor approval in the Railway dashboard. The API cannot complete it. Project: `87381f11-c1f0-4cac-a0d5-e872be0b8ada`; environment: `production`. The user must approve that staged removal; the service is not reported as deleted. No bucket was provisioned.\n\nFor a future new Railway bucket, inspect its actual addressing mode: new buckets may use virtual-hosted style, requiring `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true`. Do not assume path-style compatibility or substitute a filesystem deployment for a provider test.\n')
