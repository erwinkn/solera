import asyncio
import copy

import pytest

from data_orchestrator import AppendBatch, Automation, Batch, ByKey, Inventory, Project, ReplaceKeys, Upsert, asset
from data_orchestrator.engine import Conflict, LostOwnership, head_key, scope
from data_orchestrator.storage import Transaction

from conftest import finish


def keyed_project():
    source = {'rows': [{'id': str(i), 'revision': '1', 'count': 2} for i in range(3)], 'complete': True}
    @asset
    def inventory():
        return Inventory(source['rows'], source['complete'])
    @asset(outputs=['parents', 'children'], inputs={'rows': 'inventory'}, incremental=ByKey('rows', batch_size=2))
    def transform(ctx, rows):
        selected = [r for r in rows if r['id'] in ctx.changes['upserted_keys']]
        keys = ctx.changes['upserted_keys'] + ctx.changes['deleted_keys']
        return Batch({'parents': ReplaceKeys('owner', keys, [{'owner': r['id']} for r in selected]), 'children': ReplaceKeys('owner', keys, [{'owner': r['id'], 'value': j} for r in selected for j in range(r['count'])])})
    @asset
    def report(parents, children):
        return {'parents': len(parents), 'children': len(children)}
    return Project([inventory, transform, report]), source


async def test_atomic_multi_output_batches_and_pinned_versions(make_engine, state):
    project, _ = keyed_project()
    engine = await make_engine(project)
    run = await engine.submit(['report'])
    detail = await finish(engine, run)
    assert detail['request']['status'] == 'succeeded'
    a, b = await engine.asset_detail('parents'), await engine.asset_detail('children')
    assert a['head']['commit_id'] == b['head']['commit_id']
    assert a['checkpoint']['generation'] == 2
    assert len(a['preview']) == 3 and len(b['preview']) == 6
    assert (await engine.asset_detail('report'))['preview'] == {'parents': 3, 'children': 6}
    commits = [v for _, v in await state.scan('commit/') if v['producer'] == 'transform']
    assert len(commits) == 2
    for c in commits:
        assert set(c['outputs']) == {'parents', 'children'}
        assert await state.get('outbox/' + c['id']) is not None
        assert c['inputs']['rows']['ref']['sha256']


async def test_unchanged_and_single_changed_item(make_engine):
    project, source = keyed_project()
    engine = await make_engine(project)
    await finish(engine, await engine.submit(['report']))
    before = (await engine.asset_detail('parents'))['head']['commit_id']
    detail = await finish(engine, await engine.submit(['report']))
    assert next(t for t in detail['tasks'] if t['producer'] == 'transform')['status'] == 'skipped'
    assert (await engine.asset_detail('parents'))['head']['commit_id'] == before
    source['rows'][1]['revision'] = '2'
    engine.backend.calls.clear()
    await finish(engine, await engine.submit(['report']))
    calls = [s for s in engine.backend.calls if s['producer'] == 'transform']
    assert len(calls) == 1 and calls[0]['context']['changes']['upserted_keys'] == ['1']


async def test_deletions_and_zero_child_rows(make_engine):
    project, source = keyed_project()
    engine = await make_engine(project)
    await finish(engine, await engine.submit(['report']))
    source['rows'] = [{'id': '0', 'revision': '2', 'count': 0}]
    assert (await finish(engine, await engine.submit(['report'])))['request']['status'] == 'succeeded'
    assert (await engine.asset_detail('parents'))['preview'] == [{'owner': '0'}]
    assert (await engine.asset_detail('children'))['preview'] == []


async def test_incomplete_inventory_never_implies_deletion(make_engine):
    project, source = keyed_project()
    engine = await make_engine(project)
    await finish(engine, await engine.submit(['report']))
    source.update(rows=[{'id': '0', 'revision': '2', 'count': 1}], complete=False)
    await finish(engine, await engine.submit(['report']))
    assert len((await engine.asset_detail('parents'))['preview']) == 3
    assert len((await engine.asset_detail('children'))['preview']) == 5
    detail = await finish(engine, await engine.submit(['report'], mode='recompute'))
    assert detail['request']['status'] == 'failed'
    assert len((await engine.asset_detail('parents'))['preview']) == 3


async def test_failed_batch_repair_preserves_committed_progress(make_engine):
    project, _ = keyed_project()
    engine = await make_engine(project)
    engine.backend.fail_when = lambda s: s['producer'] == 'transform' and s['context']['changes']['upserted_keys'] == ['2']
    run = await engine.submit(['report'])
    detail = await finish(engine, run)
    assert detail['request']['status'] == 'failed'
    assert len((await engine.asset_detail('parents'))['preview']) == 2
    assert (await engine.asset_detail('parents'))['checkpoint']['generation'] == 1
    assert next(t for t in detail['tasks'] if t['producer'] == 'report')['status'] == 'blocked'
    engine.backend.fail_when = None
    engine.backend.calls.clear()
    await engine.retry(run['id'])
    assert (await finish(engine, run))['request']['status'] == 'succeeded'
    calls = [s for s in engine.backend.calls if s['producer'] == 'transform']
    assert len(calls) == 1 and calls[0]['context']['changes']['upserted_keys'] == ['2']


async def test_fill_missing_does_not_reuse_partial_heads(make_engine):
    project, _ = keyed_project()
    engine = await make_engine(project)
    run = await engine.submit(['report'])
    await engine.execute_next()
    await engine.execute_next()
    assert not (await engine.asset_detail('parents'))['head']['scope_complete']
    await engine.cancel(run['id'])
    detail = await finish(engine, await engine.submit(['report'], mode='fill_missing'))
    assert detail['request']['status'] == 'succeeded'
    assert next(t for t in detail['tasks'] if t['producer'] == 'transform')['status'] == 'succeeded'
    assert len((await engine.asset_detail('parents'))['preview']) == 3


async def test_config_changes_invalidate_all_keyed_items(make_engine):
    project, _ = keyed_project()
    engine = await make_engine(project)
    await finish(engine, await engine.submit(['report']))
    engine.backend.calls.clear()
    await finish(engine, await engine.submit(['report'], config={'parser_mode': 'new'}))
    processed = [k for s in engine.backend.calls if s['producer'] == 'transform' for k in s['context']['changes']['upserted_keys']]
    assert sorted(processed) == ['0', '1', '2']


async def test_staging_failure_publishes_nothing(make_engine, state, monkeypatch):
    project, _ = keyed_project()
    engine = await make_engine(project)
    run = await engine.submit(['report'])
    await engine.execute_next()
    claim = await engine.claim()
    prepared = await engine.prepare(claim)
    result, _ = await engine.backend.execute(prepared['spec'])
    original, count = state.stage, 0
    async def failing_stage(*a, **kw):
        nonlocal count
        count += 1
        if count == 2:
            raise OSError('Upload interrupted')
        return await original(*a, **kw)
    monkeypatch.setattr(state, 'stage', failing_stage)
    with pytest.raises(OSError):
        await engine.stage_result(claim, result)
    assert await state.get(head_key('parents', '')) is None
    assert await state.get(head_key('children', '')) is None
    assert await state.get('checkpoint/' + claim['scope']) is None
    assert not await state.scan('item/' + claim['scope'] + '/')
    await engine.cancel(run['id'])


async def test_metadata_failure_rolls_back_all_publication(make_engine, state, monkeypatch):
    project, _ = keyed_project()
    engine = await make_engine(project)
    run = await engine.submit(['report'])
    await engine.execute_next()
    claim = await engine.claim()
    prepared = await engine.prepare(claim)
    result, _ = await engine.backend.execute(prepared['spec'])
    refs, receipts = await engine.stage_result(claim, result)
    original = Transaction.put
    async def fail_checkpoint(self, key, value):
        if key.startswith('checkpoint/transform/'):
            raise RuntimeError('Injected metadata transaction failure')
        await original(self, key, value)
    monkeypatch.setattr(Transaction, 'put', fail_checkpoint)
    with pytest.raises(RuntimeError, match='Injected'):
        await engine.commit(claim, prepared, result, refs, receipts)
    assert await state.get(head_key('parents', '')) is None
    assert await state.get(head_key('children', '')) is None
    assert not await state.scan('item/' + claim['scope'] + '/')
    assert (await engine.run_detail(run['id']))['request']['status'] == 'running'


async def test_commit_replay_checks_full_payload(make_engine):
    project, _ = keyed_project()
    engine = await make_engine(project)
    await engine.submit(['inventory'])
    claim = await engine.claim()
    prepared = await engine.prepare(claim)
    result, _ = await engine.backend.execute(prepared['spec'])
    refs, receipts = await engine.stage_result(claim, result)
    first = await engine.commit(claim, prepared, result, refs, receipts)
    assert (await engine.commit(claim, prepared, result, refs, receipts))['id'] == first['id']
    with pytest.raises(Conflict, match='different result'):
        await engine.commit(claim, prepared, {**result, 'cursor': 123}, refs, receipts)


@pytest.mark.parametrize('action', ['cancel', 'expire'])
async def test_stale_worker_cannot_publish(make_engine, state, action):
    project, _ = keyed_project()
    now = [1000.0]
    engine = await make_engine(project, clock=lambda: now[0], lease_seconds=10)
    run = await engine.submit(['inventory'])
    claim = await engine.claim()
    prepared = await engine.prepare(claim)
    result, _ = await engine.backend.execute(prepared['spec'])
    refs, receipts = await engine.stage_result(claim, result)
    if action == 'cancel':
        await engine.cancel(run['id'])
    else:
        now[0] += 11
        await engine.recover_expired()
        new = await engine.claim()
        assert new['generation'] > claim['generation']
    with pytest.raises(LostOwnership):
        await engine.commit(claim, prepared, result, refs, receipts)
    assert await state.get(head_key('inventory', '')) is None


async def test_concurrent_claims_have_one_scope_owner(make_engine):
    project, _ = keyed_project()
    engine = await make_engine(project)
    await engine.submit(['inventory'])
    await engine.submit(['inventory'])
    claims = await asyncio.gather(*[engine.claim() for _ in range(8)])
    assert sum(c is not None for c in claims) == 1


async def test_request_idempotency_and_conflict(make_engine):
    project, _ = keyed_project()
    engine = await make_engine(project)
    runs = await asyncio.gather(*[engine.submit(['inventory'], command_id='request-id') for _ in range(4)])
    assert len({r['id'] for r in runs}) == 1
    with pytest.raises(Conflict):
        await engine.submit(['parents'], command_id='request-id')
    assert len(await engine.list_runs()) == 1


async def test_stale_upstream_cannot_overwrite_newer_data(make_engine):
    project, source = keyed_project()
    engine = await make_engine(project)
    await engine.submit(['report'])
    await engine.execute_next()
    old = await engine.claim()
    prepared = await engine.prepare(old)
    result, _ = await engine.backend.execute(prepared['spec'])
    refs, receipts = await engine.stage_result(old, result)
    source['rows'][0]['revision'] = 'new'
    newer = await engine.submit(['inventory'])
    assert (await finish(engine, newer))['request']['status'] == 'succeeded'
    with pytest.raises(Conflict, match='Upstream'):
        await engine.commit(old, prepared, result, refs, receipts)


async def test_duplicate_inventory_fails_before_acknowledgement(make_engine, state):
    project, source = keyed_project()
    source['rows'].append(copy.deepcopy(source['rows'][0]))
    engine = await make_engine(project)
    assert (await finish(engine, await engine.submit(['report'])))['request']['status'] == 'failed'
    assert not await state.scan('item/transform/')


async def test_empty_key_does_not_collide_with_underscore(make_engine):
    project, source = keyed_project()
    source['rows'] = [{'id': '', 'revision': 1, 'count': 1}, {'id': '_', 'revision': 1, 'count': 2}]
    engine = await make_engine(project)
    await finish(engine, await engine.submit(['report']))
    detail = await finish(engine, await engine.submit(['report']))
    assert next(t for t in detail['tasks'] if t['producer'] == 'transform')['status'] == 'skipped'
    assert len((await engine.asset_detail('children'))['preview']) == 3


async def test_cursor_and_append_deduplication(make_engine):
    payload = {'batch': 'one', 'rows': [{'n': 1}], 'cursor': 1}
    @asset
    def events(ctx):
        return Batch({'events': AppendBatch(payload['batch'], payload['rows'])}, cursor=payload['cursor'])
    engine = await make_engine(Project([events]))
    await finish(engine, await engine.submit(['events']))
    await finish(engine, await engine.submit(['events']))
    assert (await engine.asset_detail('events'))['preview'] == [{'n': 1}]
    assert (await engine.asset_detail('events'))['checkpoint']['cursor'] == 1
    payload.update(batch='two', rows=[{'n': 2}], cursor=2)
    await finish(engine, await engine.submit(['events']))
    assert (await engine.asset_detail('events'))['preview'] == [{'n': 1}, {'n': 2}]
    payload['rows'] = [{'n': 999}]
    assert (await finish(engine, await engine.submit(['events'])))['request']['status'] == 'failed'
    assert (await engine.asset_detail('events'))['checkpoint']['cursor'] == 2


async def test_upsert_deletes_and_conflicting_primary_keys(make_engine):
    mutation = {'rows': [{'id': 1, 'x': 1}, {'id': 2, 'x': 2}], 'deletes': []}
    @asset
    def rows():
        return Upsert(['id'], mutation['rows'], mutation['deletes'])
    engine = await make_engine(Project([rows]))
    await finish(engine, await engine.submit(['rows']))
    mutation.update(rows=[{'id': 2, 'x': 20}], deletes=[[1]])
    await finish(engine, await engine.submit(['rows']))
    assert (await engine.asset_detail('rows'))['preview'] == [{'id': 2, 'x': 20}]
    mutation['rows'] *= 2
    assert (await finish(engine, await engine.submit(['rows'])))['request']['status'] == 'failed'
    assert (await engine.asset_detail('rows'))['preview'] == [{'id': 2, 'x': 20}]


async def test_partitions_pause_resume_and_independent_checkpoints(make_engine):
    @asset(partitions='daily')
    def daily(ctx):
        return Batch({'daily': [{'date': ctx.partition}]}, cursor=ctx.partition)
    @asset
    def live():
        return Batch({'live': 42}, cursor='live-token')
    engine = await make_engine(Project([daily, live]))
    await finish(engine, await engine.submit(['live']))
    live_before = (await engine.asset_detail('live'))['checkpoint']
    run = await engine.submit(['daily'], partitions=['2026-01-01', '2026-01-02'])
    await engine.pause(run['id'])
    assert await engine.claim() is None
    await engine.pause(run['id'], False)
    assert (await finish(engine, run))['request']['status'] == 'succeeded'
    for p in ['2026-01-01', '2026-01-02']:
        assert (await engine.asset_detail('daily', p))['checkpoint']['cursor'] == p
    assert (await engine.asset_detail('live'))['checkpoint'] == live_before
    again = await finish(engine, await engine.submit(['daily'], partitions=['2026-01-01'], mode='fill_missing'))
    assert all(t['status'] == 'skipped' for t in again['tasks'])


async def test_automation_request_and_cursor_are_atomic(make_engine, state, monkeypatch):
    @asset
    def source():
        return 1
    now = [1000.0]
    engine = await make_engine(Project([source], automations=[Automation('tick', ('source',), 60, True)]), clock=lambda: now[0])
    now[0] += 61
    original = Transaction.put
    async def fail_cursor(self, key, value):
        if key == 'automation/tick':
            raise RuntimeError('Injected automation failure')
        await original(self, key, value)
    monkeypatch.setattr(Transaction, 'put', fail_cursor)
    with pytest.raises(RuntimeError):
        await engine.evaluate_automations()
    assert await engine.list_runs() == []
    assert (await state.get('automation/tick'))['next_at'] == 1060
    monkeypatch.setattr(Transaction, 'put', original)
    await asyncio.gather(engine.evaluate_automations(), engine.evaluate_automations())
    assert len(await engine.list_runs()) == 1
    assert (await state.get('automation/tick'))['next_at'] == 1121


async def test_scope_encoding_is_unambiguous():
    assert scope('asset', '') != scope('asset', '_')
    assert scope('asset', 'a/b') != scope('asset/a', 'b')
