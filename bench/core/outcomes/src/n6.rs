//! N6: retries with 1% of a 100M-key input's keys due and scattered. A
//! retry batch reads its due keys and walks nothing else: ≥ 100,000 due keys
//! a second; one batch alone ≤ 0.3 s warm (plans/build-plan.md, N6).
//!
//! `n6 <dir> <keys> build`: the input (every key, 100,000-key commits), one
//! failing key in 100 (a record each, retrying, due within the hour) and its
//! due index, then a day's churn on the input (1% of the keys, of which 1% are
//! failing keys: their retry is a change plan's).
//! `n6 <dir> <keys> run`: a retry pass by the clock over all of them,
//! 10,000-key batches through `due::plan`, then the first batches again warm.

use crate::*;
use solera_tracking::due;
use solera_tracking::event::{Event, Part};
use solera_tracking::outcomes::commit::due_key;
use solera_tracking::outcomes::transition::{Counters, Stored, RETRYING};
use solera_tracking::outcomes::{cond_id, output_id, Held as Kept, PerKey, Record, DUE, FAILURES};
use solera_tracking::progress::EdgeOpen;
use solera_tracking::Tracking;

const EDGE: &[u8] = b"checks/items";
const INPUT: &[u8] = b"items";
const T0: u64 = 1_762_000_000;

fn value(id: u64, gen: u64) -> [u8; 8] {
    splitmix(id ^ (gen << 40)).to_le_bytes()
}

fn now_conds() -> Arc<Conditions> {
    Arc::new(Conditions {
        definition: b"d1".as_slice().into(),
        ..Default::default()
    })
}

fn put(
    env: &Env,
    ix: &ChangeIndex,
    id: &[u8],
    out: &mut Output,
    ch: &Changes,
    m: &mut Merges,
    tag: &str,
) {
    let id: Box<[u8]> = id.into();
    let path = format!("{tag}/c{}.layer", out.head + 1);
    let spec = Spec {
        version: 0,
        time_ms: 0,
        producer: b"",
        path: &path,
        mode: Mode::Patch,
        ends_full_run: false,
        collect: 0,
    };
    let b = env
        .rt
        .block_on(build(&OneIndex(ix), &id, out, spec, &mut Held(Some(ch))))
        .unwrap()
        .expect("a change");
    out.layers.push(b.part.layer);
    out.head += 1;
    out.live = b.part.live;
    merge(env, ix, out, m, tag);
}

fn save_as(dir: &str, name: &str, out: &Output) {
    let mut e = Enc::new();
    e.uv(out.head);
    e.uv(out.live);
    e.uv(out.layers.len() as u64);
    for l in &out.layers {
        e.layer(l);
    }
    std::fs::write(format!("{dir}/{name}.state"), e.buf).unwrap();
}

fn load_as(dir: &str, name: &str) -> Output {
    let b = std::fs::read(format!("{dir}/{name}.state")).unwrap();
    let mut d = Dec::new(&b);
    let head = d.uv().unwrap();
    let live = d.uv().unwrap();
    let layers = (0..d.uv().unwrap()).map(|_| d.layer().unwrap()).collect();
    Output {
        layers,
        head,
        live,
        full_run: None,
    }
}

fn failing(env: &Env, ix: &ChangeIndex, n: u64, at: u64, m: &mut Merges) -> (Output, Output) {
    let mut k = Vec::new();
    let conds = now_conds();
    let cid = cond_id(&conds);
    let fid = output_id(FAILURES, EDGE);
    let did = output_id(DUE, EDGE);
    let mut failures = Output::default();
    let mut dues: Vec<Vec<u8>> = Vec::new();
    let failing = n / 100;
    for c in 0..failing / BATCH {
        let mut ch = Changes::new();
        for j in c * BATCH..(c + 1) * BATCH {
            let id = j * 100 + 37;
            k.clear();
            key_of(id, &mut k);
            let next_at = T0 + splitmix(id) % 3600;
            let r = Record {
                stored: Stored {
                    outcome: RETRYING,
                    tries: 1,
                    deploy: 1,
                    forced: 0,
                    since: T0 - 60,
                    last: T0 - 60,
                    next_at,
                    until: T0 + 86_400,
                    message: b"ConnectionResetError: [Errno 104] Connection reset by peer"
                        .as_slice()
                        .into(),
                },
                on: Ver::Bytes(value(id, 0).as_slice().into()),
                on_cond: cid,
                at,
                held: Kept::Absent,
            };
            ch.push(&k, Change::Upsert(Some(&r.encode()))).unwrap();
            dues.push(due_key(next_at, &k));
        }
        put(env, ix, &fid, &mut failures, &ch, m, "f");
    }
    dues.sort();
    let mut due = Output::default();
    for chunk in dues.chunks(BATCH as usize) {
        let mut ch = Changes::new();
        for d in chunk {
            ch.push(d, Change::Upsert(Some(b""))).unwrap();
        }
        put(env, ix, &did, &mut due, &ch, m, "d");
    }
    (failures, due)
}

fn build_all(env: &Env, ix: &ChangeIndex, dir: &str, n: u64) {
    let (t0, c0) = (Instant::now(), cpu());
    let mut m = Merges::default();
    let mut input = Output::default();
    let mut k = Vec::new();
    const STEP: u64 = 100_000;
    for c in 0..n / STEP {
        let mut ch = Changes::new();
        for id in c * STEP..(c + 1) * STEP {
            k.clear();
            key_of(id, &mut k);
            ch.push(&k, Change::Upsert(Some(&value(id, 0)))).unwrap();
        }
        put(env, ix, INPUT, &mut input, &ch, &mut m, "i");
        if (c + 1) % 100 == 0 {
            println!(
                "  input: {} keys, {} layers, {:.0}s",
                input.live,
                input.layers.len(),
                t0.elapsed().as_secs_f64()
            );
        }
    }
    let (failures, due) = failing(env, ix, n, input.head, &mut m);
    // A day's churn: 1% of the keys change, scattered.
    for c in 0..10u64 {
        let mut ids: Vec<u64> = (0..n / 1000)
            .map(|i| splitmix(c * 1_000_003 + i) % n)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        let mut ch = Changes::new();
        for id in ids {
            k.clear();
            key_of(id, &mut k);
            ch.push(&k, Change::Upsert(Some(&value(id, c + 1))))
                .unwrap();
        }
        put(env, ix, INPUT, &mut input, &ch, &mut m, "i");
    }
    println!(
        "build: input {} keys at c{} ({} layers, {:.0} MB), {} failing ({} layers, {:.1} MB), due index {} ({:.1} MB); {:.0}s, {:.0} CPU-s, merges {}",
        input.live,
        input.head,
        input.layers.len(),
        store_bytes(&input) as f64 / 1e6,
        failures.live,
        failures.layers.len(),
        store_bytes(&failures) as f64 / 1e6,
        due.live,
        store_bytes(&due) as f64 / 1e6,
        t0.elapsed().as_secs_f64(),
        cpu() - c0,
        m.n
    );
    save_as(dir, "input", &input);
    save_as(dir, "failures", &failures);
    save_as(dir, "due", &due);
}

/// The failing keys again, failed at commit `at` (`n6 <dir> <keys> fail <at>`):
/// at 1, before the input's last merge, a retry batch's window would read
/// the merged layer over its range.
fn refail(env: &Env, ix: &ChangeIndex, dir: &str, n: u64, at: u64) {
    let mut m = Merges::default();
    let (failures, due) = failing(env, ix, n, at, &mut m);
    save_as(dir, "failures", &failures);
    save_as(dir, "due", &due);
}

fn run(env: &Env, ix: &ChangeIndex, dir: &str) {
    let input = load_as(dir, "input");
    let failures = load_as(dir, "failures");
    let due_out = load_as(dir, "due");
    let conds = now_conds();
    let cid = cond_id(&conds);
    let mut t = Tracking::new();
    t.apply(
        &Event::of(vec![Part::EdgeOpen(EdgeOpen {
            edge: EDGE.into(),
            input: INPUT.into(),
            keyed: true,
        })])
        .encode(),
    )
    .unwrap();
    t.outputs.insert(INPUT.into(), input);
    t.outputs
        .insert(output_id(FAILURES, EDGE), failures.clone());
    t.outputs.insert(output_id(DUE, EDGE), due_out);
    let mut pk = PerKey::default();
    pk.conds = Arc::new([(cid, conds.clone())].into_iter().collect());
    pk.refs = [(cid, failures.live)].into_iter().collect();
    pk.counts[RETRYING as usize] = failures.live;
    pk.classes[0] = failures.live;
    t.per_key.insert(EDGE.into(), pk);
    let counters = Counters {
        deploy: 1,
        forced: [0; 6],
    };

    let s0 = ix.store.stats.counts();
    let (w0, c0) = (Instant::now(), cpu());
    let v = t.outcome_view(EDGE).unwrap();
    let list = t.retry_view(EDGE).unwrap().pass_list;
    let parts = env
        .rt
        .block_on(due::start(
            &OneIndex(ix),
            &v,
            &list,
            T0 + 3600,
            counters,
            "p/c1.layer",
        ))
        .unwrap()
        .expect("keys are due");
    t.apply(&Event::of(parts).encode()).unwrap();
    let s1 = ix.store.stats.counts();
    println!(
        "pass start: {} keys listed in {:.0} ms wall, {:.0} ms CPU; GETs {} ({:.1} MB)",
        t.retry_view(EDGE).unwrap().pass_list.live,
        w0.elapsed().as_secs_f64() * 1e3,
        (cpu() - c0) * 1e3,
        s1.gets - s0.gets,
        (s1.get_bytes - s0.get_bytes) as f64 / 1e6
    );

    let mut walls = vec![];
    let mut cpus = vec![];
    let (mut taken, mut skipped) = (0usize, 0usize);
    let mut afters = vec![];
    let (w0, c0) = (Instant::now(), cpu());
    let s0 = ix.store.stats.counts();
    loop {
        let r = t.retry_view(EDGE).unwrap();
        afters.push(r.outcomes.per_key.pass.as_ref().unwrap().after.clone());
        let (w, c) = (Instant::now(), cpu());
        let p = env
            .rt
            .block_on(due::plan(&OneIndex(ix), &r, &conds, BATCH as usize))
            .unwrap();
        walls.push(w.elapsed().as_secs_f64());
        cpus.push(cpu() - c);
        let n = p.inputs[0].keys.len();
        taken += n;
        skipped += BATCH as usize - n.min(BATCH as usize);
        let pk = t.per_key.get_mut(EDGE).unwrap();
        match p.range.hi {
            Some(hi) => pk.pass.as_mut().unwrap().after = Some(hi),
            None => break,
        }
    }
    let s1 = ix.store.stats.counts();
    let wall = w0.elapsed().as_secs_f64();
    let batches = walls.len();
    println!(
        "pass: {batches} batches, {taken} keys taken ({skipped} short of full batches: changed upstream since they failed), {:.1}s wall, {:.1} CPU-s = {:.0} due keys a second; batch p50 {:.0} ms (CPU {:.0}), p99 {:.0} ms, max {:.0} ms; GETs {} ({:.0} MB); peak RSS {:.0} MB",
        wall,
        cpu() - c0,
        taken as f64 / wall,
        pct(&mut walls.clone(), 0.5) * 1e3,
        pct(&mut cpus.clone(), 0.5) * 1e3,
        pct(&mut walls.clone(), 0.99) * 1e3,
        pct(&mut walls.clone(), 1.0) * 1e3,
        s1.gets - s0.gets,
        (s1.get_bytes - s0.get_bytes) as f64 / 1e6,
        status_mb("VmHWM"),
    );
    // Warm: the same batches again, alone.
    for (i, after) in afters.iter().take(5).enumerate() {
        let mut ws = vec![];
        let mut cs = vec![];
        let s0 = ix.store.stats.counts();
        for _ in 0..5 {
            t.per_key
                .get_mut(EDGE)
                .unwrap()
                .pass
                .as_mut()
                .unwrap()
                .after = after.clone();
            let r = t.retry_view(EDGE).unwrap();
            let (w, c) = (Instant::now(), cpu());
            env.rt
                .block_on(due::plan(&OneIndex(ix), &r, &conds, BATCH as usize))
                .unwrap();
            ws.push(w.elapsed().as_secs_f64());
            cs.push(cpu() - c);
        }
        let s1 = ix.store.stats.counts();
        println!(
            "batch {i} warm: {:.0} ms wall, {:.0} ms CPU (median of 5); GETs {} over 5",
            pct(&mut ws, 0.5) * 1e3,
            pct(&mut cs, 0.5) * 1e3,
            s1.gets - s0.gets
        );
    }
}

pub fn n6(dir: &str, n: u64, phase: &str) {
    for d in ["i", "f", "d", "p", "o"] {
        std::fs::create_dir_all(format!("{dir}/{d}")).unwrap();
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let env = Env {
        rt,
        raw: Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
    };
    let _rt = env.rt.enter();
    let ix = env.index();
    match phase {
        "build" => build_all(&env, &ix, dir, n),
        "run" => run(&env, &ix, dir),
        "fail" => refail(
            &env,
            &ix,
            dir,
            n,
            std::env::args().nth(5).unwrap().parse().unwrap(),
        ),
        _ => panic!("phase: build | run | fail <at>"),
    }
}
