//! Logs (`solera_tracking::log`): rows appended commit by commit to an
//! output of the change index, and a reader that consumes them by commit.

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use solera_index::{ChangeIndex, NewCommit, Store, StoreOptions, WriteOptions};
use solera_tracking::event::{Event, Part};
use solera_tracking::plan::{self, LogPlan, LogRequest, Mode, NoHold, OneIndex, Request};
use solera_tracking::progress::EdgeOpen;
use solera_tracking::retain::{self, Holder};
use solera_tracking::{log, Error, Id, Tracking};

const EVENTS: &[u8] = b"events";
const EDGE: &[u8] = b"digest/events";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn index(rt: &tokio::runtime::Runtime) -> ChangeIndex {
    let store = Store::new(
        Arc::new(InMemory::new()),
        Path::from("t"),
        rt.handle().clone(),
        StoreOptions::default(),
    )
    .unwrap();
    ChangeIndex::new(store, WriteOptions::default())
}

fn apply(t: &mut Tracking, part: Part) {
    t.apply(&Event::of(vec![part]).encode()).unwrap();
}

/// A log and its one reader.
struct World {
    ix: ChangeIndex,
    t: Tracking,
    files: u64,
}

impl World {
    fn new(rt: &tokio::runtime::Runtime) -> World {
        let mut t = Tracking::new();
        apply(
            &mut t,
            Part::EdgeOpen(EdgeOpen {
                edge: EDGE.into(),
                input: EVENTS.into(),
                keyed: false,
            }),
        );
        World {
            ix: index(rt),
            t,
            files: 0,
        }
    }

    /// Appends one commit of `rows`, each its own part.
    async fn append(&mut self, rows: &[&str], clear: bool) {
        let o = &self.t.outputs[EVENTS];
        self.files += 1;
        let commit = NewCommit {
            commit: o.head + 1,
            time_ms: 0,
            producer: b"",
            live_before: o.live,
            clear,
        };
        let path = format!("events/{}", self.files);
        let layer = log::write(&self.ix, &path, commit, rows).await.unwrap();
        let part = self.t.log_commit(EVENTS, layer, clear).unwrap();
        apply(&mut self.t, Part::OutputCommit(part));
    }

    fn plan(&self, limit: u64, reset: bool) -> Result<plan::Plan, Error> {
        let req = Request {
            mode: Mode::Incremental,
            reset,
            after: None,
            limit: 1,
            inputs: vec![],
            logs: vec![LogRequest {
                edge: EDGE.into(),
                limit,
            }],
        };
        let view = self.t.view(&req)?;
        futures(plan::plan(&view, &req, &OneIndex(&self.ix), &NoHold))
    }

    fn commit(&mut self, p: &plan::Plan) -> Result<(), Error> {
        let ops = self.t.progress_ops(p, &[], b"", None)?;
        if !ops.parts.is_empty() {
            self.t.apply(&ops.event().encode())?;
        }
        Ok(())
    }

    /// The rows a plan reads, in order.
    async fn rows(&self, l: &LogPlan) -> Vec<String> {
        let o = &self.t.outputs[EVENTS];
        let (mut out, mut after) = (vec![], None);
        loop {
            let (parts, next) = log::read(&self.ix, &o.layers, l.head, (l.from, l.to), after, 2)
                .await
                .unwrap();
            out.extend(
                parts
                    .into_iter()
                    .map(|(_, b)| String::from_utf8(b).unwrap()),
            );
            match next {
                Some(n) => after = Some(n),
                None => return out,
            }
        }
    }
}

/// A plan does no I/O for a log: it resolves at once.
fn futures<T>(f: impl std::future::Future<Output = T>) -> T {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    match std::pin::pin!(f).poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        std::task::Poll::Pending => panic!("a plan of logs alone waited"),
    }
}

fn log_of(p: &plan::Plan) -> &LogPlan {
    &p.logs[0]
}

fn held(t: &Tracking) -> (u64, bool) {
    let l = t.edges[EDGE].progress.log().unwrap();
    (l.through, l.walking)
}

#[test]
fn a_reader_gets_the_commits_after_the_last_it_read_a_batch_at_a_time() {
    let rt = runtime();
    rt.block_on(async {
        let mut w = World::new(&rt);
        for n in 0..5 {
            w.append(&[&format!("a{n}"), &format!("b{n}")], false).await;
        }
        // batches of two commits: the walk goes on until one reaches the head
        let mut seen = vec![];
        for (range, walking) in [((0, 2), true), ((2, 4), true), ((4, 5), false)] {
            let p = w.plan(2, false).unwrap();
            let l = log_of(&p);
            assert_eq!((l.from, l.to), range);
            assert_eq!(l.more(), walking);
            seen.extend(w.rows(l).await);
            w.commit(&p).unwrap();
            assert_eq!(held(&w.t), (range.1, walking));
        }
        let all: Vec<String> = (0..5)
            .flat_map(|n| [format!("a{n}"), format!("b{n}")])
            .collect();
        assert_eq!(seen, all);
        // nothing owed: the plan reads nothing, and its commit changes nothing
        let p = w.plan(2, false).unwrap();
        assert!(!log_of(&p).owed());
        w.commit(&p).unwrap();
        assert_eq!(held(&w.t), (5, false));
        // new commits are owed without a walk: the reader held the whole log once
        w.append(&["c"], false).await;
        w.append(&["d"], false).await;
        let p = w.plan(1, false).unwrap();
        assert_eq!(w.rows(log_of(&p)).await, ["c"]);
        w.commit(&p).unwrap();
        assert_eq!(held(&w.t), (6, false));
    });
}

#[test]
fn a_log_that_starts_over_starts_its_reader_over() {
    let rt = runtime();
    rt.block_on(async {
        let mut w = World::new(&rt);
        for row in ["a", "b", "c"] {
            w.append(&[row], false).await;
        }
        let p = w.plan(10, false).unwrap();
        w.commit(&p).unwrap();
        w.append(&["d"], false).await;
        let stale = w.plan(10, false).unwrap();
        // the log starts over at commit 5, with one row
        w.append(&["x"], true).await;
        w.append(&["y"], false).await;
        // a batch planned before the clear read rows the log no longer holds
        assert!(matches!(w.commit(&stale), Err(Error::Unsettled)));
        // the reader starts over, which is its caller's to ask for
        assert!(matches!(w.plan(10, false), Err(Error::Unsettled)));
        let p = w.plan(1, true).unwrap();
        let l = log_of(&p);
        assert_eq!((l.from, l.to, l.walking), (4, 5, true));
        assert_eq!(w.rows(l).await, ["x"]);
        w.commit(&p).unwrap();
        let p = w.plan(1, false).unwrap();
        assert_eq!(w.rows(log_of(&p)).await, ["y"]);
        w.commit(&p).unwrap();
        assert_eq!(held(&w.t), (6, false));
    });
}

#[test]
fn a_new_reader_reads_what_the_log_holds_now() {
    let rt = runtime();
    rt.block_on(async {
        let mut w = World::new(&rt);
        w.append(&["a"], false).await;
        w.append(&["b"], true).await;
        w.append(&["c"], false).await;
        // it never read the rows the clear dropped: no start-over, and no batch spent on them
        let p = w.plan(1, false).unwrap();
        let l = log_of(&p);
        assert_eq!((l.from, l.to), (1, 2));
        assert_eq!(w.rows(l).await, ["b"]);
    });
}

#[test]
fn a_reader_s_place_holds_the_cut() {
    let rt = runtime();
    rt.block_on(async {
        let mut w = World::new(&rt);
        w.append(&["a"], false).await;
        w.append(&["b"], false).await;
        assert_eq!(retain::cut(&w.t, EVENTS, &[]).by, Holder::Head);
        let p = w.plan(1, false).unwrap();
        w.commit(&p).unwrap();
        // a merge folds nothing above commit 1: a clear after it stays in the layers
        let cut = retain::cut(&w.t, EVENTS, &[]);
        assert_eq!((cut.commit, cut.by), (1, Holder::Piece(Id::from(EDGE))));
    });
}

#[test]
fn a_log_s_commit_is_the_one_after_its_head() {
    let rt = runtime();
    rt.block_on(async {
        let mut w = World::new(&rt);
        w.append(&["a"], false).await;
        let commit = |n| NewCommit {
            commit: n,
            time_ms: 0,
            producer: b"",
            live_before: 1,
            clear: false,
        };
        let late = log::write(&w.ix, "events/late", commit(1), &["b"])
            .await
            .unwrap();
        assert!(w.t.log_commit(EVENTS, late, false).is_err());
        let next = log::write(&w.ix, "events/next", commit(2), &["b", "c"])
            .await
            .unwrap();
        let part = w.t.log_commit(EVENTS, next, false).unwrap();
        assert_eq!(part.live, 3);
        // a commit that appends nothing is no commit, unless it starts the log over
        let none: &[&str] = &[];
        assert!(log::write(&w.ix, "events/none", commit(2), none)
            .await
            .is_err());
        let clear = NewCommit {
            clear: true,
            ..commit(2)
        };
        let cleared = log::write(&w.ix, "events/clear", clear, none)
            .await
            .unwrap();
        assert_eq!(w.t.log_commit(EVENTS, cleared, true).unwrap().live, 0);
    });
}
