//! The layer schedule both formats share (the brief's §4, decision D5): one
//! layer per commit; merge four adjacent layers, newest first, when the largest
//! is at most 30× the smallest and the result stays within the size rule
//! (at most 4× all newer layers, plus 1 MiB); past 32 layers, merge the newest
//! adjacent pair that fits; fold layers below min(horizon, preserved) into the
//! base once they reach a quarter of it. Sizes are entries × `BPE`, so the
//! structure does not depend on the format measured.

pub const BPE: u64 = 12; // bytes per entry, for the rules only
const MIB: u64 = 1 << 20;

#[derive(Clone, Debug)]
pub struct Spec {
    pub lo: u64,
    pub hi: u64,
    pub base: bool,
    pub entries: u64,
}

pub struct Sched {
    pub layers: Vec<Spec>, // the base first, then oldest to newest
    pub committed: u64,
    pub merged: u64, // entries written by merges and folds
    pub folds: u64,
    pub max_layers: usize,
}

fn bytes(e: u64) -> u64 {
    e * BPE + 1024 // a file's own overhead
}

/// `counts[c]`: the entries of commit c; the base holds the state at `load`.
pub fn schedule(
    counts: &[u64],
    load: u64,
    live: impl Fn(u64) -> u64,
    per_day: u64,
    retention_days: u64,
    preserved: Option<u64>,
) -> Sched {
    let mut s = Sched {
        layers: vec![Spec {
            lo: 0,
            hi: load,
            base: true,
            entries: live(load),
        }],
        committed: 0,
        merged: 0,
        folds: 0,
        max_layers: 1,
    };
    for c in load + 1..counts.len() as u64 {
        s.layers.push(Spec {
            lo: c,
            hi: c,
            base: false,
            entries: counts[c as usize],
        });
        s.committed += counts[c as usize];
        loop {
            let l = &s.layers[1..];
            let n = l.len();
            let fits = |a: usize, b: usize| {
                let size: u64 = l[a..b].iter().map(|x| bytes(x.entries)).sum();
                let newer: u64 = l[b..].iter().map(|x| bytes(x.entries)).sum();
                size <= 4 * newer + MIB
            };
            let mut pick = None;
            if n >= 4 {
                for a in (0..=n - 4).rev() {
                    let sz: Vec<u64> = l[a..a + 4].iter().map(|x| bytes(x.entries)).collect();
                    if *sz.iter().max().unwrap() <= 30 * *sz.iter().min().unwrap() && fits(a, a + 4)
                    {
                        pick = Some((a, 4));
                        break;
                    }
                }
            }
            if pick.is_none() && n > 32 {
                pick = (0..n - 1).rev().find(|&a| fits(a, a + 2)).map(|a| (a, 2));
            }
            let Some((a, k)) = pick else { break };
            let group = &s.layers[1 + a..1 + a + k];
            let m = Spec {
                lo: group[0].lo,
                hi: group[k - 1].hi,
                base: false,
                entries: group.iter().map(|x| x.entries).sum(),
            };
            s.merged += m.entries;
            s.layers.splice(1 + a..1 + a + k, [m]);
        }
        let horizon = c.saturating_sub(retention_days * per_day);
        let point = preserved.map_or(horizon, |p| p.min(horizon));
        let fold: Vec<usize> = (1..s.layers.len())
            .take_while(|&i| s.layers[i].hi <= point)
            .collect();
        if let Some(&last) = fold.last() {
            let size: u64 = fold.iter().map(|&i| bytes(s.layers[i].entries)).sum();
            if size * 4 >= bytes(s.layers[0].entries) {
                let hi = s.layers[last].hi;
                let base = Spec {
                    lo: 0,
                    hi,
                    base: true,
                    entries: live(hi),
                };
                s.merged += base.entries;
                s.folds += 1;
                s.layers.splice(0..=last, [base]);
            }
        }
        s.max_layers = s.max_layers.max(s.layers.len());
    }
    s
}
