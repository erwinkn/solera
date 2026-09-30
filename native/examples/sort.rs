//! Sorting written keys, strategy against strategy:
//! `cargo run --release --example sort -- 1000000 [ids|uuids|paths] [strategy,...]`.
//!
//! Keys sit in one packed buffer with `u32` offsets, as in an Arrow string
//! column, in random order. Reports each strategy's time and the heap it
//! adds at its peak (a counting allocator), on top of the keys themselves.

use solera_native::sort::{self, Keys};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let now = LIVE.fetch_add(l.size(), Relaxed) + l.size();
        PEAK.fetch_max(now, Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Relaxed);
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if new > l.size() {
            let now = LIVE.fetch_add(new - l.size(), Relaxed) + new - l.size();
            PEAK.fetch_max(now, Relaxed);
        } else {
            LIVE.fetch_sub(l.size() - new, Relaxed);
        }
        System.realloc(p, l, new)
    }
}

#[global_allocator]
static A: Counting = Counting;

struct Packed {
    values: Vec<u8>,
    offsets: Vec<u32>,
}

impl Keys for Packed {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }
    fn key(&self, i: usize) -> &[u8] {
        &self.values[self.offsets[i] as usize..self.offsets[i + 1] as usize]
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args
        .get(1)
        .map_or(1_000_000, |a| a.parse::<f64>().unwrap() as usize);
    let shape = args.get(2).map_or("ids", |s| s.as_str());
    let all = "sorted_check,perm,perm_par,pairs,pairs_par,pairs_lsd,pairs_msd_par,buckets_par";
    let which: Vec<&str> = args.get(3).map_or(all, |s| s.as_str()).split(',').collect();

    let mut x: u64 = 0x9E3779B97F4A7C15;
    let mut rnd = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    // Unique keys, then shuffled. Built in two big buffers, not one allocation
    // per key: freeing millions of small ones would bill the allocator's
    // cleanup to whichever strategy runs first.
    let gap = (1_000_000_000_000u64 / n as u64).max(2);
    let mut id = 0u64;
    let mut made = Packed {
        values: Vec::new(),
        offsets: vec![0],
    };
    for i in 0..n {
        use std::io::Write;
        id += 1 + rnd() % (2 * gap);
        let v = &mut made.values;
        match shape {
            "uuids" => {
                let (a, b) = (rnd(), rnd());
                let tail = (b & 0xFFFF_FFFF_FFFF) ^ i as u64;
                let (hi, mid, lo, b16) = (a >> 32, (a >> 16) & 0xFFFF, a & 0xFFF, b >> 48);
                write!(v, "{hi:08x}-{mid:04x}-4{lo:03x}-{b16:04x}-{tail:012x}").unwrap()
            }
            "paths" => write!(v, "site-{:05}/file-{:09}", i % 20_000, id).unwrap(),
            _ => write!(v, "cust-{:013}", id).unwrap(),
        }
        made.offsets.push(v.len() as u32);
    }
    let mut order: Vec<u32> = (0..n as u32).collect();
    for i in (1..n).rev() {
        order.swap(i, (rnd() % (i as u64 + 1)) as usize);
    }
    let mut packed = Packed {
        values: Vec::with_capacity(made.values.len()),
        offsets: Vec::with_capacity(n + 1),
    };
    packed.offsets.push(0);
    for &i in &order {
        packed.values.extend_from_slice(made.key(i as usize));
        packed.offsets.push(packed.values.len() as u32);
    }
    drop((made, order));
    let base = LIVE.load(Relaxed);
    println!(
        "{n} {shape} keys, {:.1} B/key packed, common prefix {} B",
        base as f64 / n as f64,
        sort::common_prefix(&packed)
    );

    for name in which {
        PEAK.store(LIVE.load(Relaxed), Relaxed);
        let t = Instant::now();
        let order = match name {
            "sorted_check" => {
                assert_eq!(sort::is_sorted(&packed), Ok(false));
                Vec::new()
            }
            "perm" => sort::perm(&packed),
            "perm_par" => sort::perm_par(&packed),
            "pairs" => sort::pairs(&packed),
            "pairs_par" => sort::pairs_par(&packed),
            "pairs_lsd" => sort::pairs_lsd(&packed),
            "pairs_msd_par" => sort::pairs_msd_par(&packed),
            "buckets_par" => sort::buckets_par(&packed),
            other => panic!("unknown strategy {other}"),
        };
        let secs = t.elapsed().as_secs_f64();
        let extra = PEAK.load(Relaxed) - base;
        if !order.is_empty() {
            assert!(order
                .windows(2)
                .all(|w| packed.key(w[0] as usize) < packed.key(w[1] as usize)));
        }
        println!(
            "| {name} | {secs:.2} s | {:.1} B/key | {:.0} MB |",
            extra as f64 / n as f64,
            extra as f64 / 1e6
        );
    }
}
