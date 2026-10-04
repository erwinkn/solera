# Stamped runs, phase 1 evidence

For `docs/key-index-from-first-principles.md` (W57, T31). Bench code only.

| File | What | Run |
|---|---|---|
| `codec/` | measured: stamped entries against the minimal delta (bytes per entry, block index, decode, a k-way merge), zstd-1, 16 KiB blocks, one core; output in `codec/results.txt` | `cargo run --release -- 400000` |
| `check.py` | checked: random histories, random merges under a moving cut, every Δ(P, H) with P ≥ cut (and P = −∞, and the cursor walk) key by key against the per-commit fold | `python3 check.py 3000` (~30 s) |
| `model.py` | replayed: the policy on run metadata with `spans.py`'s density model; runs, writes, PUTs, storage, catch-up reads, cold lookups, full scans | `python3 model.py --sizes 1e6,1e8 --use floor` |
| `runs/` | the replay logs the doc cites (`none`, `floor`, `set`: what of the live P set the cut and merges use; `budget`: one merge per lane at 2M entries per commit; `z4`, `f8`: parameter variants) | |
| `report.py` | `results.md` from `runs/` | `python3 report.py > results.md` |
| `handoff.md` | choices, alternatives, and what phase 2 should check first | |

Every server run was capped: `systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400%` (the
replays at 100% each, at most four at once).
