# A31 independent review: the stamped-layers key index (T31, W57)

Reviewed revision: **git:df9ac17** on `design/key-index-fp` (design doc
`docs/key-index-from-first-principles.md`; prototype `native/src/layers.rs`,
`bench/keys/fp/layers.py`, `check.py`, `test_layers.py`, `handoff.md`). Read through
Git objects exported to `/tmp/a31/src`; the review worktree (`bb/w58-...`, at
main `ec85502`) stayed clean; nothing edited, installed or pushed. Also read:
`docs/observed-set.md` on main (`088f25e`), the A17 and A25 checklists, D149-D156.

## Verdict

**Build, with the listed changes.** The algebra (presence at P = presence at H
flipped once per flip after P) is exact on the real kernels for every P from the
cut to the head, straddled and merged layers, indexed deltas and churn included;
the lifecycle rules carry A17's and A25's fixes correctly where they apply. One
query form gives wrong results (R1), the spec on main and the design disagree on
what the observed set reads (R2), and the window can strand a live reader (R3).
Each is cheap to fix before T33.

## Findings

### R1 [P1] Query form 3 with a P (lookup over a sorted key list) is wrong in both directions
`bench/keys/fp/layers.py:410-430` (`Reader.lookup(keys, p)`); design "Query forms" 3.

The lookup reads **main parts only** and returns the newest entry's `(present, stamp)`
with no presence at P. History 1: `k0` is in the base; a reader observes at commit 5
(`k0` present); commit 7 removes `k0`; the base absorbs commits 1-10 with the cut at 5,
so `k0` lands in the graveyard (the base's side part). `page(5)` reports `k0`
removed; `lookup([k0], p=5)` returns `None` ("unchanged"). The reader keeps a deleted
key. History 2: `k` added at 2, observed at 3, removed at 4, layers [1, 6] merged
(`k` absent at both ends: the layer's side part): same miss. The exhaustive probe
also shows the other direction: a tombstone stamped after P for a key absent at P
and at H is returned as "changed, absent now" (a spurious removal) because the
"absent at both ends" rule needs at_P. Over 228K lookups: 16,356 disagreements with
the fold; the paged form over the same histories: 0.
**Fix:** make form 3 the same read as form 2 restricted to the key list (the
straddler's side part and the base's graveyard included, at_P from the flips,
"neither" dropped), and test it against the fold; or remove form 3 with a P from
the design and have `keys=`/point re-checks use head lookups (P = −∞) compared
against the observation record. `docs/observed-set.md` ("points are one key-list
call") must then say which.

### R2 [P2] `docs/observed-set.md` on main specifies three reads the adopted index does not serve
`docs/observed-set.md:187-220`, `:318-324`, `:332-334`; design "What the design does not support".

(a) The before-image is specified at the cut `C` ("the keys changed in (P, C]",
"the key view at C"); the design serves only Δ(P, head) and relabels the base to
that head (D151 settled it; main's spec was not updated). (b) The before-image is
specified with "its version or payload at P"; the index has no version at P (the
design's redundant-update fallback covers it: a key is in the before-image because
it changed, so presence at P suffices). (c) After a pattern change the candidates
are "scanned in each layer's index state and at now", i.e. a membership scan at a
past endpoint, which the design lists as unsupported. It is not needed: presence at
P of any key under the prefix = its presence at H, flipped by Δ(P, H, prefix)'s
flips, so one head scan under the prefix plus one Δ(P, H, prefix) answers it.
A full compare ("reads the upstream at P and now") is the same composition over the
whole range. **Fix:** rewrite those passages against the Δ(P, head) interface
(before-image = Δ(P, head) with presence at P only; membership and full compares as
head scan + Δ), and change the design's acceptance claim from "no caller needs
scans at an old commit" to "callers compose them from Δ and head scans". Settle
before the rebuild and T33 integrate.

### R3 [P2] The window can move the cut over a live P; the design relies on an unstated ordering
`layers.py:535-543` (`set_cut`); design "Retention".

The cut is "the newer of the window's edge and the oldest live P". Reader R at
P = 5, W = 10; the head reaches 30 before R's fold completes (a fold is a paged
Δ(P, head), ~950 pages at 100M). `upkeep(oldest_p=5)` sets the cut to 20;
R's next batch and R's own fold both raise `CutError`: R cannot be folded any more
and must start over as a new consumer (P = −∞, a full scan). The design's "folded
first" is an engine obligation nothing enforces. **Fix:** define the cut as the
oldest live P only, and let the window *trigger* folds (which raise the oldest
live P); then "P ≥ cut" is an invariant and a `CutError` is a real alarm, never a
scheduling race. Reproduced: `probes/lifecycle.log` (B).

### R4 [P3] The delta's change kinds are load-bearing and unverified
A kind is relative to the exact head state (`resolve` at `layers.py:512`). A stale
warm resolver (engine cache behind a manifest change) that writes "added" for a
present key puts one wrong flip in the log, and every reader's presence at P for
that key is inverted until the cut passes the commit. **Fix:** invalidate the
engine's resolver on every manifest change, assert generation monotonicity at
commit, and add a merge-time self-check: for a merge whose inputs all lie above the
cut, `present == start XOR odd(flips)` per key; alarm on violation (free: the merge
holds all three).

### R5 [P3] Design features the prototype does not have, not all listed as deviations
- **Automaton block pruning** for globs is in the design and in the A25 checklist
  row ("the automaton seek is checked against Solera's matcher"), but the prototype
  only prunes by literal prefix; `test_globs_match_soleras_matcher` and the "A25
  cases" test the *matcher*, not the interval test where A25 R1/R2 failed.
  Production must property-test the interval test on short variable-length keys.
- **No byte budget on a page** (design: "a pattern that matches few keys cannot
  make one batch read the whole index"): `page()` loops until `limit` matches or
  the end, and `Reader._blocks` caches every fetched block for the reader's life
  (129 MB for a far reader at 100M; the whole index under a selective filter).
- **Form 2 with a pattern and P ≠ −∞** is unexercised; `page()`'s `pattern` is a
  block-interval predicate with key filtering left to the caller.
- `Reader.page` has no upper key bound; the observed set's per-range
  `changes(H, now)` needs one (`layers_scan` already takes `upto`).
- Attempt/stopped keys are input ids without the life (`_key`), while the design
  says "keyed by life and input layer names" (A17 R8's new-life rule).
- The two upkeep lanes "never overlap" by assertion only; no rule picks disjoint
  inputs. Harmless (the second publication is refused, one wasted upload), but state it.
- Deleting an old life's objects after a reset is unspecified.

### Observations (no action beyond noting)
- **The 1M builds ran on the pre-rework merge kernel (3470ecb)**; the final kernels
  (5516d62, abe283e) are validated by the 200-history soak and by this review's
  exhaustive probe at toy block sizes, and by re-running the 1M *reads*. W53's 1M
  build is the first real-size run of the final merge kernel; the doc says so.
- **Stream-versus-seek crossover** (prototype model, 1K random keys): parts up to
  ~200 MB are streamed (1 wave, ≤ 13 range GETs), parts ≥ ~400 MB are sought
  (~1 GET per key); a 1.25 GB/s NIC term (the design's stated model, absent from
  `_plan`) does not move the crossover. The 1M cold-lookup row trails two views in
  bytes because streaming the 9 MB base beats ~475 seeks under this model; on a real
  NIC that is the right call.
- **Simplicity (check 6):** at A25's grouping, stamped layers have five or six
  structural concepts (commit delta = a layer; stamped entry with flips and start
  bit; main/side parts and the graveyard; the cut; the pinned manifest; the layer
  index with tiers and the size rule) against spans' five and two views' seven, but
  far fewer rules: no endpoint set, reservations, read rule per endpoint, clipping
  at g(N+1), covers or before/after states. The per-layer side parts (phase 2) are
  the one added rule, bought 3.4× → 3.1× under churn at 1M; keep them as a measured
  option until 100M churn numbers justify them.
- Pins (`seq` rule), epoch-scoped orphan collection, the barrier before deletion,
  durable attempt accounting, publication refusal of replaced inputs or another
  life, and crash-between-upload-and-publication recovery all behaved as designed in
  the tests and in probe A (`lifecycle.log`).

## Verification and runtime qualification

- The exact df9ac17 `native/src/layers.rs` (sha256 `3038204776789c18…`) was compiled
  alone as `solera._native` in `/tmp/a31/mini` (pyo3 0.29, zstd 0.13, crc32fast; 71 s
  under `systemd-run` 300% CPU, 6 GB). The donor binary in W57's worktree was not
  used: built at 21:25:16, before 5516d62/abe283e, and that worktree's layers.rs has
  since diverged.
- Runtime: `/tmp/a31/py` holds the extension plus byte-exact copies of
  `solera/patterns.py`, `solera/objects.py`, `solera/keys/io.py` and empty package
  inits; the reviewed `layers.py`/`test_layers.py` ran in place from the export.
  Interpreter: W57's `.venv` (Python 3.13.15), `PYTHONDONTWRITEBYTECODE=1`,
  `-p no:cacheprovider`, `-c /dev/null --noconftest`. Every run under
  `systemd-run --user --scope -p MemoryMax=4G -p CPUQuota=200%`.
- `pytest test_layers.py`: 9 passed (18.8 s). `cargo test --release --lib`: 3 passed.
- `probes/probe_exhaustive.py` (Δ at **every** P from the cut to the head, paged with
  random limits, plus P = −∞, over 4 histories × 120 commits × 3 readers, in the
  test's mix with indexed deltas and in a churn mix): page checks 2,719, mismatches 0;
  lookup-with-P checks 227,962, mismatches 16,356 (R1). `exhaustive.log`.
- `probes/probe_lookup_p.py`: R1's two histories, `lookup_p.log`.
  `probes/probe_lifecycle.py`: crash recovery (A), the window race (B), attempt keys
  (C), the pin rule (D), `lifecycle.log`.
- Not run: anything at 1M or 100M, object storage, the harness, the model replay,
  dollars. The server's load average was 35-55 throughout; no timing was taken.

## Fix queue for the coordinator
1. R1: form 3 with a P, or its removal; one fold test over the key-list form.
2. R2: rewrite `docs/observed-set.md`'s before-image, membership and full-compare
   passages against Δ(P, head); update the design's "not supported" confirmation.
3. R3: the cut = oldest live P; the window triggers folds.
4. R4 and R5 as production (T33) requirements, not prototype fixes.
