# W57 (T31) phase 1 handoff

Branch `design/key-index-fp`. Design: `docs/key-index-from-first-principles.md`.

## Choices, each against its alternative

| Choice | Alternative | Why |
|---|---|---|
| presence at P from flips (adds and removes after P) | a version per reader (spans); before/after per node (two views) | Δ asks only for presence at P; the change kinds make it a parity; flips are reader-agnostic and cost ~0 B for updated keys |
| one structure: recency-tiered runs | two structures (two views' key view and time tree) | flips make a straddled run exact, so one tiling serves both head reads and catch-ups |
| a reader bound in bytes, k × newer + Z, reader-agnostic | spans' read rule per endpoint; exact covers (every level kept) | bounds every reader's read to (1 + k) × plus Z without knowing P; without it a 100-behind reader at 100M reads 7.6× on average, up to 1,000× (replayed) |
| k = 4, Z = 1 MB, f = 4, r = 4 | k 2 or 8; Z 4 or 16 MB; f 8; r 2 (all replayed) | k trades runs against reads; Z = 1 MB halves near readers' waste for +0.7 runs; f = 8 writes ~30% less but averages ~20 runs (33 at most); r = 2 writes less but slows cold lookups (larger runs above the base) |
| the cut = the newer of the window's edge and the oldest live P | the window alone; the whole live P set in merges | the oldest live P halves storage at 1M and brings a day-behind reader from 2.7× to 1.3×; the whole set changes nothing measurable over it |
| the base's tombstones in a separate graveyard file | in the base | they shadow nothing; head reads skip them |
| no filter on the base; filters on other runs, used when held | a filter everywhere; none anywhere | the base holds ~every written key; other runs' filters only help when already in hand |
| stream-versus-seek by time (waves, NIC bytes, decode) and requests | a block-count rule | the block rule streamed 100M keys per commit in W53's prototype |
| the before-image of a reader behind the window as Δ(P, head) | Δ(P, cut) | keeps H at the head, the only H the index serves |
| the replaced version for store cleanup from the writer's lookup | a predecessor in the delta; compaction-emitted garbage | the minimal delta has no predecessor; compaction finds shadowed versions days late at 100M |

## What phase 2 should check first

1. The cut as the oldest live P: the engine's set must include in-flight claims' heads; a P below the recorded cut must fail loudly (test it).
2. The automaton seek against Solera's matcher, with A25's two counterexamples.
3. A 1M-key commit and churn through real merges (flip lists, the graveyard).
4. Cold lookups at 100M with and without filters held; the warm engine path.

## Phase 2 (D151)

Prototype: `native/src/layers.rs` (blocks, merge, Δ scan with globs, lookups),
`bench/keys/fp/layers.py` (state, writer, upkeep, readers, lifecycle),
`viewbench.py --index layers`, `rebase.py`, `test_layers.py`. Commands and
what is measured or not: the doc's "Phase 2" section.

Choices made in phase 2:

| Choice | Alternative | Why |
|---|---|---|
| side parts per layer (keys absent at both ends), from a start bit | the base's graveyard only | readers after a layer and head reads skip temporary keys; churn 3.4x -> 3.1x at 1M, measured |
| no Bloom filters in the prototype | filters in layer indexes | the harness's readers are cold and would not use them |
| merges hold inputs in memory | streaming by key range | prototype simplicity; ~1 GB at a 100M base merge |
| glob filter applied while walking blocks, literal prefilter | build every entry, then match | a 1M-entry non-prefix scan from 0.28 to 0.04 s of CPU |

## State

Branch `design/key-index-fp`, clean and pushed. No background jobs (the 100M
server build was stopped at Erwin's request). Scratch: `~/.solera-t31/`
(1M build directories under `vb/`, replay logs).
