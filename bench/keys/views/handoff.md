# W53 (T26, A24) handoff: phase 1

Branch `exp/key-index-two-views`. Design: `docs/key-index-two-views-design.md`.
Phase 1 is done; phase 2 waits for the coordinator's go.

## Choices, each against its alternative

| Choice | Alternative rejected | Evidence |
|---|---|---|
| T: aligned tree, fanout 4, level 1 = packs of 4 deltas copied unmerged | b = 2 (2× the writes), b = 8/16 (1.6–2.5× the files a reader opens); raw deltas; one packed log; blocking spans | `model.tsv`; span design's cited numbers |
| T keeps every node from the floor (oldest reader start) | chains per reader (needs exact reservations, A17 R5) | design § Retention; D110 tension noted, position-anchored compaction proposed |
| T drops absent→absent keys; read-ahead classed from K at N | keep them as spans do (A12-1) | design § Read-ahead, worked example |
| K = base + T's chain; base absorbs one level-8 (100M) / level-5 (1M) node, r = 4 | a dedicated tiered LSM (same tiers, written twice); leveled (51× writes, cited) | `model.tsv` |
| Snapshots by pinning K (base files, w, S) | K head + T undo (more reads per page, grows with the pass) | design § Snapshots |
| Codec zstd-1 | zlib-1 (v4), lz4, none, zstd-3 | `codec/results.tsv` |
| 16 KiB blocks | 64 KiB, 256 KiB | `codec/results.tsv` |
| Row entries (v4 encoding) | columnar | `codec/results.tsv`: −20% ids, +2–5% paths |
| No Bloom filter on the base; filters on T files, read when cheaper than the blocks they save | filter everywhere (175 MB per cold lookup at 100M) | design § Filters |
| Globs by interval skip-scan over the block index | common-prefix skip, trigram filters per block | `globs.txt` |
| Cleanup: delete only after a journal write that follows the listing; T by floor, others by pins and epochs | listing-time judgement (A17 R1) | design § Cleanup |

## Reproduce

- `CARGO_TARGET_DIR=/tmp/w53-target cargo build --release` in `codec/`, then
  `views-codec 400000 > results.tsv` (~5 min, one core).
- `python3 bench/keys/views/globs.py` (~3 min), `python3 bench/keys/views/model.py`.

## Open

- Railway uploads are billed ($0.05/GB service egress; buckets public-network
  only), unlike the brief's assumption. Verified on docs.railway.com today.
- Spans' harness (`spanbench.py`, D88) is needed from W42 for phase 2.
- Whether spans are also measured with zstd and 16 KiB blocks.
- The decision rule for the verdict (proposed in the design).
