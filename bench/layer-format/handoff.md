# T44 handoff (W57): choices, each against its alternative

Branch `exp/layer-format` (never merged). The results and the recommendation
are in `results.md`; this lists how the bench was built, and why.

| Choice | Alternative rejected | Why |
|---|---|---|
| A standalone Rust crate (`bench/layer-format`) holding both formats, one reader, one simulated store | Python harness over `solera.keys` (W53's way) | Both formats' encode and decode in the same language and allocator; peak heap measured exactly (a counting allocator) |
| One I/O planner for both: metadata in one GET under 1 MiB, else a top level first; page-grain units; batches doubling 256 KiB → 32 MiB; 64 KiB coalescing | arrow-rs's stock `ParquetRecordBatchStream` over an object store | The stock path fetches all page indexes, whole row groups, coalesces at 1 MiB: 2–5× the bytes or memory (`ablations.py`). The decision is between formats, so their I/O policy must be the same (D184) |
| Parquet's partial page index: a `PageIndexProvider` holding only the row groups a read touches | Fetch every page index | 7.4 MB of the 100M base's page indexes per cold read |
| Our block index in segments of 128 blocks, under a top level | One flat index | The same two-level reach as Parquet's footer and page index, for a fair cold read at 100M |
| Configurations from 10M sweeps: ours 32 KiB blocks; Parquet 128K-row groups, 32 KiB / 4,096-row pages, units of 4 pages | Each format's defaults | Each at its balanced point between stored bytes and point reads; 16K-row groups reported as a variant |
| Rows in our blocks | Columns within blocks | 5% more bytes in columns: zstd matches a commit with its version in row order |
| The layer schedule on entry counts × 12 B | On one format's bytes | The same layers for both formats |
| An order-sensitive digest of every key's result, against the fold of the commit stream | Comparing result sets in memory | Constant memory at 100M results; every key and both its versions enter it |
| Realistic data: 53-byte keys with a hash part, a load over 1,000 commits, generation versions | Sequential keys, version 0 | The first data compressed to 0.6 B/entry, hiding both formats' costs |
| Real sleeps for the store's latency, as `solera.keys.io.ObjectIO` does | A virtual clock | Decode overlaps fetches as it would; W53's numbers are comparable |

## Open

- Combining entries below the retention horizon, clears and string versions
  are not modelled; neither is parallel decode.
- A planner that grows its units as a read goes on would make Parquet's full
  scans 8% faster with a quarter of the GETs (ablation "whole row groups").
- If Parquet is chosen: the reader in `pq.rs` is the starting point (footer
  first, partial page index, page-grain units, batch sized to the selection).
