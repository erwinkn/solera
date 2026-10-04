# bench/keys

Benchmarks of the key index. `spanbench.py` measures the span index as
built, through the shipped `KeyIndex` API; the other scripts are replays
of the merge policy on metadata (`spans.py`, `adversarial.py`,
`retention.py`, `tiling.py`) and format microbenchmarks (`params.py`,
`cpu.py`). Results of record are in `results.md` and
`docs/key-index-design.md`.

## spanbench.py

This branch (`bench/spanbench`) is for measuring, not for main: it adds a
zstd codec to the native format (`CODEC_ZSTD = 2`) and a `codec` field to
`Options`, so the span index can be measured with and without the format
levers.

    uv run python bench/keys/spanbench.py --size 1e6 --commits 12000 --large 100000 --large-every 3000

**What it builds.** In one process, with no injected latency, on a local
file store under `--dir` (default `/tmp/spanbench`):

- a base of `--size` keys at generation 1 (`KeyIndex.replace`), or, with
  `--backfill K`, an empty output filled K new keys a commit until it
  holds `--size` keys (the commit count follows: ⌈size / K⌉; D80's
  per-key default is `--backfill 16`);
- `--commits` commits of 1K keys (90% updates, 5% removes, 5% adds),
  each resolved by `KeyIndex.resolve` (exact writes) and installed as
  its span; with `--large N --large-every M`, a commit of N updates
  every M commits;
- consumers reading every 1, 360 and 8,640 commits, each holding its
  `next` as a live endpoint; four readers kept 1, 100, 360 and 10,000
  commits behind the head, each reserved at head + 1 when its distance
  comes up;
- after every commit, upkeep's merge policy (`plan_merge`, `merge`) run
  until it plans nothing.

The fold — each key's generation, 0 when absent — is kept as numpy
arrays by key id (`key(i)` is a bijection), so the key-by-key check holds
at 100M keys: snapshots `before-P.npy` per reader, `head.npy` at the end.

**What it reads.** Each reader in a fresh process, cold, through an
`ObjectIO` that injects 30 ms per request and 80 MB/s per connection,
64 requests in parallel:

- `changes(P → head)` for each reader, 100K keys a page: first page,
  full read, GETs, MB, peak RSS above the process's baseline; every key
  and class checked against the fold;
- `write`: a commit of 1K random updates resolved (exact: every existing
  key's block read), not uploaded;
- `lookups`: 1K exact lookups at the head;
- `page`: a 100K-key page mid-index at the head.

A `Mismatches` column above 0 is a correctness failure, not noise.

**Output.** A JSON line with the build's totals (`spans`, `spans_max`,
`index_mb`, `entries`, `live`, `delta_mb`, `merge_mb`,
`merge_entry_writes` — entries merges wrote per entry committed —
`merge_byte_writes`, `merges`, `build_s`), then a Markdown table of the
reads. `built.json`, `options.json` and `readers.json` stay in the build
directory.

**Options.**

| Flag | Default | Meaning |
|---|---|---|
| `--size` | `1e6` | keys in the base, or the backfill's target |
| `--commits` | 12000 | commits after the base (ignored with `--backfill`) |
| `--large`, `--large-every` | 0, 0 | a commit of `--large` updates every `--large-every` commits |
| `--backfill K` | 0 | start empty, add K new keys a commit |
| `--codec` | `zlib` | blocks' codec, at level 1: `zlib` or `zstd` |
| `--block-size` | 65536 | bytes of entries a block holds before compression (16384 for 16 KiB) |
| `--no-reads` | | build only |
| `--reads DIR` | | run the readers on a finished build's directory |
| `--dir` | `/tmp/spanbench` | where builds go: `{size}-{commits}-{large}x{every}-b{backfill}-{codec}-{block}k` |
| `--seed` | 11 | the trace's seed |

**Structure and format, separately.** The span structure (spans,
merges, endpoints) is the same whatever the codec and block size; those
two change only how a span's files are encoded. Run the same trace
(same `--seed`, size and commits) four ways — `--codec zlib|zstd` ×
`--block-size 65536|16384` — and the structure's numbers (spans, merge
entry writes) stay put while bytes, GETs and read times move with the
format. Readers decode each file in its own codec, so `--reads` on a
build works whatever the reader's defaults.

**Budget.** At 1M keys and 12,000 commits a build takes about 45 minutes
on 3 cores (`RAYON_NUM_THREADS=3`) and the readers a minute; the 100M
build takes hours and a few GB of memory (the base is built through
Arrow, 10M keys a chunk). Build once with `--no-reads`, then `--reads`
as often as needed: the readers never write.
