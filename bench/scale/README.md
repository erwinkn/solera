# Scale harness

One end-to-end run of a small cast at the scale target: 100M keys per output, about 1% of them
changing a day, 10,000-key batches, one 4-vCPU engine. It goes through the real engine, its API
and pool workers, on the tracking path that `SOLERA_TRACKING` names (`today` or `core`). It
times each part of the work separately and writes `report.md` and `results.json`.

    uv run --no-sync python bench/scale/run.py --keys 100000000 --days 30 --root /data/scale

Run the same command after each engine change (#40, B6 to B10, the switch to the core) to see
what moved. `--profile` also samples the engine with py-spy (it needs `sudo -n py-spy`).

## The cast (project.py)

    feed       a keyed source (key "id"): k000000000 …, each at the day it last changed
      └─▶ items      a keyed copy of feed
            ├─▶ tally      a count per 16 groups of keys, built on ctx.load()
            └─▶ copy       a keyed copy of items
                  └─▶ archive    a keyed copy of copy

Every asset runs on the pool `scale`. The driver starts the engine (serve.py) and `--workers`
pool workers. The day each key last changed is one byte per key in `ROOT/versions`, 255 for an
absent key. The driver writes that file, and feed's loader reads it.

Keyed outputs use `PackStore`, a FileStore that packs each attempt's keyed write into one object,
`{output}/{generation}.pkl`. FileStore itself keeps one object per key, which costs two inodes
per key, so a local disk holds a few million keys of it and not 100M (ERW-21). PackStore reads a
batch's keys by grouping them by generation and loading each pack once. It never cleans up,
because a pack still holds other keys' live rows, so the data directory only grows. No number
in the report measures the store's bytes.

`--form` picks the data's form. With `dict`, the default, feed's loader returns a `Loaded` and a
dict per key, the copies take and return lists of dicts, and a pack is the write's `{key: rows}`,
pickled. With `arrow`, feed's loader returns one Arrow table (`Loaded(table, version=...)`), the
copies take and return `pa.Table`, and a pack, `{output}/{generation}.arrow`, is an Arrow IPC file
of the producer's rows, taken from its table by each chunk's `indices` with no Python object per
row. Run both to tell the product's per-key cost from the harness's.

## Phases

1. `source`: feed's keys, in source commits of 1M keys.
2. `items`, `tally`, `copy`, `archive`: each runs in full, alone, in that order. A full run's
   written keys are in one range.
3. `day 1` … `day N`: `--change` of the live keys change. `--adds` of those changes are new
   keys, as many live keys are removed, and the rest are updated. The changes arrive as source
   commits of `--batch` keys, then the four assets run as one run. A day's written keys are
   scattered.

## What each phase records

| Field | How |
|---|---|
| wall_s | Driver clock, from one engine sample line to a later one. serve.py writes one line a second. |
| engine_cpu_s, engine_cores | /proc/{engine}/stat |
| loop_busy, loop_longest_ms | serve.py wraps the event loop's selector: time outside `select` is busy, and the longest stretch is the longest stall. Each sample line covers the second before it: the line before the phase is only the baseline of the cumulative counters, and extrema come from the lines after it |
| engine_rss_peak_mb | serve.py, one sample a second |
| engine_rss_peak_so_far_mb | serve.py, the kernel's high-water mark of the engine's RSS |
| engine_write_bytes | /proc/{engine}/io write_bytes: everything the engine wrote, journal and run history included |
| batches, batches_by_asset | Succeeded attempts by asset, from the run's detail: one attempt is one batch |
| keys_planned, keys_per_s | Keys the producers' batches planned (added, updated, removed and unchanged), from project.py stats; keys_per_s is over the phase's wall time |
| rows_produced | Rows the producers were handed (removed keys have none) |
| engine_cpu_ms_per_batch | engine_cpu_s over batches: the whole engine, source commits and the API included |
| engine_thread_cpu_by_part_s | serve.py: the CPU of native work on threads (`in_thread`: the core's plans and commit builds, the index's reads), measured on the thread and added to the part of the engine that awaited it (`BUCKETS`, matched against the awaiting stack) |
| planning_cpu_s, keys_per_planning_cpu_s, commit_cpu_s, commit_cpu_ms_per_batch | With `--profile`: planning's CPU is py-spy's share of the engine's own threads plus the measured thread CPU awaited by planning, and keys planned over it. Building commits is the same, over batches |
| state_bytes_added, index_bytes_per_entry | Bytes added under the state, by directory. The change index is `scale/keys` (today) or `scale/core` (core); index_bytes_per_entry is its growth over the entries committed: source keys changed, plus each keyed output's keys written and removed |
| attempts, attempt_steps_s | Sum of each attempt's lifecycle steps (preparing … settling), from the run's detail |
| workers | Each producer and store call's wall and CPU seconds and key counts, from `stats/{pid}.jsonl`, tally's own load and write included |
| engine_cpu_by_part_s | With `--profile`: the engine's CPU split by `BUCKETS` (journaling, source commits, building commits, planning, API, upkeep, other), in proportion to py-spy samples. Samples on executor threads are `threads`: a native call there has no frame of its own, so a sample cannot say whose it is, and engine_thread_cpu_by_part_s splits it |

`report.md` puts N1, N3, N7 and N9 (plans/build-plan.md) beside their thresholds. N3 and N7 need
`--profile`: they are planning's and building commits' CPU, as described above. Without it, the
report gives only the proxies, which are the whole run's wall time and the whole engine's CPU. It
also shows the engine CPU of each full run against the six-minute target, and the engine's peak
memory.

What is not measured:

- N1's "entries rewritten per entry committed" needs the index to count its rewrites. The report
  gives the engine's bytes written per entry instead, which includes rewrites and also the
  journal.
- N3 is measured with the cast's four consumers, not 50 tasks.
- N12 (engine memory under 50 tasks) is not exercised: the cast has four. The report gives the
  peak for this cast with no threshold.
- py-spy's split is by sampled stacks (`BUCKETS` in run.py), so N3 and N7 are as exact as the
  bucket patterns.

## Checks

At the end, every keyed output must hold exactly the live keys: the driver streams each output's
keys in key order against `versions`, at any scale. Tally's every group must hold its count. Up to
1M keys (`VERIFY`), the driver also reads every key's value back after every day and compares it
with `versions`. The run exits non-zero if any of this fails.

`--change` and `--adds` run from 0 to 0.5: a day removes as many keys as it adds, from the keys it
changes, so the live count stays `--keys`.

The randomized check of the harness itself is the same run, small, with heavy churn, over several
seeds, on both paths:

    for seed in 1 2 3; do for path in today core; do
      SOLERA_TRACKING=$path uv run --no-sync python bench/scale/run.py --keys 3000 --days 4 \
        --change 0.3 --adds 0.3 --batch 500 --workers 3 --engine-cpus '' --seed $seed --root /tmp/scale-model
    done; done

## Options

- `--engine-cpus` (default `0-3`) pins the engine with taskset. `--worker-cpus` pins the workers.
  On a machine with more than 4 CPUs, give the engine 4 and the workers the rest.
- `--concurrency` sets the engine's attempts at once. `--workers` sets the number of pool worker
  processes.
- `--port` sets the API port (default 8765).
