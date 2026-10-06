# T45 handoff: choices made in the spike, and why

Branch: `exp/engine-planning`, from main `08c40c2`. Never merge it. The
results are in `README.md`; the numbers are in `results.md` and
`results.json`.

## Choices

- **Two planners measured, not one.** Today's planner made full,
  `keys="all"` and far-behind batches cost the whole key space, so it
  could only answer "no". Commit `819b512` makes it stream, a minimal
  fix that leaves behaviour unchanged (planner tests green). The bench
  runs both from one script: `--planner asis` loads main's `owed.py` and
  `LayerIndex._bound` from git (`08c40c2`). The recommendation rests on
  the streaming numbers.
  - *Alternative:* measure only the planner as it is and extrapolate. It
    would have hidden where the real limit lies.
- **The planner as it is runs at 1M and 10M only, one batch for the heavy
  scenarios.** A batch costs O(N), so at 100M a full-run batch would take
  tens of minutes. That cost is linear in N, so the 100M figure is an
  extrapolation.
- **The engine is a 4-vCPU machine.** Each run is in a
  `systemd-run --scope` with `CPUQuota=400%` and `MemoryMax=16G`, and
  the default thread pool is set to 8 threads, as `min(32, cpus + 4)`
  gives on 4 vCPUs. The server has 96 cores, so without a cap the pool
  would have 32 threads and the numbers would flatter the engine.
- **The index is built, not modelled.** It uses today's write path,
  `resolve` and `write` called directly. `write_patch`'s bytes rule would
  stream the whole index for each scattered commit, hours at 100M, for
  byte-identical deltas: 10M built both ways gives 101,623,475 bytes
  each. Today's merge rule runs to rest after each commit, with the cut
  at the oldest reader.
- **Keys are 41 bytes and random-looking but sorted.** Sequential keys
  compressed to 1.5 B/key, which is unrealistic; these give about
  10 B/key.
- **The retries stand-in uses the upstream index as the outcome index**,
  with 1% of keys due by a hash. The walk's bound (`WALK × batch`) is the
  real one; the stored-outcome decode is replaced by the hash test, which
  makes the measured CPU a lower bound.
- **S3 is simulated by a local store with 30 ms added per GET**, and the
  page cache is dropped (`posix_fadvise`) for cold runs. There is no
  bandwidth cap.
- **Planning processes read the layer files locally**, standing in for a
  disk cache shared read-only. The layer files are immutable, so this is
  how such a pool would read them. They have no shared memory tier.

## Open items for whoever builds on this

- **Land the streaming planner or its MVCC equivalent.** It must be fixed
  whatever the redesign. `819b512` is small (owed.py, one helper in
  layers.py), but it is on the experiment branch only.
- **Plan off the event loop.** Recommended: a process pool sharing the
  disk cache. The cheap first step is a planning thread.
- **Smaller fixes:**
  - single-flight index-object fetches in `LayerIndex._prepare`;
  - `.lix` files kept on the engine's disk;
  - an index filled on its first plan after a restart;
  - one block cache shared across plans, instead of 64 MiB per `LayerIndex`.
- **Retry plans need a due-time index.** Today's walk is the most
  expensive plan, by 10–20x.
- **Not measured:**
  - real S3 bandwidth and throttling;
  - planning while commits and merges change the index under the plans
    (the state is static here);
  - more than 50 concurrent tasks (the cost per plan grows linearly past
    saturation; see README).

## Reproduce

    uv sync --all-extras
    python bench/planning/plan.py all       # ~20 min for 10M and 100M once built (builds: 7 + 19 min)
    python bench/planning/plan.py all --only asis

The indexes live in `$SOLERA_BENCH` (default `~/solera-bench`, about
1.1 GB). Run it on a real disk, not tmpfs.
