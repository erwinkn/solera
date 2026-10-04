# A20 snapshot-page prototype

Study only. No module here is imported by Solera. The design and verdict are in
[docs/key-index-alternative.md](../../../docs/key-index-alternative.md).

`snapshot.rs` measures immutable, fixed-range copy-on-write leaves, shared
directory groups, per-commit packing and root-pair diff. Keys are numeric IDs,
rows are 16 bytes, and deletes leave an absent slot. This favors the candidate:
no string comparison, compression, tree splitting, disk cache, actual object
I/O, CAS, pack relocation or failure recovery. It does **not** measure a new
storage engine. `data_gets` counts ranges in its simulated pack layout, not
requests issued to a server. Optional gap coalescing reads bytes between live
pages. The local CPU and allocation measurements are real.

The fixed layout uses groups of 128 leaves and a variable-size root. The root
fits under 32 KiB in the 1M runs. The 100M estimates use extra directory levels.
The 48-byte encoded directory reference is an estimate; it is not measured
serialization. Leaf write amplification excludes directory and pack-GC writes.
Physical pack retention counts whole raw leaf packs containing any reachable
leaf. It excludes directory bytes, so it is a lower bound for this pack policy.

`model.py` checks arbitrary byte-key semantics against independent dictionaries.
It has single-version immutable leaves, a flat fence directory and splits. It
retains empty fences and materializes query candidates. It is a correctness
reference, **not** the proposed bounded-memory tree traversal or its GC.

`analyze.py` separates measurements from estimates. The remote model adds two
metadata latency rounds, 64 parallel data requests, and transfer at 500 MB/s.
It tries gap sizes 0, 64 KiB, 1 MiB and 16 MiB, choosing the lowest modeled time.
It excludes request variance, directory transfer, codec CPU, actual SDK
scheduling, journal latency and output materialization. Actual latency can be
higher. Times are not S3 measurements or guarantees.

The optional codec sample encodes 128 head leaves with prefix-compressed string
keys and zlib level 1. Even IDs and sorted random IDs are ordered in a 1M-key
population. UUID-like keys are sorted within each sampled page, not a complete
1M-key UUID tree, so that sample is illustrative only. Source payloads are
16 random bytes. Absent slots are omitted. No codec timings feed remote latency
estimates. The raw 16-byte layout is the reference for the tables unless a
compressed value is explicitly identified.

## Reproduction

Run from the repository root on Linux with user systemd. Output files are small;
the fixture does not write its synthetic multi-GB pack history to disk.

```sh
systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400% \
  rustc --edition=2021 -O bench/keys/alternative/snapshot.rs -o /tmp/solera-a20-snapshot
systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400% \
  /usr/bin/time -v /tmp/solera-a20-snapshot 1000000 256 12000 1000 \
  history-churn /tmp/solera-a20-sample.bin \
  > bench/keys/alternative/history.jsonl 2> bench/keys/alternative/history-resource.txt
systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400% \
  /usr/bin/time -v bash bench/keys/alternative/run-sweep.sh \
  > bench/keys/alternative/sweep.jsonl 2> bench/keys/alternative/sweep-resource.txt
systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400% \
  python3 bench/keys/alternative/model.py \
  > bench/keys/alternative/model-result.txt 2>&1
systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400% \
  python3 bench/keys/alternative/analyze.py /tmp/solera-a20-sample.bin \
  > bench/keys/alternative/analysis.json
```

The default seed is 11. The trace starts with 1M live keys at generation 1.
`uniform` updates distinct existing keys. `hot` chooses half its attempts from
a 1% set spread across the keyspace, as in the historical span benchmark.
`history-churn` makes 10% of writes absent, including already absent keys;
other writes set a new generation and may re-add a key. It remains within 1M
possible keys. This differs from the older benchmark's expanding keyspace,
90% update/5% remove/5% insert mix and occasional large commit. Those differences
are disclosed in the comparison; old times were not reproduced.

The sweep includes 1,000 commits per page size, 256 commits of 16 and 100K keys,
a hot trace, and 12,000 uniform commits at 64 rows/page. History modes retain
101 roots every 100 commits from 2,000 through 12,000, plus query roots at
11,990, 11,000 and 2,000. Every slot at the three query roots and head is checked
against the independent per-commit arrays; diff counts/checksums and sampled
historical point reads are checked too. The byte-key model checks complete
expected output mappings across endpoint pairs and page sizes.

The Rust diff is one uninterrupted traversal for the full query and a separate
first-page traversal. It counts ranges across the selected set, which assumes
the read planner can schedule them together. A production streaming query needs
bounded planning batches and may pay more. It does not prove a resumable
production reader's latency.

## Evidence files

- `history.jsonl`, `sweep.jsonl`: final native measurements and physical-layout counters.
- `history-resource.txt`, `sweep-resource.txt`: process resource records and scope IDs.
- `analysis.json`: derived costs and explicitly modeled extrapolations.
- `model-result.txt`: semantic checks.
- `environment.txt`: host, caps, tools and source hashes.
- `handoff.md`: decisions, limitations and next steps.
