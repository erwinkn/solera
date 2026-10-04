# A20 handoff and decision audit

The recommendation is **keep spans**, subject to its outstanding correctness
fixes and the shipped-API measurements. This study does not certify the current
implementation. See [the design](../../../docs/key-index-alternative.md).

Work is on `study/key-index-alternative`, based on
`7c81f845344e23bae935b9471f34f3ef2b221556`. The final commit is identified in the
Initiative report, so this file does not need a self-referential commit hash.
No product files or dependencies changed.

## Decisions, least certain first

| Choice | Main alternative | Confidence | Why, and when it would be wrong |
|---|---|---|---|
| Estimate remote times from counted pack ranges, two metadata rounds, 64 requests and 500 MB/s | Implement a real object-store tree | Low for absolute time, high for the disclosed scope. This is enough to reject page-copy cost without pretending to have a new engine. Metadata traversal, scheduling and decoding can make real reads slower; a better codec/planner can make them faster. Remote figures must not become an SLO. |
| Use fanout 128 and 48-byte raw directory references in estimates | Search compressed/adaptive directory representations | Medium. These expose the small-leaf tradeoff, but are not a lower bound. Do not rely on the estimated 109–644× total byte ratios as universal results. Measured leaf amplification stands independently. |
| Test a fixed numeric-key page fixture and a separate arbitrary-byte-key semantic model | Build the entire adaptive tree with persistence, GC and engine wiring | Medium. This deliberately favors the candidate and stays within design-study scope. It does not establish production memory bounds, long-value handling, split-diff speed or engine compatibility. D92. |
| Compare scenario results against attributed historical spans and leveled measurements | Rebuild and rerun three implementations on an identical trace | Medium. Exact 10/1K span-window and old-position timings are unavailable. They are left blank, not interpolated. Old hardware, workload and cap differences prevent a controlled latency comparison. |
| Choose packed immutable snapshot pages with root-pair diff | Coarse time summaries, MVCC key/version files, checkpoint replay | High for the candidate choice. A9 already covered those alternatives. This tests its strongest structurally different option and removes per-endpoint version groups. D90. |
| Count dead slices in retained packs | Treat packing as one free PUT while counting only reachable pages | High. Packs cannot be partially deleted. Relocation needs additional copying and reference publication. D95. |
| Recommend keeping spans; no default second-index hybrid | Replace spans or maintain both structures | High for the measured workloads, conditional on span fixes. A predominantly bulk/clustered workload or a new buffered-tree design could justify another study. D94. |

The small page-size sweep and gap-coalescing variants are measurement points,
not proposed production defaults. No user decision was inferred or closed.
The assignment's requested native decision records and this artifact replace a
Git-backed decision log; no Git command was used to record choices.

I stand behind the measured copying/retention result and the recommendation.
I would not stand behind a claim that this is a finished tree, a matched
three-engine speed comparison, a proof that all simpler indexes must lose, or
an implementation of safe tracing GC. None of those claims is made.

## Checked evidence

- Final native source runs in `history.jsonl` and `sweep.jsonl`: 1M-key histories,
  12,000 commits, first/full diff queries at 10, 1K and 10K commits behind,
  head/historical points, scans, 101 roots, small and large commits, hot keys,
  page-size sweep and whole-pack retention. No assertion failed.
- Every slot in the three historical query roots and head matches an independent
  per-commit array. Query counts/checksums and sampled reads at all pinned roots
  also match. The native fixture's numeric, fixed-layout limits are explicit.
- `model.py`: 31,706 complete mapping/lookup checks, plus explicit read-ahead
  cancellation, absent read-ahead, source reversion/equality, empty/high-byte
  keys, maximum generation, split leaves and hot-key snapshots. It uses no
  Solera implementation as its oracle.
- `environment.txt` records the 8 GB/400% scope settings, tools and source hashes.
  The final history peaks at about 1.71 GB RSS; the 64-row history about 1.79 GB.
  Resource records show successful process exits and no swapping by the jobs.
- Ruff checks/format, rustfmt check, shell syntax and Markdown local links were
  checked. JSON-derived values were cross-checked against the document.

## Remaining work and limits

This assignment is complete. No background work or pending commands remain at
handoff. No full Solera test suite, live object store, Postgres, Linux container,
macOS, TLA or Lean run was needed for the new study-only files, and none was run.
No 100M benchmark was run. Those figures are estimates.

If the coordinator chooses to revisit snapshots, first implement byte-bounded
range-aligned tree diff and engine read-ahead/root-pin transfer. Then test
publication, zombie collection, and packed-page relocation with actual storage.
That is a new implementation task, not hidden unfinished work in this study.
For spans, continue the existing A17 fix and shipped-API remeasurement work.

Recovery artifacts are the checked-in sources, JSONL/JSON results, resource
records and this handoff. `/tmp/solera-a20-snapshot` and
`/tmp/solera-a20-sample.bin` are disposable build/sample outputs; the documented
commands recreate them. No external service or data was changed.
