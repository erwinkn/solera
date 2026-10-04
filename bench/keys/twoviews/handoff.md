# A21 handoff: two key index views

The note is [`docs/key-index-two-views.md`](../../../docs/key-index-two-views.md).
Verdict: no-go for experiments on replacing spans; take the digest check, raw
delta retention back to the oldest endpoint, and the read-ratio measurement
into spans and T22.

## Files

- `check.py`: the time view's algebra, retention rule, read-ahead over the key
  view and the digest identity, against the per-commit fold. `python3
  bench/keys/twoviews/check.py --histories 5000` (10 s). Calibrations (each
  must fail) are run by hand: see the note's last section.
- `model.py`: spans, spans with eager merging and the key view replayed with
  `spans.py`'s policy; the time view analytic; requests, round trips, seconds
  and dollars per workload. `python3 bench/keys/twoviews/model.py --sizes 1e6
  --readers 10` takes seconds; the 100M, 100-reader replay takes the longest.
- `results.md`: the model's output as quoted in the note.

All runs used `systemd-run --user --scope -p MemoryMax=8G -p CPUQuota=400%`.
The digest micro-measurement was a throwaway C loop in `/tmp` (deleted); its
code is described in the note.

## Choices

- **Both views share the deltas as level 0** (D101). A commit stays one PUT, and
  the views never lag the head: background work only reduces fan-in.
  Alternative: views materialised from a log with watermarks, readers combining
  a view with deltas past it. Rejected: it adds a lag state for nothing, since
  referencing the delta in place is free.
- **The time view keeps finer nodes instead of versions.** A node holds one net
  change per key; endpoints inside it are served by its children, kept by the
  chain rule. Alternative: versions per endpoint inside nodes, which is spans.
- **Absent-to-absent keys are pruned from nodes**, since read-ahead keys are
  classed from the key view. Equal-payload live-to-live keys are not pruned:
  that loses the delivered generation (a calibration shows it).
- **Fanout 4, top level 4⁹ commits.** Fanout 2 reads fewer nodes but writes
  ~2× more at 1M; a fixed top at 4⁶ makes month-old readers read 9× what
  changed at 1M.
- **"Spans + levers"** is the capped policy with merges forced past 6 spans and
  one metadata object (D102). T22 may implement eager merging differently (a
  smaller window, a lower λ); the finding that it costs catch-up locality
  should be re-checked there.
- **The read model is calibrated, not measured.** It reproduces the measured
  100M cold lookups within ~5% in GETs (1,100 against 1,166 for spans; 1,091
  against 907 for leveled) and under-reads 1M wall times (it omits Python).

## Open questions

- None blocking. The flip conditions in the note's Go/no-go are for the
  coordinator to watch in T22 and A17's re-review.

## State

Branch `study/key-index-two-views`, based on `main` at 7eeac0f. Only
`docs/key-index-two-views.md` and `bench/keys/twoviews/` are added. No product
code, no dependency change, no background work left.
