# T43 handoff: docs/tracking-design.md (branch design/tracking, d25eb9f)

## Choices made beyond Fable's brief and the settled decisions
- P13 load reports into the upstream log, for produced outputs too (Fable D16 covered sources only).
  Why: without staging/fencing (D184), a dead attempt's row in a table keeps a key owed forever.
  Alternative: per-key repair owed by the producer (doesn't work for ordinary producers); a full run (too heavy).
- P14 exact per-piece owed counts updated from each commit's entries (the mechanism for D183).
  Shortcut: h = replaced version when the key's previous change is at or before the piece's commit; else a warm-index get.
  Alternative: re-diff every reading edge on each commit (costs the whole lag per commit).
- P15 "newer version wins" upsert as an optional SQL-store recipe, not contract.
- Owed vs due split (P9): failing keys not yet due don't make a partition stale (keeps STA-3).
- Engine builds every commit's entries (consequence of D184), so Fable's rule 3 retry-on-parent-mismatch is moot.
- Migration adds two steps to T43's list: success-only progress (3) and exact owed counts (6); exact cleanup (8) is independent.
- Cast: behaviors.md's, plus `archive` downstream of `copy` for the clear/gate example.

## Checks
- model.py here: python3 model.py 3000 -> ~68k random steps pass (decode = brute record; propagated counts = brute counts; diff/scan from entries = true states), and prints every worked example.

## Open
- Unkeyed outputs' retention of appended data (Fable open question) needs Erwin.

## Revision after W65 review (083f7e0)
- P13 narrowed to sources; P15 proposed as contract for in-place stores; P16 new (anomaly: alarm + re-owe; never logged).
- §6 rule compares whole observations; clears make older pieces wait for one scan; batch-cut split rule (batches start at piece boundaries; the task's progress key stays a boundary until the run ends).
- Cut >= cleanup cursor. Step 4 backfill; step 5 namespace reset (D169) recommended over a last fold.
- model.py v2 (model_v1.py kept): conditions, split by rule, pending counts after clears; 66,638 steps pass.
