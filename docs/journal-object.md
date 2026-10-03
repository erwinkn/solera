# The journal as one object: why, and what it costs

Status: **built** (K18): `solera_server/journal.py`, on `solera.objects.swap`.
The rule is in `object-store-state.md` §10 (and `swap` in §0). It is checked in TLA+:
`spec/tla/JournalObject.tla`, described in `verification.md`, "Formal
model: the journal object". This file records why the numbered segments
went, what replaced them, and what it costs.

## Why

The journal used to be numbered, create-only segments. A new engine fenced
the old one by creating the next number, and cleanup deleted the
segments a checkpoint covered. Every journal bug came from those two
meeting: the place where engines conflict moved with every flush, and
cleanup kept emptying slots a fence could land in. 0b3e226 (a fence was
deleted, and a zombie wrote into its slot), F7 (a fence landed in a slot
cleanup emptied), F14 and F15 (fences in holes, the hole test). Fencing by
create was there only because obstore's `file://` backend has no
compare-and-swap. S3, GCS, R2 and Railway's buckets all have `If-Match`.

Now the journal is one object at a fixed name, swapped with `If-Match`. A
fence is a write like any other, and it lands only on the journal it read.

## What it deletes

The fence segments, the `fences` list in every checkpoint, the hole test
(§10's "Fences in holes"), the LIST on open, `seq` and the gap handling
(`_behind`, `JournalCorrupt` on a gap), the fence's nonce, the
`EngineStarted` event (nothing reads `Model.engine`), and the second
checkpoint kept for recovery. Opening takes two GETs and one PUT.

Five rules remain. With each one switched off, TLC finds the bug it
prevents:

| Rule | Replaces | Without it, TLC finds |
|---|---|---|
| The journal names its engine id, random per process | the fence nonce (f300500) | B's fence writes A's bytes again, so the ETag stays the same and A's next write still lands after B has fenced (`OneWriter`, 9 steps). |
| A refused swap reads the object: its own bytes mean the write landed, the old ETag means retry, and anything else is a conflict | own-bytes recognition | A lone engine takes its own lost answer for another engine's write, and stops (`AppendsAlone`). |
| An opener whose checkpoint is gone reads the journal again | F7's "open again" | The opener gives up (`OpensNeverFail`, 25 steps). |
| A checkpoint is read back before the journal names it | the second checkpoint kept for recovery | The journal names a checkpoint nobody can parse (`NoAckedLoss`, 11 steps). |
| Cleanup deletes only what it listed before its move | segment cleanup | A zombie's cleanup deletes a newer engine's checkpoint before that engine's journal names it (`NoAckedLoss`, 24 steps, two engines). |

**The old bugs.** 0b3e226, F14 and F15 have no analogue: nothing that
fences can be deleted, and a fence lands only on the journal it read. F7
shrinks to its harmless half: a 404 on a named GET, then a retry. The
last rule is the one place where the old pattern comes back, with a
zombie's cleanup racing a newer engine. TLC found that race while
checking this design, and listing first closes it.

## What it costs

Each flush rewrites every event since the checkpoint. I measured this by
instrumenting `Journal` in `tests/test_soak.py` (500 runs) and
`tests/sim` (64 runs):

| | Soak (demo, 4 sites) | Sim |
|---|---|---|
| Segment per flush (old) | 1.7 KB mean, 7.7 KB p99 | 2.0 KB mean, 11 KB p99 |
| Checkpoint | 98 to 182 KB, 149 KB mean | 28 KB mean |
| Flushes per checkpoint (old cadence) | 151 | 13 |
| Journal per flush, at the old cadence | 132 KB mean, 295 KB max | 16 KB mean, 60 KB max |

The soak's checkpoint is mostly pending history rows (105 KB, bounded by
the lake's 2,000-row flush) and key indexes (48 KB). Measured per item:
about 300 B per head, 270 B per partition record, 350 B per automation,
0.3 to 5 KB per key index and 6 KB per active run. A project of 50 assets
× 100 partitions, with 500 key indexes and 200 active runs, comes to an
estimated 5 to 10 MB of state.

The old cadence wrote a checkpoint once the journal reached
`max(256 KB, checkpoint size)`. Applied to a journal that is rewritten on
every flush, it would rewrite up to the whole state each time. The new
cadence is `max(64 KB, checkpoint size / 16)`:

| State | Written per flush (mean) | A checkpoint every | Snapshot cost |
|---|---|---|---|
| 150 KB (the soak) | 32 KB, plus 4 KB of checkpoint amortized | 38 flushes | small |
| 10 MB (measured below) | 320 KB, about 1.3 ms of upload | 640 KB of journal: 16× the old rate | about 100 ms under the flusher's lock |

A request costs 12 ms on Railway (a PUT, p50), and the bandwidth is about
250 MB/s. So the bytes cost little. What costs is the snapshot rate, and the
1/16 holds it to at most 16 times the old rate.

**No spill rule.** A spill would write the journal's events out as
immutable chunks named by the journal, to keep the object small. It
would add one concept. The numbers do not call for it: a flush writes at
most a sixteenth of the state, and that only becomes slow (more than
another request's worth, about 3 MB a flush) at around 100 MB of state,
far beyond any estimate.

**The checkpoint's cost, measured.** The engine encodes the journal and the
checkpoints with orjson (keys sorted, so a state always gives the same
bytes); events stay on the standard library, which refuses `inf` and
`nan`. Every state the test suite and the simulation encode (3,124 of
them) holds only string keys, ints within 64 bits and finite floats, and
decodes from orjson exactly as from `json`. On a 10.7 MB state shaped like
the model's (heads, partition records, runs; a local filesystem, median of
five):

| | `json` | orjson |
|---|---|---|
| encode | 84 ms | 26 ms |
| parse | 83 ms | 57 ms |
| the checkpoint under the flusher's lock, with `Model.snapshot`'s deep copy (278 ms) | 429 ms | 416 ms |
| the same, encoding the model's own structures at once (no copy) | — | 100 ms |
| the same, the read-back compared by bytes, not parsed | — | 43 ms |

The deep copy was most of the hold, and it is not needed: the snapshot is
encoded on the spot, before any later event changes the model
(`Model.snapshot(copied=False)`). Most of what remained was the
read-back's parse (57 ms). The read-back now compares bytes instead: the
encoding is deterministic and came from a valid state, so the bytes
written, read back unchanged, are a checkpoint that parses. That leaves
43 ms per checkpoint locally for `durable()` to wait at 10 MB, and on
S3 the PUT and GET of 10 MB add about 80 ms at 250 MB/s — acceptable, so
flushes do not continue during a checkpoint.

## Rejected alternatives

- **Keep the numbered segments.** That design produced four bugs and
  needs the hole test.
- **A journal object that names one segment per flush** (a manifest, as
  in Delta Lake or Iceberg). It has no holes, but every durable flush
  costs two sequential PUTs, about 25 ms instead of 12.
- **A lease** (a lock object with a time to live). It needs clocks and a
  bound on pauses, and `If-Match` fences without either.
- **For `file://`, a lock file created with `O_EXCL`, plus a rename.**
  A crash leaves the lock behind, and breaking it needs a timeout.
  `fcntl.flock` is released by the kernel when the process dies.
