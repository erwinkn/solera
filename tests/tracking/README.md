# The tracking model

A brute-force model of the tracking redesign (`docs/tracking-design.md` on
`design/tracking`), checked by random histories after every step. It settles
design choices by model check and serves as the oracle the new core's units
test against. `docs/verification.md`, "The tracking model", says what it
covers, what it found and what it leaves out.

| Module | What it is |
|---|---|
| `core.py` | The output log (entries with both versions, clears, `diff`/`scan`/`get` from entries) and progress (pieces, points, decoding, the planner, the batch commit rule) |
| `staleness.py` | One consumer input (checks over items), its staleness kept by one witness due key (`witness`) |
| `fullrun.py` | The cast (feed and items upstream; copy, tally, archive, report, checks); a full run clears at its first batch and the gate holds readers (`clear`), on file or table stores |
| `histories.py` | One shrunk history per rule: passes with every rule on, fails with that rule off |
| `oracle.py` | The stable interface for the build: replay a history, ask what is held, owed, due, and planned next |
| `__main__.py` | Sweeps, ablations, shrinking and work counts from the command line |

## Running it

```bash
uv run pytest tests/tracking -q                    # CI: 200 histories per store, every short staleness history, every calibration (~20 s)
uv run pytest tests/tracking -q --slow             # 5,000 histories per store
uv run python -m tests.tracking staleness          # 3,000 histories
uv run python -m tests.tracking fullrun --kind table
uv run python -m tests.tracking staleness --off rule3_range --shrink   # a minimal failing history
uv run python -m tests.tracking fullrun --ablate   # each rule switched off in turn
uv run python -m tests.tracking staleness --stats  # the work the witness does
```

## The oracle

```python
from tests.tracking import oracle

w = oracle.replay_fullrun([
    ("items", (("k1", True), ("k2", True))),   # items commits k1 and k2 at fresh versions
    ("step", "copy", 1),                        # copy plans one key and commits it
])
oracle.holds(w, "copy")       # what copy processed from items, per key
oracle.due(w, "copy")         # ["k2"]
oracle.next_plan(w, "copy")   # (["k2"], "\uffff"): the next batch takes k2 and runs to +inf
oracle.held(w, "archive")     # True: copy has not processed k2 yet (D177), so the gate holds archive
```

`replay_fullrun` uses the decided full-run form (`form="clear"`). While the gate
holds a consumer, `due` is empty and `next_plan` takes nothing; with a full
run due, `owed` and `due` name every key upstream holds, as the full run's
first batch takes them.

`replay_staleness` and `replay_fullrun` raise `oracle.Broken` if a
history breaks an invariant, so an answer always comes from a consistent
state. An observation is `(version, Cond)`, `"held"`, or absent; its `Cond`
has `pat=None`, since patterns only decide which keys are taken (a pattern
change owes only the keys that change membership). A range bound is a key,
`""` for -inf or `"\uffff"` for +inf.

The brute force ranges over a small fixed key space, so a history may name
only its keys, or the replay raises `ValueError`: `staleness.KEYS` (k1 to k5
and x1; `("pat", i)` picks `staleness.PATTERNS[i]`) and `fullrun.KEYS` (k1 to
k5; k2 and k4 are even for tally).

## Events

A history is a list of plain tuples. An event that does not apply (a commit
with nothing planned, a plan while one is in flight) is skipped, so any
sublist of a history is a history.

**Staleness** (`staleness.history`): `("up", ((key, version or None), ...), lost)` an
items commit, its notification lost or not; `("clear", changes, lost)` items'
full run begins in the design's form; `("complete",)` it ends;
`("plan", n)`, `("full", n)` (a full run's first batch, which resets progress),
`("keys", keys)`; `("commit", failing keys, keys loaded at the head)`;
`("cancel",)`; `("pat", i)`, `("ctx",)`, `("defn",)` change the conditions;
`("held",)` progress can no longer be trusted; `("retry", key, by_timer)`;
`("restart",)`.

**Full run** (`fullrun.history`): `("items" | "feed", ((key, present), ...))`;
`("plan" | "step", asset, n)` (a step plans and commits at once);
`("commit" | "cancel", asset)`; `("load", reader)` a reader's batch loads
before it commits (archive, report); `("keys", asset, keys)`;
`("define", asset)` a new definition; `("full", asset)` a full run by hand;
`("restart",)`; `("summary",)` the whole reader of copy runs.
