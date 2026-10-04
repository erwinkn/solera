"""A25 read-only probes. No product imports, writes, builds or installs.

Committed modules are loaded from immutable Git objects. Design traces below
are small models of the written rules, not executions of a two-view engine.
"""
import itertools
import statistics
import subprocess
import sys
import types

REV = "dcfc0692be17731c8586a48e44fa251e6fa0828d"


def module(path, name):
    result = types.ModuleType(name)
    sys.modules[name] = result
    source = subprocess.check_output(["git", "show", f"{REV}:{path}"], text=True)
    exec(compile(source, f"{REV}:{path}", "exec"), result.__dict__)
    return result


glob = module("bench/keys/views/globs.py", "a25_globs")
patterns = module("python/solera/patterns.py", "a25_patterns")
model = module("bench/keys/views/model.py", "a25_model")

print("SOURCE", REV)
for pattern, low, high, match in [
    ("?", "aa", "ca", "b"),
    ("a", "a", "ab", "a"),
    ("tenant/**", "tenant/a/x", "tenant/a/z", "tenant/a/y"),
    ("**", "a/b", "a/d", "a/c"),
]:
    product_match = patterns.Matcher(patterns.spec(pattern))(match)
    keep = glob.intersects(glob.tokens(pattern), low, high)
    assert product_match and low <= match < high and not keep
    print("FALSE_NEGATIVE", repr(pattern), repr(low), repr(high), "match", repr(match))

shape = model.Shape(100_000_000, 4, 4)
jb = shape.base_level()
period = 4**jb
sampled = [1 + len(shape.chain(h, jb)) for h in range(0, period, period // 2000)]
all_runs = [1 + len(shape.chain(h, jb)) for h in range(period)]
print("K_RUNS sampled", statistics.mean(sampled), max(sampled),
      "exhaustive", statistics.mean(all_runs), max(all_runs),
      "worst_head", all_runs.index(max(all_runs)))
assert max(sampled) == 19 and max(all_runs) == 25
reported = model.replay(shape.keys, 4, 4)[-1]
for head in (8639, 32767, 65534):
    # A base at -1 and a daily reader. Every built level remains above floor 0.
    floor = min(head + 1 - model.DAY, 0)
    floor = max(0, floor)
    retained = sum(((head + 1) // 4**j) * shape.entries(j) for j in range(1, jb + 1))
    print("T_STORAGE head", head, "floor", floor, "retained_entries", round(retained),
          "reported_day_entries", round(reported), "ratio", round(retained / reported, 3))

# Each entry is (state before, state after), live state = (generation, payload).
# Absence has no generation here. This checks presence, class and live versions.
def combine(parts):
    parts = [part for part in parts if part is not None]
    if not parts:
        return None
    before, after = parts[0][0], parts[-1][1]
    return None if before is None and after is None else (before, after)


def cls(before, after):
    if before is None:
        return "neither" if after is None else "added"
    if after is None:
        return "removed"
    return "neither" if before[1] == after[1] else "updated"


checks = 0
for history in itertools.product((None, "a", "b"), repeat=7):
    state = None
    states = [None]
    leaves = []
    for commit, payload in enumerate(history):
        old = state
        if payload != (old[1] if old else None):
            state = None if payload is None else (commit + 1, payload)
            leaves.append((old, state))
        else:
            leaves.append(None)  # empty commit
        states.append(state)
    for first in range(len(leaves)):
        for last in range(first, len(leaves)):
            for max_level in (0, 1):
                cover = shape.cover(first, last, max_level)
                at, chunks = first, []
                for level in cover:
                    width = 4**level
                    chunks.append(combine(leaves[at:at + width]))
                    at += width
                net = combine(chunks)
                before, after = (states[first], states[last + 1])
                got = cls(*net) if net is not None else "neither"
                assert got == cls(before, after), (history, first, last, cover, net)
                if net is not None and after is not None:
                    assert net[1] == after
                checks += 1
print("ALGEBRA_PASS", checks, "one-key histories/ranges/covers; classes, presence, live generations/payloads; empty commits")

# Written read-ahead table: older fixed pass N, newer committed selection r.
def written_class(was_live, now_live, now_generation, delivered_generation):
    if was_live and now_live:
        return "skip" if now_generation <= delivered_generation else "updated"
    return {(True, False): "removed", (False, True): "added", (False, False): "skip"}[(was_live, now_live)]

assert written_class(True, False, 0, 50) == "removed"
assert written_class(False, True, 30, 50) == "added"
print("READ_AHEAD_COUNTEREXAMPLES N=4 r=5: add-at-5 is removed by K@4; remove-at-5 is re-added by K@4")

# Position-only compaction loses an active pass's upper endpoint.
histories = [("v1", "v2"), ("v3", "v2")]
summaries = [(None, final) for at7, final in histories]
assert summaries[0] == summaries[1]
assert histories[0][0] != histories[1][0]
print("RETENTION_COUNTEREXAMPLE readers start at 4 and 16, old pass N=7: compact [4,15] has identical before/after for histories with different state at 7")

# Deterministic names in one engine epoch, two index lives under the same prefix.
def name(level, start, epoch):
    return f"keys/feed/_/t{level}-{start:012d}-e{epoch}.kx"

old_path = name(1, 0, 7)
new_path = name(1, 0, 7)
assert old_path == new_path
store = {old_path: "old-life data"}
pending_gc = [old_path]
store[new_path] = "new-life data"
for path in pending_gc:
    del store[path]
assert new_path not in store
print("LIFE_COUNTEREXAMPLE epoch=7 reset/feed commits 0..3: old GC deletes new-life pack", new_path)
print("LIMITS design traces are not an engine/S3 concurrency test; tombstone generation identity is not normalized by this oracle")
