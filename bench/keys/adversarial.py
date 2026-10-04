"""The second review's counterexamples (A12-3, A12-4) replayed under the
`versions` and `capped` policies of spans.py.

    uv run python bench/keys/adversarial.py

- A12-3, an old pass facing newer data: a 100M-key base, then spans of 1
  entry, 1M and 1M entries, one per commit. A pass pinned at commit 0 holds
  endpoints 0 and 1 (its landing point). Does upkeep merge its 1-entry span
  with the 2M newer entries? What does the pass then read?
- A12-4, many large pinned commits: the base, then forty 2M-entry commits,
  an endpoint at each. How many spans remain, and what do the readers at
  those endpoints read?
"""

from __future__ import annotations

from spans import Seg, Sim, Span, Workload


def sim(policy: str, n: int = 10**8) -> Sim:
    return Sim(Workload(n, 1), policy, 1.0, 1e6, None, float("inf"))


def span(c: int, entries: float, n: int) -> Span:
    return Span(c, c, [Seg(c, entries / n, 0.0)])


def reads(s: Sim, p: int, last: int) -> float:
    """What a reader of [p, last] reads: every span overlapping it."""

    return sum(s.entries(r) for r in s.spans if r.b >= p and r.a <= last)


def a12_3(policy: str) -> str:
    s = sim(policy)
    n = s.n
    s.spans += [span(0, 1, n), span(1, 1e6, n), span(2, 1e6, n)]
    live = {0, 1, 3}
    s.upkeep(2, live)
    return f"{len(s.spans)} spans; the pass at [0, 0] reads {reads(s, 0, 0):,.0f} entries for its 1"


def a12_4(policy: str) -> str:
    s = sim(policy)
    n = s.n
    s.spans += [span(c, 2e6, n) for c in range(40)]
    live = set(range(41))
    s.upkeep(39, live)
    worst = max(reads(s, p, p) / 2e6 for p in range(40))
    return (
        f"{len(s.spans)} spans; written {s.written / 1e6:,.0f}M entries; a reader of one commit reads up to "
        f"{worst:.1f}x its 2M"
    )


def main():
    for name, fn in (("A12-3", a12_3), ("A12-4", a12_4)):
        for policy in ("versions", "capped"):
            print(f"{name} {policy:8}: {fn(policy)}")


if __name__ == "__main__":
    main()
