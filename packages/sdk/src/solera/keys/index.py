"""Key index operations (docs/object-store-state.md §6).

An index is one log-structured merge tree of `.kx` files per keyed output
and partition. `IndexState` is the engine-held record of which files exist;
it is plain data, changed only through its pure transition methods, so the
engine can journal it. `KeyIndex` does the I/O: computing a commit's delta,
writing delta files, paging, reading pending deltas, and compaction.

Levels: level 0 holds delta files, one per commit, with overlapping key
ranges, newest first. Levels 1+ hold compacted files with non-overlapping
ranges; deeper levels are older. For any key, the newest file holding it
has its current entry.
"""

from __future__ import annotations

import asyncio
import bisect
import itertools
import math
from dataclasses import dataclass, field, replace
from urllib.parse import quote

from ..ids import ulid
from . import (
    FOOTER_SIZE,
    IMPL,
    bloom_check_keys,
    bloom_check_pairs,
    bloom_check_tombstones,
    check_block,
    encode_file,
    lookup,
    merge_files,
    merge_range,
    parse_footer,
    parse_index,
    parse_tail,
    replace_diff,
    sort_entries,
)
from .io import RANGE, ObjectIO

# -- engine-held state ---------------------------------------------------------------


def key_str(key: bytes) -> str:
    return key.decode("utf-8", "surrogateescape")


def key_bytes(key: str) -> bytes:
    return key.encode("utf-8", "surrogateescape")


_s, _b = key_str, key_bytes


def index_prefix(output: str, scope: str) -> str:
    """Where a new index's files go: `keys/{output}/{scope}/` (`_` for the
    unpartitioned scope). An index keeps its prefix when its output is renamed."""

    return f"keys/{output}/{quote(scope or '_', safe='')}/"


@dataclass(frozen=True)
class FileInfo:
    name: str
    level: int
    min: bytes
    max: bytes
    entries: int
    size: int
    tail: int  # bytes from the start of the filters to the end of the file
    index: int  # bytes from the start of the block index to the end of the file

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "level": self.level,
            "min": _s(self.min),
            "max": _s(self.max),
            "entries": self.entries,
            "size": self.size,
            "tail": self.tail,
            "index": self.index,
        }

    @classmethod
    def from_json(cls, d: dict) -> FileInfo:
        return cls(
            d["name"], d["level"], _b(d["min"]), _b(d["max"]), d["entries"], d["size"], d["tail"], d["index"]
        )

    @classmethod
    def describe(cls, name: str, level: int, data: bytes) -> FileInfo:
        footer = parse_footer(data[-FOOTER_SIZE:])
        tail = parse_tail(data[footer["filters_offset"] :], len(data))
        return cls(
            name,
            level,
            tail["min_key"],
            tail["max_key"],
            footer["entries"],
            len(data),
            len(data) - footer["filters_offset"],
            len(data) - footer["index_offset"],
        )


@dataclass(frozen=True)
class IndexState:
    """What the engine holds per index: `count` live keys (exact unless
    `count_exact` is false, see §6), the files by level, and the delta log
    consumers read — `(batch, files)`, oldest first. A delta file stays in
    the log after compaction merges it out of the levels, until no consumer
    needs it."""

    count: int = 0
    count_exact: bool = True
    files: tuple[FileInfo, ...] = ()
    log: tuple[tuple[int, tuple[FileInfo, ...]], ...] = ()
    prefix: str = ""  # where the files live (`index_prefix` when the index was created)

    def to_json(self) -> dict:
        return {
            "prefix": self.prefix,
            "count": self.count,
            "count_exact": self.count_exact,
            "files": [f.to_json() for f in self.files],
            "log": [[b, [f.to_json() for f in fs]] for b, fs in self.log],
        }

    @classmethod
    def from_json(cls, d: dict | None) -> IndexState:
        if not d:
            return cls()
        return cls(
            d["count"],
            d.get("count_exact", True),
            tuple(FileInfo.from_json(f) for f in d["files"]),
            tuple((b, tuple(FileInfo.from_json(f) for f in fs)) for b, fs in d["log"]),
            d.get("prefix", ""),
        )

    def path(self, name: str) -> str:
        return f"{self.prefix}{name}.kx"

    def pinned(self, log_from: int | None = None, log_to: int | None = None) -> IndexState:
        """The part of the index one reader needs: the levels, and the log
        entries in `[log_from, log_to]` (none when `log_from` is None)."""

        if log_from is None:
            return replace(self, log=())
        hi = log_to if log_to is not None else math.inf
        return replace(self, log=tuple(e for e in self.log if log_from <= e[0] <= hi))

    def covers(self, first: int, last: int) -> bool:
        """Whether the log still holds every batch in `[first, last]`."""

        logged = {b for b, _ in self.log}
        return all(b in logged for b in range(first, last + 1))

    # -- views ------------------------------------------------------------------------

    def level(self, n: int) -> list[FileInfo]:
        files = [f for f in self.files if f.level == n]
        if n == 0:
            return sorted(files, key=lambda f: f.name, reverse=True)  # newest first
        return sorted(files, key=lambda f: f.min)

    @property
    def depth(self) -> int:
        return max((f.level for f in self.files), default=0)

    def newest_first(self) -> list[list[FileInfo]]:
        """Levels in recency order: each level-0 file alone, then levels 1..depth."""

        out = [[f] for f in self.level(0)]
        for n in range(1, self.depth + 1):
            files = self.level(n)
            if files:
                out.append(files)
        return out

    def referenced(self) -> set[str]:
        """Every file name the index still needs: its levels and its log."""

        return {f.name for f in self.files} | {f.name for _, fs in self.log for f in fs}

    # -- transitions (pure) ----------------------------------------------------------------

    def committed(self, batch: int, delta: DeltaFiles, *, keep_log: bool) -> IndexState:
        """Install a commit's delta files. An empty index takes them straight into level 1."""

        level = 1 if not self.files else 0
        placed = tuple(replace(f, level=level) for f in delta.files)
        log = self.log + ((batch, placed),) if keep_log and placed else self.log
        return IndexState(
            count=self.count + delta.added - delta.removed,
            count_exact=self.count_exact and delta.exact,
            files=self.files + placed,
            log=log,
            prefix=self.prefix,
        )

    def compacted(
        self, added: list[FileInfo], removed: list[str], *, recount: int | None = None
    ) -> IndexState:
        """Swap compaction inputs for outputs (a moved file is both, under one name)."""

        gone = set(removed)
        return IndexState(
            count=recount if recount is not None else self.count,
            count_exact=True if recount is not None else self.count_exact,
            files=tuple(f for f in self.files if f.name not in gone) + tuple(added),
            log=self.log,
            prefix=self.prefix,
        )

    def truncated(self, lowest_needed_batch: int | None) -> IndexState:
        """Drop log entries no consumer still needs (`None`: no consumers at all)."""

        if lowest_needed_batch is None:
            return replace(self, log=())
        return replace(self, log=tuple(e for e in self.log if e[0] >= lowest_needed_batch))


@dataclass(frozen=True)
class Delta:
    """A commit's delta: entries sorted by key (`deleted` 1 for removals), and
    how the live key count changes. `exact` is false when a count change was
    inferred from a filter rather than read."""

    keys: list[bytes]
    versions: list[bytes]
    deleted: bytes
    added: int
    removed: int
    exact: bool

    def __len__(self) -> int:
        return len(self.keys)


@dataclass(frozen=True)
class DeltaFiles:
    files: list[FileInfo]
    added: int
    removed: int
    exact: bool

    def to_json(self) -> dict:
        return {
            "files": [f.to_json() for f in self.files],
            "added": self.added,
            "removed": self.removed,
            "exact": self.exact,
        }

    @classmethod
    def from_json(cls, d: dict) -> DeltaFiles:
        return cls([FileInfo.from_json(f) for f in d["files"]], d["added"], d["removed"], d["exact"])


@dataclass
class Options:
    block_size: int = 64 * 1024
    level: int = 1
    bits_per_item: int = 14
    k: int = 10
    max_file_bytes: int = 64 * 2**20
    whole_threshold: int = 2 * RANGE  # levels this small are read whole: no more requests than tail + block
    latency_budget: float = 2.0  # seconds; the read strategy's tie-breaker
    concurrency: int = 64
    # Planning estimates: a remote store, and this implementation's CPU (bench/keys/results.md).
    request_latency: float = 0.03
    bandwidth: float = 500e6  # all requests together
    connection_bandwidth: float = 80e6  # one request
    decode_rate: float = 4.5e6 if IMPL == "native" else 1e6  # entries per second
    check_rate: float = 0.4e6 if IMPL == "native" else 0.12e6  # keys through a file's filters per second
    l0_max_files: int = 8
    l0_max_bytes: int = 64 * 2**20
    level_base: int = 64 * 2**20
    fanout: int = 10


# -- reading ------------------------------------------------------------------------


@dataclass
class _Parsed:
    info: FileInfo
    tail: dict  # parsed index, plus the filters when `filters`
    filters: bool = True
    data: bytes | None = None  # the file from its start through at least its last block
    window: dict[int, bytes] = field(default_factory=dict)  # the blocks a scan's last page fetched
    firsts: list[bytes] = field(init=False)

    def __post_init__(self):
        self.firsts = [b[0] for b in self.tail["blocks"]]

    def block_of(self, key: bytes) -> int:
        """Index of the only block that could hold `key`, or -1."""

        if not self.tail["blocks"] or key < self.info.min or key > self.info.max:
            return -1
        return bisect.bisect_right(self.firsts, key) - 1

    def runs(self, wanted) -> list[list[int]]:
        """Sorted block indexes grouped into range reads: consecutive blocks, up to `RANGE` each."""

        blocks = self.tail["blocks"]
        out: list[list[int]] = []
        for i in sorted(wanted):
            if out and out[-1][-1] == i - 1 and blocks[i][1] + blocks[i][2] - blocks[out[-1][0]][1] <= RANGE:
                out[-1].append(i)
            else:
                out.append([i])
        return out

    def span(self, run: list[int]) -> tuple[int, int]:
        blocks = self.tail["blocks"]
        return blocks[run[0]][1], blocks[run[-1]][1] + blocks[run[-1]][2]


@dataclass(frozen=True)
class Cost:
    """What a set of reads costs: requests, bytes, the largest request, and
    entries to decode."""

    requests: int = 0
    nbytes: int = 0
    largest: int = 0
    entries: int = 0

    def __add__(self, other: Cost) -> Cost:
        return Cost(
            self.requests + other.requests,
            self.nbytes + other.nbytes,
            max(self.largest, other.largest),
            self.entries + other.entries,
        )


class KeyIndex:
    """I/O over one index. `state` is the pinned `IndexState` to read."""

    def __init__(self, io: ObjectIO, prefix: str | None, state: IndexState, options: Options | None = None):
        self.io = io
        self.prefix = (prefix if prefix is not None else state.prefix).rstrip("/") + "/"
        self.state = state
        self.o = options or Options()
        self._parsed: dict[str, _Parsed] = {}

    def path(self, name: str) -> str:
        return f"{self.prefix}{name}.kx"

    # -- file access ---------------------------------------------------------------------

    def _small(self, f: FileInfo) -> bool:
        """Read whole at once rather than tail, then blocks: mostly tail anyway, or
        cheaper to transfer than a second round trip."""

        return f.size <= 2 * f.tail or f.size <= self.o.request_latency * self.o.connection_bandwidth

    async def _open(self, f: FileInfo, *, data: bool = False, filters: bool = True) -> _Parsed:
        """A parsed file: its block index, plus its filters when `filters`, plus
        its data blocks when `data`. Reads only what was not read before."""

        p = self._parsed.get(f.name)
        if p is None or (filters and not p.filters):
            if data or self._small(f):
                raw = await self.io.read_whole(self.path(f.name), f.size)
                p = _Parsed(f, parse_tail(raw[f.size - f.tail :], f.size), data=raw)
            elif filters:
                raw = await self.io.read(self.path(f.name), f.size - f.tail, f.size, f.size)
                p = _Parsed(f, parse_tail(raw, f.size))
            else:
                raw = await self.io.read(self.path(f.name), f.size - f.index, f.size, f.size)
                p = _Parsed(f, parse_index(raw, f.size), filters=False)
            self._parsed[f.name] = p
        if data and p.data is None:
            p.data = await self.io.read(self.path(f.name), 0, f.size - f.tail, f.size)
        return p

    async def _blocks(self, p: _Parsed, wanted) -> dict[int, bytes]:
        """Fetch blocks by index, consecutive ones as a single range read."""

        blocks = p.tail["blocks"]
        out: dict[int, bytes] = {}
        wanted = set(wanted)
        for i in wanted & p.window.keys():
            out[i] = p.window[i]
        wanted -= out.keys()
        if p.data is not None:
            for i in wanted:
                _, off, size, _, crc = blocks[i]
                out[i] = p.data[off : off + size]
                check_block(out[i], crc)
            return out

        async def fetch(run: list[int]):
            start, end = p.span(run)
            data = await self.io.read(self.path(p.info.name), start, end, p.info.size)
            for i in run:
                _, off, size, _, crc = blocks[i]
                out[i] = data[off - start : off - start + size]
                check_block(out[i], crc)

        await asyncio.gather(*(fetch(r) for r in p.runs(wanted)))
        return out

    async def _all_blocks(self, f: FileInfo) -> tuple[int, list[bytes]]:
        p = await self._open(f, data=True)
        fetched = await self._blocks(p, range(len(p.tail["blocks"])))
        return p.tail["codec"], [fetched[i] for i in range(len(fetched))]

    def _estimate(self, cost: Cost, checks: int = 0) -> float:
        """Seconds for `cost`, plus `checks` filter checks: request rounds, transfer, and CPU."""

        o = self.o
        transfer = max(cost.nbytes / o.bandwidth, cost.largest / o.connection_bandwidth)
        rounds = math.ceil(cost.requests / o.concurrency) * o.request_latency
        return rounds + transfer + cost.entries / o.decode_rate + checks / o.check_rate

    # -- a commit's delta ----------------------------------------------------------------

    async def changes(
        self,
        keys: list[bytes],
        versions: list[bytes],
        removes: list[bytes] = (),
        *,
        replace: bool = False,
    ) -> Delta:
        """Which written entries change the index.

        `replace=False`: upsert `keys` at `versions`, delete `removes`.
        `replace=True`: the written entries are the whole new content; every
        live key not written is deleted."""

        keys, versions, _ = sort_entries(list(keys), list(versions), bytes(len(keys)))
        keys, versions = list(keys), list(versions)
        if replace:
            if removes:
                raise ValueError("a replacement has no separate removes")
            return await self._replace(keys, versions)
        removes = sorted(set(removes))
        if removes and set(removes) & set(keys):
            raise ValueError("a key cannot be both written and removed")
        return await self._patch(keys, versions, removes)

    async def _replace(self, keys: list[bytes], versions: list[bytes]) -> Delta:
        runs, codec = [], 1
        for level in self.state.newest_first():
            run = []
            for file_codec, blocks in await asyncio.gather(*(self._all_blocks(f) for f in level)):
                codec = file_codec
                run += blocks
            runs.append(run)
        changed, existed, removed, _live = replace_diff(runs, codec, keys, versions)
        idx = [n for n in range(len(keys)) if changed[n]]
        out_k, out_v, out_d = [], [], bytearray()
        added = i = j = 0
        while i < len(idx) or j < len(removed):
            if j >= len(removed) or (i < len(idx) and keys[idx[i]] < removed[j]):
                n = idx[i]
                out_k.append(keys[n])
                out_v.append(versions[n])
                out_d.append(0)
                added += 0 if existed[n] else 1
                i += 1
            else:
                out_k.append(removed[j])
                out_v.append(b"")
                out_d.append(1)
                j += 1
        return Delta(out_k, out_v, bytes(out_d), added, len(removed), True)

    async def _patch(self, keys: list[bytes], versions: list[bytes], removes: list[bytes]) -> Delta:
        want = dict(zip(keys, versions, strict=True))
        unresolved = sorted(set(keys) | set(removes))
        known: dict[bytes, tuple[bool, bytes]] = {}  # key -> (live, version), read exactly
        absent: set[bytes] = set()  # keys no file holds
        exact = True
        levels = self.state.newest_first()
        # Newest first, levels are read whole — exact answers, no filters — up to the first
        # one too big to; from there every level goes through its filters, since "definitely
        # changed" must hold across every level that could hold the key.
        n = next((i for i, level in enumerate(levels) if not self._read_whole(level)), len(levels))
        whole, filtered = levels[:n], levels[n:]

        # 1. The whole levels, fetched at once and then consulted newest first.
        candidates = [self._candidates(level, unresolved) for level in whole]
        await asyncio.gather(
            *(
                self._open(f, data=True)
                for level, c in zip(whole, candidates, strict=True)
                for f in level
                if f.name in c
            )
        )
        for level in whole:
            if not unresolved:
                break
            found = await self._exact(level, self._candidates(level, unresolved))
            known.update(found)
            unresolved = [k for k in unresolved if k not in found]

        # 2. The rest through their filters; only "maybe" keys get block reads.
        if unresolved and filtered:
            verdicts, holders, spent = await self._filter(filtered, unresolved, want)
            maybe = []
            for key in unresolved:
                verdict = verdicts[key]
                if verdict == "absent":
                    absent.add(key)
                elif verdict == "changed":
                    # Live at another version — or, behind a false-positive key
                    # filter, not there at all: counted as an existing key.
                    exact = False
                    known[key] = (True, None)
                else:
                    maybe.append(key)
            if maybe:
                found = await self._resolve(filtered, holders, maybe, spent)
                known.update(found)
                absent.update(k for k in maybe if k not in found)
        else:
            absent.update(unresolved)

        out_k, out_v, out_d = [], [], bytearray()
        added = removed = 0
        rm = set(removes)
        for key in sorted(set(keys) | rm):
            live, version = (False, None) if key in absent else known.get(key, (False, None))
            if key in rm:
                if live:
                    out_k.append(key)
                    out_v.append(b"")
                    out_d.append(1)
                    removed += 1
                continue
            v = want[key]
            if live and version == v:
                continue  # rewritten unchanged
            out_k.append(key)
            out_v.append(v)
            out_d.append(0)
            added += 0 if live else 1
        return Delta(out_k, out_v, bytes(out_d), added, removed, exact)

    def _candidates(self, level: list[FileInfo], keys: list[bytes]) -> dict[str, list[bytes]]:
        """Per file of a level, the sorted keys inside its key range."""

        out: dict[str, list[bytes]] = {}
        if len(level) == 1:
            f = level[0]
            lo, hi = bisect.bisect_left(keys, f.min), bisect.bisect_right(keys, f.max)
            if lo < hi:
                out[f.name] = keys[lo:hi]
            return out
        mins = [f.min for f in level]  # a level 1+ has non-overlapping files, sorted by min
        for key in keys:
            i = bisect.bisect_right(mins, key) - 1
            if i >= 0 and key <= level[i].max:
                out.setdefault(level[i].name, []).append(key)
        return out

    def _read_whole(self, level: list[FileInfo]) -> bool:
        """Whether a level is small enough to read whole without looking at its filters."""

        return sum(f.size for f in level) <= self.o.whole_threshold

    async def _exact(self, level, candidates) -> dict[bytes, tuple[bool, bytes]]:
        by_name = {f.name: f for f in level}

        async def one(name: str, keys: list[bytes]):
            return await self._lookup(await self._open(by_name[name], data=True), keys)

        found: dict[bytes, tuple[bool, bytes]] = {}
        for part in await asyncio.gather(*(one(name, keys) for name, keys in candidates.items())):
            found.update(part)
        return found

    async def _lookup(self, p: _Parsed, keys: list[bytes]) -> dict[bytes, tuple[bool, bytes]]:
        """`(live, version)` of each of the sorted `keys` the file holds."""

        blocks_for: dict[int, list[bytes]] = {}
        for key in keys:
            b = p.block_of(key)
            if b >= 0:
                blocks_for.setdefault(b, []).append(key)
        if not blocks_for:
            return {}
        fetched = await self._blocks(p, blocks_for)
        found = {}
        for b, bkeys in blocks_for.items():
            hit, vers, dels = lookup([fetched[b]], p.tail["codec"], bkeys)
            for key, h, v, d in zip(bkeys, hit, vers, dels, strict=True):
                if h:
                    found[key] = (not d, v)
        return found

    async def _filter(self, levels, keys, want):
        """Classify keys with the filters of every file that could hold them:
        "absent" (no key filter matches), "changed" (a written key that no pair
        filter and no tombstone filter matches: live, at another version), or
        "maybe" (needs an exact read). Also returns, per file, the keys its key
        filter matched — the only files an exact read of them needs — and the
        estimated seconds this took."""

        key_hit = dict.fromkeys(keys, False)
        pair_hit = dict.fromkeys(keys, False)
        tomb_hit = dict.fromkeys(keys, False)
        holders: dict[str, list[bytes]] = {}
        per_file = []
        for level in levels:
            by_name = {f.name: f for f in level}
            per_file += [(by_name[n], ks) for n, ks in self._candidates(level, keys).items()]
        parsed = await asyncio.gather(*(self._open(f) for f, _ in per_file))
        tails = [f.tail for f, _ in per_file]
        spent = self._estimate(
            Cost(len(tails), sum(tails), max(tails, default=0)), sum(len(ks) for _, ks in per_file)
        )
        for p, (_, ks) in zip(parsed, per_file, strict=True):
            nb, kk, bits = p.tail["key_filter"]
            held = [k for k, h in zip(ks, bloom_check_keys(bits, nb, kk, ks), strict=True) if h]
            if not held:
                continue
            holders[p.info.name] = held
            for k in held:
                key_hit[k] = True
            nb, kk, bits = p.tail["tomb_filter"]
            for k, h in zip(held, bloom_check_tombstones(bits, nb, kk, held), strict=True):
                tomb_hit[k] = tomb_hit[k] or bool(h)
            pks = [k for k in held if k in want]
            if pks:
                nb, kk, bits = p.tail["pair_filter"]
                for k, h in zip(
                    pks, bloom_check_pairs(bits, nb, kk, pks, [want[k] for k in pks]), strict=True
                ):
                    pair_hit[k] = pair_hit[k] or bool(h)
        out = {}
        for k in keys:
            if not key_hit[k]:
                out[k] = "absent"
            elif k in want and not pair_hit[k] and not tomb_hit[k]:
                out[k] = "changed"
            else:
                out[k] = "maybe"
        return out, holders, spent

    async def _resolve(self, levels, holders, maybe, spent: float) -> dict[bytes, tuple[bool, bytes]]:
        """Exact lookups of `maybe` keys in every file whose key filter matched
        them, all levels at once; each key's newest entry wins. Per level, the
        planner reads just the blocks, or the rest of its files whole."""

        wanted = set(maybe)
        needs = []  # per level, newest first: (parsed file, keys to look up)
        for level in levels:
            row = []
            for f in level:
                ks = [k for k in holders.get(f.name, ()) if k in wanted]
                if ks:
                    row.append((self._parsed[f.name], ks))
            needs.append(row)
        rest = self._plan_reads(needs, spent)

        async def one(p, ks, whole):
            if whole:
                await self._open(p.info, data=True)
            return await self._lookup(p, ks)

        found: dict[bytes, tuple[bool, bytes]] = {}
        parts = await asyncio.gather(
            *(one(p, ks, whole) for row, whole in zip(needs, rest, strict=True) for p, ks in row)
        )
        for part in parts:  # newest first
            for key, entry in part.items():
                found.setdefault(key, entry)
        return found

    def _plan_reads(self, needs, spent: float) -> list[bool]:
        """Per level: read the rest of its files whole (True), or only the blocks
        the lookups need (False)? Both decode the same blocks; they differ in
        requests and bytes. The combination with the fewest requests that fits
        the latency budget wins, else the fastest. The `spent` seconds — tails
        and filter checks — count toward the budget."""

        best = None
        for combo, cost in self._read_options(needs):
            total = spent + self._estimate(cost)
            rank = (0, cost.requests, total) if total <= self.o.latency_budget else (1, total, cost.requests)
            if best is None or rank < best[0]:
                best = (rank, combo)
        return list(best[1])

    def _read_options(self, needs):
        """Every way to read `needs` (per level: files and their keys), level by
        level blocks or rest, with its cost."""

        per_level = []
        for row in needs:
            blocks = rest = Cost()
            for p, ks in row:
                wanted = {p.block_of(k) for k in ks} - {-1}
                entries = sum(p.tail["blocks"][i][3] for i in wanted)
                if p.data is not None:  # read whole already
                    blocks, rest = blocks + Cost(entries=entries), rest + Cost(entries=entries)
                    continue
                spans = [p.span(r) for r in p.runs(wanted)]
                blocks += Cost(
                    len(spans),
                    sum(e - s for s, e in spans),
                    max((e - s for s, e in spans), default=0),
                    entries,
                )
                size = p.info.size - p.info.tail
                rest += Cost(math.ceil(size / RANGE), size, min(size, RANGE), entries)
            per_level.append((blocks, rest))
        for combo in itertools.product((False, True), repeat=len(needs)):
            cost = Cost()
            for whole, options in zip(combo, per_level, strict=True):
                cost += options[whole]
            yield combo, cost

    # -- writing ------------------------------------------------------------------------

    async def write(self, batch: int, attempt: str, delta: Delta) -> DeltaFiles:
        """Write a delta as the batch's files, `{batch}-{attempt}[.{n}]`: the
        attempt id keeps a retried batch from colliding with its own upload."""

        chunks = self._split(delta.keys, delta.versions, delta.deleted)
        files = []
        for n, (ks, vs, ds) in enumerate(chunks):
            data = encode_file(
                ks,
                vs,
                ds,
                block_size=self.o.block_size,
                level=self.o.level,
                bits_per_item=self.o.bits_per_item,
                k=self.o.k,
            )
            name = f"{batch:012d}-{attempt}" + (f".{n:04d}" if len(chunks) > 1 else "")
            await self.io.write(self.path(name), data)
            files.append(FileInfo.describe(name, 0, data))
        return DeltaFiles(files, delta.added, delta.removed, delta.exact)

    def _split(self, keys, versions, deleted):
        """Consecutive chunks of about `max_file_bytes` once compressed (~2x)."""

        if not keys:
            return []
        out, start, size = [], 0, 0
        budget = 2 * self.o.max_file_bytes
        for i in range(len(keys)):
            size += len(keys[i]) + len(versions[i]) + 4
            if size >= budget:
                out.append((keys[start : i + 1], versions[start : i + 1], deleted[start : i + 1]))
                start, size = i + 1, 0
        if start < len(keys):
            out.append((keys[start:], versions[start:], deleted[start:]))
        return out

    # -- scans: full delivery and pending deltas ----------------------------------------------

    async def _scan(self, levels: list[list[FileInfo]], after: bytes | None, limit: int, drop_deleted: bool):
        """Up to `limit` entries of the merged view with keys > `after`, and the
        cursor to continue from (`None` when the view is exhausted).

        `levels` are newest first; the files within one level never overlap.
        Files are chosen from their metadata before anything is read — per
        level, only those covering the next `limit` keys — and only their
        block indexes are read, never their filters."""

        chosen, bound = [], None
        for level in levels:
            got = 0
            for f in sorted(level, key=lambda f: f.min):
                if after is not None and f.max <= after:
                    continue
                if got > limit:
                    # Everything below this file's first key is complete in the chosen ones.
                    bound = f.min if bound is None else min(bound, f.min)
                    break
                chosen.append(f)
                got += f.entries
        parsed = await asyncio.gather(*(self._open(f, filters=False) for f in chosen))
        spans = []
        for p in parsed:
            blocks = p.tail["blocks"]
            start = max(0, bisect.bisect_right(p.firsts, after) - 1) if after is not None and blocks else 0
            end, got = start, 0
            # Enough blocks for `limit` entries, and at least one block past the
            # one holding `after`, so every call makes progress.
            while end < len(blocks) and (got < limit + 1 or end - start < 2):
                got += blocks[end][3]
                end += 1
            spans.append(range(start, end))
            if end < len(blocks):
                nxt = blocks[end][0]  # everything below the next unfetched block is complete
                bound = nxt if bound is None else min(bound, nxt)
        fetched = await asyncio.gather(
            *(self._blocks(p, span) for p, span in zip(parsed, spans, strict=True))
        )
        runs = []
        for p, span, got in zip(parsed, spans, fetched, strict=True):
            runs.append([got[i] for i in span])
            if p.data is None:
                p.window = got  # the next page starts in it: a file never pays for the same block twice
        codec = parsed[0].tail["codec"] if parsed else 1
        keys, versions, deleted = merge_range(runs, codec, after, None, False)
        out_k, out_v, out_d = [], [], bytearray()
        cursor = after
        for i, key in enumerate(keys):
            if bound is not None and key >= bound:
                return out_k, out_v, bytes(out_d), cursor
            cursor = key
            if drop_deleted and deleted[i]:
                continue
            out_k.append(key)
            out_v.append(versions[i])
            out_d.append(deleted[i])
            if len(out_k) == limit:
                more = i + 1 < len(keys) or bound is not None
                return out_k, out_v, bytes(out_d), cursor if more else None
        return out_k, out_v, bytes(out_d), cursor if bound is not None else None

    async def page(self, after: bytes | None, limit: int):
        """One page of the full delivery: live keys > `after`, their versions, and
        the next cursor (`None` when done)."""

        keys, versions, _, nxt = await self._scan(self.state.newest_first(), after, limit, drop_deleted=True)
        return keys, versions, nxt

    async def pending(self, first_batch: int, last_batch: int, after: bytes | None, limit: int):
        """Changes in batches `[first_batch, last_batch]`, newest winning, keys > `after`:
        keys, versions, deleted flags, and the next cursor (`None` when done)."""

        logged = dict(self.state.log)
        missing = [b for b in range(first_batch, last_batch + 1) if b not in logged]
        if missing:
            raise LookupError(f"delta log no longer holds batches {missing[:5]}")
        # Each batch is a level of its own: its files (a split delta) never overlap.
        levels = [list(logged[b]) for b in range(last_batch, first_batch - 1, -1)]
        return await self._scan(levels, after, limit, drop_deleted=False)

    async def recount(self, page: int = 100_000) -> int:
        """Count live keys exactly by scanning the whole index."""

        n, after = 0, None
        while True:
            keys, _, after = await self.page(after, page)
            n += len(keys)
            if after is None:
                return n

    # -- compaction ------------------------------------------------------------------------

    def plan_compaction(self) -> tuple[list[FileInfo], int] | None:
        """The next compaction: input files (newest first) and the output level.

        Level 0 acts once it holds `l0_max_files` files or `l0_max_bytes`. It
        merges into level 1 — with every level-1 file it overlaps, which for
        random keys is all of them — only once it holds a `fanout`-th of level
        1's bytes, so each merge rewrites level 1 for at least that many new
        bytes; until then its files merge among themselves, into one level-0
        file. A level over its target (`level_base · fanout^(n-1)`) pushes one
        file — the one overlapping the next level least — down, merging it with
        the files it overlaps; the deepest level moves down whole, which
        rewrites nothing."""

        s, o = self.state, self.o
        l0 = s.level(0)
        size = sum(f.size for f in l0)
        if l0 and (len(l0) >= o.l0_max_files or size >= o.l0_max_bytes):
            l1 = s.level(1)
            if len(l0) > 1 and size * o.fanout < sum(f.size for f in l1):
                return l0, 0
            lo, hi = min(f.min for f in l0), max(f.max for f in l0)
            return l0 + [f for f in l1 if f.max >= lo and f.min <= hi], 1
        for n in range(1, s.depth + 1):
            files = s.level(n)
            if not files or sum(f.size for f in files) <= o.level_base * o.fanout ** (n - 1):
                continue
            if n == s.depth:
                return files, n + 1
            below = s.level(n + 1)
            pick = min(
                files,
                key=lambda f, below=below: sum(g.size for g in below if g.max >= f.min and g.min <= f.max),
            )
            return [pick] + [g for g in below if g.max >= pick.min and g.min <= pick.max], n + 1
        return None

    async def compact(self, plan=None) -> tuple[list[FileInfo], list[str]] | None:
        """Run one compaction; returns (added files, removed names) for `IndexState.compacted`."""

        plan = plan or self.plan_compaction()
        if plan is None:
            return None
        inputs, out_level = plan
        if out_level > self.state.depth:
            # The deepest level moves down whole: nothing below it to merge with.
            return [replace(f, level=out_level) for f in inputs], [f.name for f in inputs]
        drop = out_level >= self.state.depth  # nothing older below: tombstones can go
        datas = await asyncio.gather(*(self.io.read_whole(self.path(f.name), f.size) for f in inputs))
        merged = merge_files(
            list(datas),
            drop_deleted=drop,
            block_size=self.o.block_size,
            level=self.o.level,
            bits_per_item=self.o.bits_per_item,
            k=self.o.k,
            max_file_bytes=self.o.max_file_bytes,
        )
        # A level-0 file is as recent as its newest input: level 0 orders by name, and delta
        # names start with their batch.
        stamp = ulid() if out_level else f"{inputs[0].name.split('-', 1)[0]}-c{ulid()}"
        added = []
        for n, data in enumerate(merged):
            name = f"c{stamp}-{n:04d}" if out_level else f"{stamp}.{n:04d}"
            await self.io.write(self.path(name), data)
            added.append(FileInfo.describe(name, out_level, data))
        return added, [f.name for f in inputs]
