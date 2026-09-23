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
import math
from dataclasses import dataclass, field, replace
from urllib.parse import quote

from ..ids import ulid
from . import (
    FOOTER_SIZE,
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
    whole_threshold: int = 32 * 2**20  # levels up to this size are always read whole
    latency_budget: float = 2.0  # seconds; the read strategy's tie-breaker
    concurrency: int = 64
    request_latency: float = 0.03  # planning estimates for a remote store
    bandwidth: float = 500e6
    l0_max_files: int = 8
    l0_max_bytes: int = 64 * 2**20
    level_base: int = 64 * 2**20
    fanout: int = 10


# -- reading ------------------------------------------------------------------------


@dataclass
class _Parsed:
    info: FileInfo
    tail: dict  # parsed index, plus the filters when `filters`
    firsts: list[bytes] = field(default_factory=list)
    whole: bytes | None = None
    filters: bool = True

    def block_of(self, key: bytes) -> int:
        """Index of the only block that could hold `key`, or -1."""

        if not self.tail["blocks"] or key < self.info.min or key > self.info.max:
            return -1
        return bisect.bisect_right(self.firsts, key) - 1


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

    async def _open(self, f: FileInfo, *, whole: bool = False, filters: bool = True) -> _Parsed:
        """A parsed file: its block index, plus its filters when `filters`, plus
        all its bytes when `whole`. Reads only what was not read before."""

        p = self._parsed.get(f.name)
        if p is not None and (p.whole is not None or (not whole and (p.filters or not filters))):
            return p
        if whole or f.size <= 2 * f.tail:
            # Small files, and files that are mostly tail anyway: one read of everything.
            data = await self.io.read_whole(self.path(f.name), f.size)
            tail = parse_tail(data[f.size - f.tail :], f.size)
            p = _Parsed(f, tail, [b[0] for b in tail["blocks"]], data)
        elif filters:
            raw = await self.io.read(self.path(f.name), f.size - f.tail, f.size, f.size)
            tail = parse_tail(raw, f.size)
            p = _Parsed(f, tail, [b[0] for b in tail["blocks"]])
        else:
            raw = await self.io.read(self.path(f.name), f.size - f.index, f.size, f.size)
            tail = parse_index(raw, f.size)
            p = _Parsed(f, tail, [b[0] for b in tail["blocks"]], filters=False)
        self._parsed[f.name] = p
        return p

    async def _blocks(self, p: _Parsed, wanted: list[int]) -> dict[int, bytes]:
        """Fetch blocks by index, consecutive ones as a single range read."""

        blocks = p.tail["blocks"]
        out: dict[int, bytes] = {}
        wanted = sorted(set(wanted))
        if p.whole is not None:
            for i in wanted:
                _, off, size, _, crc = blocks[i]
                out[i] = p.whole[off : off + size]
                check_block(out[i], crc)
            return out
        runs: list[list[int]] = []
        for i in wanted:
            if (
                runs
                and runs[-1][-1] == i - 1
                and blocks[i][1] + blocks[i][2] - blocks[runs[-1][0]][1] <= RANGE
            ):
                runs[-1].append(i)
            else:
                runs.append([i])

        async def fetch(run: list[int]):
            start = blocks[run[0]][1]
            end = blocks[run[-1]][1] + blocks[run[-1]][2]
            data = await self.io.read(self.path(p.info.name), start, end, p.info.size)
            for i in run:
                _, off, size, _, crc = blocks[i]
                out[i] = data[off - start : off - start + size]
                check_block(out[i], crc)

        await asyncio.gather(*(fetch(r) for r in runs))
        return out

    async def _all_blocks(self, f: FileInfo) -> tuple[int, list[bytes]]:
        p = await self._open(f, whole=True)
        fetched = await self._blocks(p, list(range(len(p.tail["blocks"]))))
        return p.tail["codec"], [fetched[i] for i in range(len(fetched))]

    def _estimate(self, requests: float, nbytes: float) -> float:
        return math.ceil(requests / self.o.concurrency) * self.o.request_latency + nbytes / self.o.bandwidth

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
        filtered: list[list[FileInfo]] = []

        # 1. Newest to oldest, levels cheap enough are read whole: exact answers.
        for level in self.state.newest_first():
            if not unresolved:
                break
            candidates = self._candidates(level, unresolved)
            if not candidates:
                continue
            if filtered or not self._read_whole(level, sum(len(v) for v in candidates.values())):
                # From the first level read through its filters, every older level is too:
                # "definitely changed" must hold across every level that could hold the key.
                filtered.append(level)
                continue
            found = await self._exact(level, candidates, whole=True)
            known.update(found)
            unresolved = [k for k in unresolved if k not in found]

        # 2. The rest through their filters; only "maybe" keys get block reads.
        if unresolved and filtered:
            verdicts = await self._filter(filtered, unresolved, want)
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
            for level in filtered:
                if not maybe:
                    break
                found = await self._exact(level, self._candidates(level, maybe), whole=False)
                known.update(found)
                maybe = [k for k in maybe if k not in found]
            absent.update(maybe)
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

    def _read_whole(self, level: list[FileInfo], nkeys: int) -> bool:
        """Read this level whole (exact, no filters), or through its filters?

        The cheapest option in requests that fits the latency budget wins,
        else the fastest; small levels and level-0 files are always read whole."""

        size = sum(f.size for f in level)
        if level[0].level == 0 or size <= self.o.whole_threshold:
            return True
        whole = (math.ceil(size / RANGE), size)
        # Through the filters: every candidate tail, then blocks for the few keys
        # the filters can't clear — assume about 1%, at least one.
        filtered = (len(level) + max(1, nkeys // 100), sum(f.tail for f in level))
        options = [("whole", *whole), ("filtered", *filtered)]
        fits = [o for o in options if self._estimate(o[1], o[2]) <= self.o.latency_budget]
        best = (
            min(fits, key=lambda o: o[1]) if fits else min(options, key=lambda o: self._estimate(o[1], o[2]))
        )
        return best[0] == "whole"

    async def _exact(self, level, candidates, *, whole: bool) -> dict[bytes, tuple[bool, bytes]]:
        by_name = {f.name: f for f in level}
        found: dict[bytes, tuple[bool, bytes]] = {}

        async def one(name: str, keys: list[bytes]):
            p = await self._open(by_name[name], whole=whole)
            blocks_for: dict[int, list[bytes]] = {}
            for key in keys:
                b = p.block_of(key)
                if b >= 0:
                    blocks_for.setdefault(b, []).append(key)
            if not blocks_for:
                return
            fetched = await self._blocks(p, list(blocks_for))
            for b, bkeys in blocks_for.items():
                hit, vers, dels = lookup([fetched[b]], p.tail["codec"], bkeys)
                for key, h, v, d in zip(bkeys, hit, vers, dels, strict=True):
                    if h:
                        found[key] = (not d, v)

        await asyncio.gather(*(one(name, keys) for name, keys in candidates.items()))
        return found

    async def _filter(self, levels, keys, want) -> dict[bytes, str]:
        """Classify keys with the filters of every file that could hold them:
        "absent" (no key filter matches), "changed" (a written key that no pair
        filter and no tombstone filter matches: live, at another version), or
        "maybe" (needs an exact read)."""

        key_hit = dict.fromkeys(keys, False)
        pair_hit = dict.fromkeys(keys, False)
        tomb_hit = dict.fromkeys(keys, False)
        per_file = []
        for level in levels:
            by_name = {f.name: f for f in level}
            per_file += [(by_name[n], ks) for n, ks in self._candidates(level, keys).items()]
        parsed = await asyncio.gather(*(self._open(f) for f, _ in per_file))
        for p, (_, ks) in zip(parsed, per_file, strict=True):
            nb, kk, bits = p.tail["key_filter"]
            for k, h in zip(ks, bloom_check_keys(bits, nb, kk, ks), strict=True):
                key_hit[k] = key_hit[k] or bool(h)
            nb, kk, bits = p.tail["tomb_filter"]
            for k, h in zip(ks, bloom_check_tombstones(bits, nb, kk, ks), strict=True):
                tomb_hit[k] = tomb_hit[k] or bool(h)
            pks = [k for k in ks if k in want]
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
        return out

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
        runs = []
        codec = 1
        for p in parsed:
            codec = p.tail["codec"]
            blocks = p.tail["blocks"]
            if not blocks:
                runs.append([])
                continue
            start = max(0, bisect.bisect_right(p.firsts, after) - 1) if after is not None else 0
            end, got = start, 0
            # Enough blocks for `limit` entries, and at least one block past the
            # one holding `after`, so every call makes progress.
            while end < len(blocks) and (got < limit + 1 or end - start < 2):
                got += blocks[end][3]
                end += 1
            fetched = await self._blocks(p, list(range(start, end)))
            runs.append([fetched[i] for i in range(start, end)])
            if end < len(blocks):
                nxt = blocks[end][0]  # everything below the next unfetched block is complete
                bound = nxt if bound is None else min(bound, nxt)
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

        Level 0 is merged into level 1 once it holds `l0_max_files` files or
        `l0_max_bytes`. A level over its target (`level_base · fanout^(n-1)`)
        pushes one file — the one overlapping the next level least — down,
        merging it with the files it overlaps; the deepest level moves down
        whole, which rewrites nothing."""

        s, o = self.state, self.o
        l0 = s.level(0)
        if l0 and (len(l0) >= o.l0_max_files or sum(f.size for f in l0) >= o.l0_max_bytes):
            lo, hi = min(f.min for f in l0), max(f.max for f in l0)
            below = [f for f in s.level(1) if f.max >= lo and f.min <= hi]
            return l0 + below, 1
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
        stamp = ulid()
        added = []
        for n, data in enumerate(merged):
            name = f"c{stamp}-{n:04d}"
            await self.io.write(self.path(name), data)
            added.append(FileInfo.describe(name, out_level, data))
        return added, [f.name for f in inputs]
