"""Drives a native `Merge` synchronously over files held in memory (tests)."""

from solera import _native


def segments(files, per):
    """Each file's blocks, `per` at a time: `(data, [(offset, size, crc)], codec)`."""

    for data in files:
        idx = _native.parse_index(data, len(data))
        blocks = idx["blocks"]
        for i in range(0, len(blocks), per):
            run = blocks[i : i + per]
            start, end = run[0][1], run[-1][1] + run[-1][2]
            yield data[start:end], [(off - start, size, crc) for _, off, size, _, crc in run], idx["codec"]


def drive(job, runs, rows=(), per=3, on_garbage=None):
    """Runs `job` over `runs` (each a list of whole files, in key order) and,
    for a streamed replacement, the chunks `rows`; returns the files written,
    and hands a compaction's garbage files to `on_garbage`."""

    feeds = [segments(files, per) for files in runs]
    rows = iter(rows)
    out = []
    while (step := job.step()) is not None:
        kind, x = step
        if kind == "run":
            seg = next(feeds[x], None)
            job.end(x) if seg is None else job.feed(x, *seg)
        elif kind == "rows":
            chunk = next(rows, None)
            job.end_rows() if chunk is None else job.feed_rows(chunk)
        elif kind == "garbage":
            on_garbage(x)
        else:
            out.append(x)
    return out
