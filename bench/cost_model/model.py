import math

# ---- assumptions (K0 validates the first block) --------------------------------
E_RAW, E_ZIP = 40, 20  # bytes per entry: key ~24 + version ~16; ~2x compression
BLOCK_RAW = 64 * 1024
PER_BLOCK = BLOCK_RAW // E_RAW  # ~1638 entries per block
BLOCK_ZIP = PER_BLOCK * E_ZIP  # ~32 KB per compressed block
FILE_ZIP = 64 * 2**20  # compacted file size cap
UPPER = 0.11  # upper levels ~1/9 of the bottom level (fanout 10)
L0_MAX = 8  # delta files before compaction
WHOLE = 32 * 2**20  # below this, fetch a level whole instead of by block
RANGE = 16 * 2**20  # large reads/writes in 16 MB ranges / multipart parts
CONC, LAT, BW = 64, 0.030, 500e6  # parallel requests, per-request latency, aggregate bytes/s
CPU = {"native": 30e6, "python": 1.5e6}  # entries merged or scanned per second
PUT, GET, GB_MONTH = 5.0 / 1e6, 0.40 / 1e6, 0.023
ATTEMPT_PUT, ATTEMPT_GET = 4, 2  # spec, result, log chunk, joined log / spec + result reads
JOURNAL_PUT = 1  # upper bound: one flush per commit if nothing else is running
FILTER_B = 3.5  # Bloom filters per entry: keys + (key, version) pairs, 14 bits each
FILTER_FP = 0.002  # false-positive rate of one filter check at 14 bits per item
BUDGET = 2.0  # latency budget per commit for the cost-aware read strategy


def size(n):
    return n * E_ZIP


def files(nbytes):
    return max(1, math.ceil(nbytes / FILE_ZIP))


def blocks(n):
    return max(1, math.ceil(n / PER_BLOCK))


def touched(total_blocks, k, clustered):
    # Clustered keys hit consecutive blocks, which a reader fetches as one range read per 16 MB.
    if clustered:
        return max(1, math.ceil(min(total_blocks, math.ceil(k / PER_BLOCK) + 1) * BLOCK_ZIP / RANGE))
    return total_blocks * (1 - (1 - 1 / total_blocks) ** k)


def levels_wa(n):
    return 5 * max(1.0, math.log10(max(n, 1e4) / 1e4)) if n > 1e4 else 3


def n_levels(n):
    return max(1, math.ceil(math.log10(max(n, 1e4) / 1e4)))


def options(lvl_n, lvl_b, k, clustered, warm, share, filters, unchanged, levels):
    """(gets, bytes) for each way of answering "did these k keys change?" against one level."""
    cached_tails = warm
    cached_upper = warm and share == UPPER
    fi = min(files(lvl_b), 2) if clustered else files(lvl_b)  # key ranges select the files
    out = {"whole": (0, 0) if cached_upper else (math.ceil(lvl_b / RANGE), lvl_b)}
    t = touched(blocks(lvl_n), k, clustered)
    out["blocks"] = (
        (0 if cached_tails else fi) + (0 if cached_upper else t),
        (0 if cached_tails else fi * 100e3) + (0 if cached_upper else t * BLOCK_ZIP),
    )
    if filters:
        # A written (key, version) absent from every level's pair filter is a real change: no block read.
        # Unchanged rewrites and false positives ("maybe present") fall back to an exact block read.
        p_maybe = unchanged + (1 - unchanged) * (1 - (1 - FILTER_FP) ** levels)
        tm = touched(blocks(lvl_n), max(1, k * p_maybe), clustered)
        tail_b = fi * 100e3 + (lvl_n * (fi / files(lvl_b))) * FILTER_B
        out["filters"] = (
            (0 if cached_tails else fi) + (0 if cached_upper else tm),
            (0 if cached_tails else tail_b) + (0 if cached_upper else tm * BLOCK_ZIP),
        )
    return out


def choose(opts):
    """Cheapest option that fits the latency budget, else the fastest."""
    fits = {name: gb for name, gb in opts.items() if wall(*gb) <= BUDGET}
    if fits:
        return min(fits.items(), key=lambda kv: (kv[1][0], wall(*kv[1])))
    return min(opts.items(), key=lambda kv: wall(*kv[1]))


def lookup(n, k, clustered=False, warm=False, filters=True, unchanged=0.0):
    """GETs and bytes to find out which of k written keys changed, in an index of n."""
    gets, nbytes = 0.0, 0.0
    gets += 0 if warm else L0_MAX / 2  # the (small) level-0 delta files, fetched whole
    levels = n_levels(n)
    for share in (1.0, UPPER):  # bottom level, then all upper levels together
        lvl_n, lvl_b = n * share, size(n * share)
        if lvl_b <= WHOLE:
            if not warm:
                gets += math.ceil(lvl_b / RANGE)
                nbytes += lvl_b
            continue
        _, (g, b) = choose(options(lvl_n, lvl_b, k, clustered, warm, share, filters, unchanged, levels))
        gets += g
        nbytes += b
    return gets, nbytes


def wall(gets, nbytes):
    return math.ceil(gets / CONC) * LAT + nbytes / BW


def commit(n, k, clustered=False, warm=False, filters=True, unchanged=0.0):
    """One incremental commit writing k keys into an index of n (attempt overhead included)."""
    g, b = lookup(n, k, clustered, warm, filters, unchanged)
    delta_b = size(k)
    puts = max(1, math.ceil(delta_b / RANGE)) + ATTEMPT_PUT + JOURNAL_PUT
    gets = g + ATTEMPT_GET
    # compaction, amortized: each changed entry rewritten ~WA times, read and written in 16 MB ranges
    moved = k * E_ZIP * levels_wa(n)
    puts += moved / RANGE
    gets += moved / RANGE
    return {
        "gets": gets,
        "puts": puts,
        "usd": gets * GET + puts * PUT,
        "wall": wall(g, b + delta_b),
        "compaction_mb": moved / 1e6,
    }


def full_replace(n, changed):
    """A bare return of all n rows: compare every key against the index."""
    b = size(n)
    gets = math.ceil(b / RANGE) + files(b) + ATTEMPT_GET
    puts = max(1, math.ceil(size(changed) / RANGE)) + ATTEMPT_PUT + JOURNAL_PUT
    return {
        "gets": gets,
        "puts": puts,
        "usd": gets * GET + puts * PUT,
        "io_s": wall(gets, b),
        "cpu_native_s": n / CPU["native"],
        "cpu_python_s": n / CPU["python"],
    }


def initial_load(n):
    """First commit: nothing to compare; a sorted file is written straight to the bottom level."""
    b = size(n)
    puts = max(1, math.ceil(b / RANGE)) + ATTEMPT_PUT + JOURNAL_PUT
    return {
        "puts": puts,
        "usd": puts * PUT + ATTEMPT_GET * GET,
        "io_s": b / BW,
        "sort_native_s": n * math.log2(max(n, 2)) / (CPU["native"] * 4),
        "sort_python_s": n * math.log2(max(n, 2)) / (CPU["python"] * 4),
    }


def full_pass(n, batch):
    """A consumer re-reads everything in batches of `batch` keys, one attempt per batch (cold)."""
    batches = math.ceil(n / batch)
    # A batch covers a narrow key range: per level, one or two files overlap it (their footer + block
    # index, then the blocks covering the batch); the small level-0 files are read whole.
    per_level = 2 + max(1, math.ceil(batch * E_ZIP / RANGE))  # consecutive blocks: one range read
    per_batch_gets = (
        2 * per_level + L0_MAX / 2 if size(n) > WHOLE else math.ceil(size(n) / RANGE) + L0_MAX / 2
    )
    gets = batches * (per_batch_gets + ATTEMPT_GET)
    puts = batches * (ATTEMPT_PUT + JOURNAL_PUT)
    return {"batches": batches, "gets": gets, "puts": puts, "usd": gets * GET + puts * PUT}


def storage(n):
    return (size(n) * 1.1 + n * FILTER_B) / 1e9 * GB_MONTH
