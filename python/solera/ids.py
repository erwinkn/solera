"""Time-sortable unique ids (ULID): 48-bit millisecond timestamp + 80 random bits,
Crockford base32, 26 characters. Lexicographic order is creation order — within
a millisecond too: a process's next id in the same millisecond is its last plus
one, so two runs submitted at once list in the order they were made."""

from __future__ import annotations

import os
import threading
import time

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_last = [-1, 0]  # this process's last id: its millisecond, its random part
_lock = threading.Lock()


def ulid(now: float | None = None) -> str:
    ms = int((time.time() if now is None else now) * 1000) & ((1 << 48) - 1)
    with _lock:
        random = _last[1] + 1 if ms == _last[0] else int.from_bytes(os.urandom(10), "big")
        if random >> 80:  # the millisecond's ids ran out: a fresh draw
            random = int.from_bytes(os.urandom(10), "big")
        _last[:] = [ms, random]
    n = (ms << 80) | random
    out = []
    for _ in range(26):
        out.append(_ALPHABET[n & 31])
        n >>= 5
    return "".join(reversed(out))


def ulid_time(value: str) -> float:
    """The creation time (seconds) embedded in a ULID."""

    n = 0
    for ch in value[:10]:
        n = (n << 5) | _ALPHABET.index(ch)
    return n / 1000
