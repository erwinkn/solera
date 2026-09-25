"""Time-sortable unique ids (ULID): 48-bit millisecond timestamp + 80 random bits,
Crockford base32, 26 characters. Lexicographic order is creation order."""

from __future__ import annotations

import os
import time

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def ulid(now: float | None = None) -> str:
    ms = int((time.time() if now is None else now) * 1000) & ((1 << 48) - 1)
    n = (ms << 80) | int.from_bytes(os.urandom(10), "big")
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
