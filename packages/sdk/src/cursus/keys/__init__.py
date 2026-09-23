"""The engine-owned key index (docs/object-store-state.md §6).

`.kx` files are encoded and decoded by the `cursus_native` extension when it
is installed, else by the pure-Python reference in `_python`; both follow
docs/key-index-format.md. Set `CURSUS_KEYS_IMPL=python` to force the
reference. Tails are always parsed in Python (they are small); the
per-entry work — encoding, decoding, filter checks, sorting, merging — is
what the native extension accelerates.
"""

from __future__ import annotations

import os

from . import _python
from ._python import (
    CODEC_NONE,
    CODEC_ZLIB,
    FOOTER_SIZE,
    FormatError,
    check_block,
    filter_nbits,
    parse_footer,
    parse_tail,
)


def _load_native():
    if os.environ.get("CURSUS_KEYS_IMPL", "").lower() == "python":
        return None
    try:
        import cursus_native
    except ImportError:
        return None
    return cursus_native


_native = _load_native()
IMPL = "native" if _native is not None else "python"
_impl = _native or _python

encode_file = _impl.encode_file
decode_block = _impl.decode_block
bloom_check_keys = _impl.bloom_check_keys
bloom_check_pairs = _impl.bloom_check_pairs
bloom_check_tombstones = _impl.bloom_check_tombstones
sort_entries = _impl.sort_entries
merge_files = _impl.merge_files
lookup = _impl.lookup
merge_range = _impl.merge_range
replace_diff = _impl.replace_diff

__all__ = [
    "CODEC_NONE",
    "CODEC_ZLIB",
    "FOOTER_SIZE",
    "IMPL",
    "FormatError",
    "bloom_check_keys",
    "bloom_check_pairs",
    "bloom_check_tombstones",
    "check_block",
    "decode_block",
    "encode_file",
    "filter_nbits",
    "lookup",
    "merge_files",
    "merge_range",
    "parse_footer",
    "parse_tail",
    "replace_diff",
    "sort_entries",
]
