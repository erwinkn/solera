"""The engine-owned key index (docs/object-store-state.md §6).

`.kx` files are encoded and decoded by the `solera._native` extension,
following docs/key-index-format.md. Tails are parsed in Python (they are
small); the per-entry work — encoding, decoding, filter checks, sorting,
merging — is native. `_python` is the format's executable reference, which
the tests hold the extension to.
"""

from __future__ import annotations

from .. import _native as _impl
from ._python import (
    CODEC_NONE,
    CODEC_ZLIB,
    FOOTER_SIZE,
    FormatError,
    check_block,
    filter_nbits,
    parse_footer,
    parse_index,
    parse_tail,
)

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
    "parse_index",
    "parse_tail",
    "replace_diff",
    "sort_entries",
]
