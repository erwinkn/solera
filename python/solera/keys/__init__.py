"""The engine-owned key index (docs/object-store-state.md §6).

The per-key work — encoding, decoding, filter checks, sorting, merging — is
the `solera._native` extension, following docs/key-index-format.md; Python
chooses files, fetches bytes, and parses tails through it. `_python` is the
format's executable reference, which the tests hold the extension to.
"""

from __future__ import annotations

from .._native import (
    CODEC_NONE,
    CODEC_ZLIB,
    FOOTER_SIZE,
    FormatError,
    Job,
    LimitError,
    LocalError,
    Rows,
    SortedRun,
    bloom_check_keys,
    bloom_check_pairs,
    bloom_check_tombstones,
    check_block,
    decode_block,
    decode_garbage,
    encode_file,
    filter_nbits,
    lookup,
    merge_range,
    parse_footer,
    parse_index,
    parse_tail,
    sort_entries,
    write_files,
)

__all__ = [
    "CODEC_NONE",
    "CODEC_ZLIB",
    "FOOTER_SIZE",
    "FormatError",
    "Job",
    "LimitError",
    "LocalError",
    "Rows",
    "SortedRun",
    "bloom_check_keys",
    "bloom_check_pairs",
    "bloom_check_tombstones",
    "check_block",
    "decode_block",
    "decode_garbage",
    "encode_file",
    "filter_nbits",
    "lookup",
    "merge_range",
    "parse_footer",
    "parse_index",
    "parse_tail",
    "sort_entries",
    "write_files",
]
