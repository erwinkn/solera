"""The engine-owned key index (docs/object-store-state.md §6).

The per-key work — encoding, decoding, filter checks, sorting, merging — is
the `solera._native` extension, following docs/key-index-format.md; Python
chooses files, fetches bytes, and parses tails through it. The format's
executable reference, which the tests hold the extension to, is
`tests/sdk/keys_reference.py`.
"""

from __future__ import annotations

from .._native import (
    CODEC_NONE,
    CODEC_ZLIB,
    FOOTER_SIZE,
    FormatError,
    LimitError,
    LocalError,
    Merge,
    Rows,
    SortedEntries,
    bloom_check_keys,
    check_block,
    decode_block,
    encode_file,
    filter_nbits,
    lookup,
    merge_page,
    merge_range,
    parse_footer,
    parse_index,
    parse_tail,
    write_files,
)

__all__ = [
    "CODEC_NONE",
    "CODEC_ZLIB",
    "FOOTER_SIZE",
    "FormatError",
    "Merge",
    "LimitError",
    "LocalError",
    "Rows",
    "SortedEntries",
    "bloom_check_keys",
    "check_block",
    "decode_block",
    "encode_file",
    "filter_nbits",
    "lookup",
    "merge_page",
    "merge_range",
    "parse_footer",
    "parse_index",
    "parse_tail",
    "write_files",
]
