"""The engine-owned key index (docs/object-store-state.md §6,
docs/key-index-design.md): stamped layers (`layers`), the engine's cache of
them (`layer_cache`), Δ(P, H) over them (`delta`), and the engine's resolver
(`resolver`).

The per-key work — encoding, decoding, merging, joins, scans, lookups,
sorting written keys — is the `solera._native` extension, following
docs/key-index-format.md; Python chooses files, fetches bytes and keeps the
indexes.
"""

from __future__ import annotations

from .._native import FormatError, LimitError, Rows, SortedEntries

__all__ = ["FormatError", "LimitError", "Rows", "SortedEntries"]
