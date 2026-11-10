"""Key patterns on inputs (docs/per-key-processing.md §11): which keys of a
keyed upstream an incremental input takes.

    Incremental("sharepoint_files", include="ICP/Results/**/*.csv",
                exclude={"archive": "**/archive/**"}, each=True)

Globs over the key string: `**` crosses `/` (and `**/` matches no directory
too), `*` and `?` do not; `Regex("…")` for the rest, in the Rust `regex`
dialect (see `Regex`). A key is taken when it matches an `include` (any key,
without one) and no `exclude`. Exclusions are named, so that what left a key
out can be told on demand. Matching is native (`Matcher`), the same matcher the
planner uses.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from solera._native import KeyMatcher, regex_check


@dataclass(frozen=True)
class Regex:
    """A pattern as a regular expression, matched against the whole key.

    The dialect is Rust's `regex` crate (https://docs.rs/regex), not Python's
    `re`: matching takes time linear in the key, so lookaround (`(?=…)`,
    `(?!…)`, `(?<=…)`, `(?<!…)`) and backreferences (`\\1`) are not
    supported, and a pattern using them is refused here, when the input is
    declared. The pattern must match the whole key (as `re.fullmatch`), and
    `.` matches a newline too.
    """

    pattern: str

    def __post_init__(self):
        regex_check(self.pattern)


def glob_regex(glob: str) -> str:
    out, i = [], 0
    while i < len(glob):
        if glob.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif glob.startswith("**", i):
            out.append(".*")
            i += 2
        elif glob[i] == "*":
            out.append("[^/]*")
            i += 1
        elif glob[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(glob[i]))
            i += 1
    return "".join(out)


def _one(pattern) -> dict:
    if isinstance(pattern, Regex):
        return {"regex": pattern.pattern}
    if isinstance(pattern, str) and pattern:
        return {"glob": pattern}
    raise ValueError(f"a key pattern is a non-empty glob string or Regex(...), got {pattern!r}")


def spec(include=None, exclude=None) -> dict | None:
    """The manifest form: `{"include": [p, …], "exclude": [[name, p], …]}`,
    each `p` `{"glob": …}` or `{"regex": …}`; `None` without patterns."""

    if include is None and not exclude:
        return None
    includes = [] if include is None else [include] if isinstance(include, str | Regex) else list(include)
    if include is not None and not includes:
        raise ValueError("include= names no pattern")
    if isinstance(exclude, Mapping):
        excludes = [[str(name), _one(p)] for name, p in exclude.items()]
    else:  # one pattern, as for include=: a string is never its characters
        listed = [exclude] if isinstance(exclude, str | Regex) else list(exclude or ())
        excludes = [[f"exclude[{n}]", _one(p)] for n, p in enumerate(listed)]
    return {"include": [_one(p) for p in includes], "exclude": excludes}


Matcher = KeyMatcher
"""Whether an input takes a key, from its manifest spec (`None`: every key):
`included(key)`, `excluded_by(key)` (the first matching exclusion's name) and
`m(key)`, natively, with the planner's matcher."""
