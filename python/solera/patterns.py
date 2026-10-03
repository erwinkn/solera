"""Key patterns on inputs (docs/per-key-processing.md §11): which keys of a
keyed upstream an `Incremental` or `Each` input takes.

    Each("sharepoint_files", include="ICP/Results/**/*.csv",
         exclude={"archive": "**/archive/**"})

Globs over the key string: `**` crosses `/` (and `**/` matches no directory
too), `*` and `?` do not; `Regex("…")` for the rest. A key is taken when it
matches an `include` (any key, without one) and no `exclude`. Exclusions are
named, so that what left a key out can be told on demand.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Regex:
    """A pattern as a regular expression, matched against the whole key."""

    pattern: str

    def __post_init__(self):
        re.compile(self.pattern)


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


def _compiled(p: dict) -> re.Pattern:
    return re.compile(p["regex"] if "regex" in p else glob_regex(p["glob"]), re.DOTALL)


class Matcher:
    """Whether an input takes a key, from its manifest spec (`None`: every key)."""

    def __init__(self, patterns: dict | None):
        patterns = patterns or {}
        self.include = [_compiled(p) for p in patterns.get("include") or ()]
        self.exclude = [(name, _compiled(p)) for name, p in patterns.get("exclude") or ()]

    def included(self, key: str) -> bool:
        return not self.include or any(p.fullmatch(key) for p in self.include)

    def excluded_by(self, key: str) -> str | None:
        return next((name for name, p in self.exclude if p.fullmatch(key)), None)

    def __call__(self, key: str) -> bool:
        return self.included(key) and self.excluded_by(key) is None
