//! Key patterns: the globs of an input's `include` and `exclude`, with the
//! meaning `solera.patterns.glob_regex` gives them. `**/` matches any number
//! of leading directories (none included), `**` anything, `*` anything but
//! `/`, `?` one character but `/`; everything else matches itself. A key
//! is taken when it matches an include (any key, without one) and no exclude.
//! Each glob compiles to the same regular expression as in Python, matched
//! in linear time.

use crate::error::{Error, Result};
use regex::bytes::Regex;
use std::ops::Bound;

#[derive(Clone, Debug)]
pub struct Glob {
    re: Regex,
    prefix: Vec<u8>,
}

impl PartialEq for Glob {
    fn eq(&self, o: &Glob) -> bool {
        self.re.as_str() == o.re.as_str()
    }
}

impl Eq for Glob {}

impl Glob {
    pub fn new(glob: &str) -> Result<Glob> {
        let mut re = String::from("^(?:");
        let mut prefix = None;
        let mut rest = glob;
        while let Some(c) = rest.chars().next() {
            let (part, n) = if rest.starts_with("**/") {
                ("(?:.*/)?", 3)
            } else if rest.starts_with("**") {
                (".*", 2)
            } else if c == '*' {
                ("[^/]*", 1)
            } else if c == '?' {
                ("[^/]", 1)
            } else {
                re.push_str(&regex::escape(&rest[..c.len_utf8()]));
                rest = &rest[c.len_utf8()..];
                continue;
            };
            prefix.get_or_insert(glob.len() - rest.len());
            re.push_str(part);
            rest = &rest[n..];
        }
        re.push_str(")$");
        let re = Regex::new(&re).map_err(|e| Error::Invalid(format!("glob {glob:?}: {e}")))?;
        let prefix = glob.as_bytes()[..prefix.unwrap_or(glob.len())].to_vec();
        Ok(Glob { re, prefix })
    }

    pub fn matches(&self, key: &[u8]) -> bool {
        self.re.is_match(key)
    }

    /// The literal text every match starts with.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }
}

/// An input's patterns: includes (none: every key) and excludes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pattern {
    pub include: Vec<Glob>,
    pub exclude: Vec<Glob>,
}

impl Pattern {
    pub fn new<S: AsRef<str>>(include: &[S], exclude: &[S]) -> Result<Pattern> {
        let globs = |gs: &[S]| {
            gs.iter()
                .map(|g| Glob::new(g.as_ref()))
                .collect::<Result<_>>()
        };
        Ok(Pattern {
            include: globs(include)?,
            exclude: globs(exclude)?,
        })
    }

    #[inline]
    pub fn matches(&self, key: &[u8]) -> bool {
        (self.include.is_empty() || self.include.iter().any(|g| g.matches(key)))
            && !self.exclude.iter().any(|g| g.matches(key))
    }

    /// Bounds every match lies in: the longest prefix all includes share.
    pub fn bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        let Some(first) = self.include.first() else {
            return (Bound::Unbounded, Bound::Unbounded);
        };
        let mut p = first.prefix();
        for g in &self.include[1..] {
            let q = g.prefix();
            let n = p.iter().zip(q).take_while(|(a, b)| a == b).count();
            p = &p[..n];
        }
        if p.is_empty() {
            return (Bound::Unbounded, Bound::Unbounded);
        }
        let mut hi = p.to_vec();
        while hi.last() == Some(&0xff) {
            hi.pop();
        }
        let hi = match hi.last_mut() {
            Some(b) => {
                *b += 1;
                Bound::Excluded(hi)
            }
            None => Bound::Unbounded,
        };
        (Bound::Included(p.to_vec()), hi)
    }
}
