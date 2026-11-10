//! Key patterns: the globs of an input's `include` and `exclude`, with the
//! meaning `solera.patterns.glob_regex` gives them. `**/` matches any number
//! of leading directories (none included), `**` anything, `*` anything but
//! `/`, `?` one character but `/`; everything else matches itself. A key
//! is taken when it matches an include (any key, without one) and no exclude.

use std::ops::Bound;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Tok {
    Lit(Vec<u8>),
    One,
    Star,
    Any,
    AnyDir,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Glob {
    toks: Vec<Tok>,
}

impl Glob {
    pub fn new(glob: &str) -> Glob {
        let g = glob.as_bytes();
        let mut toks: Vec<Tok> = Vec::new();
        let mut i = 0;
        while i < g.len() {
            let t = if g[i..].starts_with(b"**/") {
                i += 3;
                Tok::AnyDir
            } else if g[i..].starts_with(b"**") {
                i += 2;
                Tok::Any
            } else if g[i] == b'*' {
                i += 1;
                Tok::Star
            } else if g[i] == b'?' {
                i += 1;
                Tok::One
            } else {
                if let Some(Tok::Lit(l)) = toks.last_mut() {
                    l.push(g[i]);
                } else {
                    toks.push(Tok::Lit(vec![g[i]]));
                }
                i += 1;
                continue;
            };
            toks.push(t);
        }
        Glob { toks }
    }

    pub fn matches(&self, key: &[u8]) -> bool {
        matches(&self.toks, key)
    }

    /// The literal text every match starts with.
    pub fn prefix(&self) -> &[u8] {
        match self.toks.first() {
            Some(Tok::Lit(l)) => l,
            _ => &[],
        }
    }
}

fn char_len(b: u8) -> usize {
    match b {
        0xf0..=0xff => 4,
        0xe0..=0xef => 3,
        0xc0..=0xdf => 2,
        _ => 1,
    }
}

fn matches(toks: &[Tok], s: &[u8]) -> bool {
    let Some(t) = toks.first() else {
        return s.is_empty();
    };
    let rest = &toks[1..];
    match t {
        Tok::Lit(l) => s.starts_with(l) && matches(rest, &s[l.len()..]),
        Tok::One => {
            !s.is_empty() && s[0] != b'/' && matches(rest, &s[char_len(s[0]).min(s.len())..])
        }
        Tok::Star => {
            for i in 0..=s.len() {
                if matches(rest, &s[i..]) {
                    return true;
                }
                if i < s.len() && s[i] == b'/' {
                    return false;
                }
            }
            false
        }
        Tok::Any => {
            if rest.is_empty() {
                return true;
            }
            (0..=s.len()).any(|i| matches(rest, &s[i..]))
        }
        Tok::AnyDir => {
            matches(rest, s)
                || s.iter()
                    .enumerate()
                    .any(|(i, &c)| c == b'/' && matches(rest, &s[i + 1..]))
        }
    }
}

/// An input's patterns: includes (none: every key) and excludes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pattern {
    pub include: Vec<Glob>,
    pub exclude: Vec<Glob>,
}

impl Pattern {
    pub fn new<S: AsRef<str>>(include: &[S], exclude: &[S]) -> Pattern {
        Pattern {
            include: include.iter().map(|g| Glob::new(g.as_ref())).collect(),
            exclude: exclude.iter().map(|g| Glob::new(g.as_ref())).collect(),
        }
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
