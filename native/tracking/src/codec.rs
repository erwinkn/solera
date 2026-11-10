//! The byte encoding of events and snapshots: unsigned LEB128 varints,
//! length-prefixed byte strings, and a tag byte before each sum type. Fields
//! are read in the order they are written; a reader refuses trailing bytes.

use solera_index::{LayerRef, Version};

use crate::error::{format, Result};

/// A version owned: absent, an integer (an attempt's generation) or bytes (a
/// source's version word). Compared only for equality.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub enum Ver {
    #[default]
    Absent,
    Int(u64),
    Bytes(Box<[u8]>),
}

impl Ver {
    pub fn of(v: Version<'_>) -> Ver {
        match v {
            Version::Absent => Ver::Absent,
            Version::Int(n) => Ver::Int(n),
            Version::Bytes(b) => Ver::Bytes(b.into()),
        }
    }

    pub fn as_version(&self) -> Version<'_> {
        match self {
            Ver::Absent => Version::Absent,
            Ver::Int(n) => Version::Int(*n),
            Ver::Bytes(b) => Version::Bytes(b),
        }
    }

    pub fn is_absent(&self) -> bool {
        matches!(self, Ver::Absent)
    }
}

#[derive(Default)]
pub struct Enc {
    pub buf: Vec<u8>,
}

impl Enc {
    pub fn new() -> Enc {
        Enc::default()
    }

    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn uv(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    pub fn bytes(&mut self, b: &[u8]) {
        self.uv(b.len() as u64);
        self.buf.extend_from_slice(b);
    }

    pub fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                self.uv(v);
            }
        }
    }

    pub fn version(&mut self, v: Version<'_>) {
        match v {
            Version::Absent => self.u8(0),
            Version::Int(n) => {
                self.u8(1);
                self.uv(n);
            }
            Version::Bytes(b) => {
                self.u8(2);
                self.bytes(b);
            }
        }
    }

    pub fn layer(&mut self, l: &LayerRef) {
        self.bytes(l.path.as_bytes());
        for v in [l.size, l.index_start, l.top_start, l.lo, l.hi] {
            self.uv(v);
        }
        self.opt_u64(l.floor);
        for v in [l.entries, l.keys] {
            self.uv(v);
        }
        self.bytes(&l.first_key);
        self.bytes(&l.last_key);
        self.uv(l.flags);
    }
}

pub struct Dec<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Dec<'a> {
    pub fn new(buf: &'a [u8]) -> Dec<'a> {
        Dec { buf, pos: 0 }
    }

    pub fn done(&self) -> bool {
        self.pos == self.buf.len()
    }

    pub fn end(&self) -> Result<()> {
        if self.done() {
            Ok(())
        } else {
            format(format!("{} trailing bytes", self.buf.len() - self.pos))
        }
    }

    pub fn u8(&mut self) -> Result<u8> {
        match self.buf.get(self.pos) {
            Some(&b) => {
                self.pos += 1;
                Ok(b)
            }
            None => format("truncated"),
        }
    }

    pub fn uv(&mut self) -> Result<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.u8()?;
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        format("varint too long")
    }

    pub fn len(&mut self) -> Result<usize> {
        let n = self.uv()?;
        if n > (self.buf.len() - self.pos) as u64 {
            return format("length past the end");
        }
        Ok(n as usize)
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.len()?;
        let b = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(b)
    }

    /// A count of items that follow, each at least one byte long.
    pub fn count(&mut self) -> Result<usize> {
        self.len()
    }

    pub fn string(&mut self) -> Result<String> {
        match std::str::from_utf8(self.bytes()?) {
            Ok(s) => Ok(s.to_owned()),
            Err(_) => format("not utf-8"),
        }
    }

    pub fn opt_u64(&mut self) -> Result<Option<u64>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.uv()?)),
            t => format(format!("option tag {t}")),
        }
    }

    pub fn version(&mut self) -> Result<Version<'a>> {
        match self.u8()? {
            0 => Ok(Version::Absent),
            1 => Ok(Version::Int(self.uv()?)),
            2 => Ok(Version::Bytes(self.bytes()?)),
            t => format(format!("version tag {t}")),
        }
    }

    pub fn ver(&mut self) -> Result<Ver> {
        self.version().map(Ver::of)
    }

    pub fn layer(&mut self) -> Result<LayerRef> {
        Ok(LayerRef {
            path: self.string()?,
            size: self.uv()?,
            index_start: self.uv()?,
            top_start: self.uv()?,
            lo: self.uv()?,
            hi: self.uv()?,
            floor: self.opt_u64()?,
            entries: self.uv()?,
            keys: self.uv()?,
            first_key: self.bytes()?.to_vec(),
            last_key: self.bytes()?.to_vec(),
            flags: self.uv()?,
        })
    }
}
