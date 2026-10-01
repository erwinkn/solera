//! The canonical row digest, version 1 (docs/row-digest.md).
//!
//! Walkers over Python values (`lib.rs`) and Arrow arrays (`arrow.rs`) turn
//! leaves into `Scalar`s and containers into `list`/`record`/`map` framing;
//! this module owns the bytes, the digests and the revision text, so both
//! sides agree by construction.

use std::fmt::Write as _;

use xxhash_rust::xxh3::xxh3_128;

use crate::format::{put_varint, Error, Result};

/// The grammar version: the first byte of everything digested.
pub const VERSION: u8 = 1;

pub type Digest = [u8; 16];

fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Value(msg.into()))
}

/// A leaf value, whatever representation it came in.
pub enum Scalar<'a> {
    Null,
    Bool(bool),
    Int(i128),
    /// An integer beyond i128, as canonical decimal text.
    BigInt(String),
    Float(f64),
    /// `unscaled · 10^-scale`, `unscaled` as decimal text with an optional `-`.
    Decimal(String, i64),
    Str(&'a [u8]),
    Bytes(&'a [u8]),
    /// Days since 1970-01-01.
    Date(i64),
    /// Nanoseconds since midnight.
    Time(i64),
    /// Nanoseconds since 1970-01-01T00:00; `true` for an instant (UTC), `false` for wall clock.
    Timestamp(i128, bool),
    Duration(i128),
    Interval(i32, i32, i64),
}

fn put_len(out: &mut Vec<u8>, b: &[u8]) {
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// A decimal normalized: trailing zeros of the unscaled value moved into the
/// exponent; zero is `0`, exponent 0. Returns (sign and digits, exponent).
fn normalize(unscaled: &str, scale: i64) -> (String, i64) {
    let (neg, digits) = match unscaled.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, unscaled),
    };
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return ("0".into(), 0);
    }
    let kept = digits.trim_end_matches('0');
    let exp = (digits.len() - kept.len()) as i64 - scale;
    (format!("{}{kept}", if neg { "-" } else { "" }), exp)
}

fn float_bits(f: f64) -> u64 {
    if f.is_nan() {
        0x7FF8_0000_0000_0000
    } else if f == 0.0 {
        0
    } else {
        f.to_bits()
    }
}

impl Scalar<'_> {
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Scalar::Null => out.push(b'n'),
            Scalar::Bool(b) => out.extend_from_slice(&[b'o', *b as u8]),
            Scalar::Int(i) => {
                out.push(b'i');
                put_len(out, i.to_string().as_bytes());
            }
            Scalar::BigInt(s) => {
                out.push(b'i');
                put_len(out, s.as_bytes());
            }
            Scalar::Float(f) => {
                out.push(b'f');
                out.extend_from_slice(&float_bits(*f).to_le_bytes());
            }
            Scalar::Decimal(unscaled, scale) => {
                let (digits, exp) = normalize(unscaled, *scale);
                out.push(b'e');
                put_len(out, digits.as_bytes());
                put_varint(out, ((exp << 1) ^ (exp >> 63)) as u64);
            }
            Scalar::Str(s) => {
                out.push(b's');
                put_len(out, s);
            }
            Scalar::Bytes(b) => {
                out.push(b'b');
                put_len(out, b);
            }
            Scalar::Date(d) => {
                out.push(b'D');
                out.extend_from_slice(&d.to_le_bytes());
            }
            Scalar::Time(ns) => {
                out.push(b'h');
                out.extend_from_slice(&ns.to_le_bytes());
            }
            Scalar::Timestamp(ns, instant) => {
                out.push(if *instant { b'T' } else { b't' });
                out.extend_from_slice(&ns.to_le_bytes());
            }
            Scalar::Duration(ns) => {
                out.push(b'u');
                out.extend_from_slice(&ns.to_le_bytes());
            }
            Scalar::Interval(months, days, ns) => {
                out.push(b'v');
                out.extend_from_slice(&months.to_le_bytes());
                out.extend_from_slice(&days.to_le_bytes());
                out.extend_from_slice(&ns.to_le_bytes());
            }
        }
    }

    /// A declared revision's text (docs/row-digest.md § Revisions).
    pub fn render(&self, out: &mut Vec<u8>) -> Result<()> {
        let mut s = String::new();
        match self {
            Scalar::Null => return err("a revision cannot be null"),
            Scalar::Str(b) | Scalar::Bytes(b) => {
                out.extend_from_slice(b);
                return Ok(());
            }
            Scalar::Bool(b) => s.push_str(if *b { "true" } else { "false" }),
            Scalar::Int(i) => write!(s, "{i}").unwrap(),
            Scalar::BigInt(t) => s.push_str(t),
            Scalar::Float(f) => {
                let f = if *f == 0.0 { 0.0 } else { *f };
                write!(s, "{f:?}").unwrap()
            }
            Scalar::Decimal(unscaled, scale) => {
                let (digits, exp) = normalize(unscaled, *scale);
                let (neg, digits) = match digits.strip_prefix('-') {
                    Some(d) => ("-", d.to_string()),
                    None => ("", digits),
                };
                s.push_str(neg);
                if exp >= 0 {
                    s.push_str(&digits);
                    s.extend(std::iter::repeat_n('0', exp as usize));
                } else {
                    let point = (-exp) as usize;
                    let padded = format!("{digits:0>width$}", width = point + 1);
                    let (int, frac) = padded.split_at(padded.len() - point);
                    write!(s, "{int}.{frac}").unwrap();
                }
            }
            Scalar::Date(d) => date(&mut s, *d),
            Scalar::Time(ns) => clock(&mut s, *ns as i128),
            Scalar::Timestamp(ns, instant) => {
                let day = 86_400_000_000_000i128;
                date(&mut s, ns.div_euclid(day) as i64);
                s.push('T');
                clock(&mut s, ns.rem_euclid(day));
                if *instant {
                    s.push('Z');
                }
            }
            Scalar::Duration(ns) => write!(s, "{ns}ns").unwrap(),
            Scalar::Interval(m, d, ns) => write!(s, "{m}m{d}d{ns}ns").unwrap(),
        }
        out.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

/// `YYYY-MM-DD` of days since 1970-01-01 (proleptic Gregorian).
fn date(s: &mut String, days: i64) {
    let (y, m, d) = civil(days);
    write!(s, "{y:04}-{m:02}-{d:02}").unwrap();
}

/// `HH:MM:SS[.fffffffff]`, trailing zeros of the fraction dropped.
fn clock(s: &mut String, ns: i128) {
    let secs = ns / 1_000_000_000;
    let frac = ns % 1_000_000_000;
    write!(
        s,
        "{:02}:{:02}:{:02}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
    .unwrap();
    if frac != 0 {
        let f = format!("{frac:09}");
        write!(s, ".{}", f.trim_end_matches('0')).unwrap();
    }
}

/// Year, month, day of days since 1970-01-01 (Howard Hinnant's `civil_from_days`).
pub fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + (m <= 2) as i64, m, d)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (`days_from_civil`).
pub fn days(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

// -- containers -------------------------------------------------------------------------

pub fn list(out: &mut Vec<u8>, n: usize) {
    out.push(b'l');
    put_varint(out, n as u64);
}

/// The fields of a record, or entries of a map, gathered then written
/// sorted: each is its name (or encoded key) and its encoded value.
#[derive(Default)]
pub struct Entries {
    data: Vec<u8>,
    spans: Vec<(usize, usize, usize)>, // name start, value start, end
}

impl Entries {
    pub fn clear(&mut self) {
        self.data.clear();
        self.spans.clear();
    }

    /// Starts an entry named `name` (a record field) — then encode its value into `buf()`.
    pub fn name(&mut self, name: &[u8]) -> &mut Vec<u8> {
        let start = self.data.len();
        self.data.extend_from_slice(name);
        self.spans.push((start, self.data.len(), 0));
        &mut self.data
    }

    /// Ends the entry started last; a null value is dropped.
    pub fn end(&mut self) {
        let last = self.spans.last_mut().unwrap();
        if &self.data[last.1..] == b"n" {
            self.data.truncate(last.0);
            self.spans.pop();
        } else {
            last.2 = self.data.len();
        }
    }

    /// Writes the entries as a record (`r`, names length-prefixed) or a map
    /// (`m`, names already encoded keys), sorted; a repeated name is an error.
    pub fn write(&mut self, out: &mut Vec<u8>, record: bool) -> Result<()> {
        let data = &self.data;
        self.spans
            .sort_unstable_by(|a, b| data[a.0..a.1].cmp(&data[b.0..b.1]));
        for w in self.spans.windows(2) {
            if data[w[0].0..w[0].1] == data[w[1].0..w[1].1] {
                return err(format!(
                    "{} {:?} appears twice",
                    if record { "field" } else { "map key" },
                    String::from_utf8_lossy(&data[w[0].0..w[0].1])
                ));
            }
        }
        out.push(if record { b'r' } else { b'm' });
        put_varint(out, self.spans.len() as u64);
        for &(n, v, e) in &self.spans {
            if record {
                put_len(out, &data[n..v]);
            } else {
                out.extend_from_slice(&data[n..v]);
            }
            out.extend_from_slice(&data[v..e]);
        }
        Ok(())
    }
}

// -- digests ----------------------------------------------------------------------------

fn digest(production: u8, body: &[&[u8]]) -> Digest {
    let mut buf = Vec::with_capacity(2 + body.iter().map(|b| b.len()).sum::<usize>());
    buf.push(VERSION);
    buf.push(production);
    for b in body {
        buf.extend_from_slice(b);
    }
    xxh3_128(&buf).to_le_bytes()
}

/// `row(r)` of an encoded record.
pub fn row(record: &[u8]) -> Digest {
    digest(b'R', &[record])
}

/// `group(rows)` of row digests, in any order.
pub fn group(rows: &mut [Digest]) -> Digest {
    rows.sort_unstable();
    let mut n = Vec::new();
    put_varint(&mut n, rows.len() as u64);
    digest(b'G', &[&n, rows.as_flattened()])
}

/// `value(v)` of an encoded value.
pub fn value(enc: &[u8]) -> Digest {
    digest(b'V', &[enc])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(s: Scalar) -> Vec<u8> {
        let mut out = Vec::new();
        s.encode(&mut out);
        out
    }

    fn text(s: Scalar) -> String {
        let mut out = Vec::new();
        s.render(&mut out).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn scalars() {
        assert_eq!(enc(Scalar::Int(-12)), b"i\x03-12");
        assert_eq!(
            enc(Scalar::Decimal("120".into(), 2)),
            enc(Scalar::Decimal("12".into(), 1))
        );
        assert_eq!(enc(Scalar::Decimal("-000".into(), 5)), b"e\x010\x00");
        assert_eq!(enc(Scalar::Float(-0.0)), enc(Scalar::Float(0.0)));
        assert_eq!(enc(Scalar::Float(f64::NAN)), enc(Scalar::Float(-f64::NAN)));
        assert_ne!(
            enc(Scalar::Timestamp(0, true)),
            enc(Scalar::Timestamp(0, false))
        );
        assert_eq!(text(Scalar::Decimal("120".into(), 2)), "1.2");
        assert_eq!(text(Scalar::Decimal("-5".into(), 3)), "-0.005");
        assert_eq!(text(Scalar::Decimal("12".into(), -2)), "1200");
        assert_eq!(text(Scalar::Float(1.0)), "1.0");
        assert_eq!(text(Scalar::Bool(true)), "true");
        assert_eq!(text(Scalar::Date(days(2024, 2, 29))), "2024-02-29");
        assert_eq!(
            text(Scalar::Timestamp(
                86_400_000_000_000 * 3 + 1_500_000_000,
                true
            )),
            "1970-01-04T00:00:01.5Z"
        );
        assert_eq!(
            text(Scalar::Timestamp(-1, false)),
            "1969-12-31T23:59:59.999999999"
        );
        for d in [-800_000, -1, 0, 59, 60, 19_000, 2_932_896] {
            let (y, m, dd) = civil(d);
            assert_eq!(days(y, m, dd), d);
        }
    }

    #[test]
    fn records_sort_and_drop_nulls() {
        let mut e = Entries::default();
        for (name, v) in [
            ("b", Scalar::Int(1)),
            ("a", Scalar::Null),
            ("a2", Scalar::Bool(true)),
        ] {
            v.encode(e.name(name.as_bytes()));
            e.end();
        }
        let mut out = Vec::new();
        e.write(&mut out, true).unwrap();
        assert_eq!(out, b"r\x02\x02a2o\x01\x01bi\x011");
        let mut e = Entries::default();
        for name in ["x", "x"] {
            Scalar::Int(1).encode(e.name(name.as_bytes()));
            e.end();
        }
        assert!(e.write(&mut Vec::new(), true).is_err());
    }

    #[test]
    fn groups_are_multisets() {
        let (a, b) = (row(b"r\x00"), row(b"r\x01\x01xi\x011"));
        assert_eq!(group(&mut [a, b]), group(&mut [b, a]));
        assert_ne!(group(&mut [a, b]), group(&mut [a, b, b]));
    }
}
