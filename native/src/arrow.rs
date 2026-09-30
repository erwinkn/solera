//! Written content as Arrow record batches, read in place: the key column,
//! and either a declared revision column or a digest of each row.
//!
//! A row digest is XXH3-128 over the row's columns in name order, each as its
//! name (varint length, bytes) and its value's canonical encoding (`value`):
//! values that are equal logically hash the same whatever their physical
//! type — any string layout, any integer width, any timestamp unit.

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use rayon::prelude::*;
use xxhash_rust::xxh3::Xxh3;

use crate::format::{put_varint, Error, Result};
use crate::rows::{Arena, Versions};
use crate::sort::Keys;

fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Value(msg.into()))
}

/// Row `i` of a column split into chunks: which chunk, and where in it.
struct Chunks {
    starts: Vec<usize>,
    lookup: Vec<u32>, // the chunk holding row `j << 12`, for every j
}

impl Chunks {
    fn new(lens: impl Iterator<Item = usize>) -> Chunks {
        let mut starts = vec![0];
        for n in lens {
            starts.push(starts.last().unwrap() + n);
        }
        let n = *starts.last().unwrap();
        let mut lookup = Vec::with_capacity((n >> 12) + 1);
        let mut c = 0u32;
        for j in 0..=(n >> 12) {
            while starts[c as usize + 1] <= j << 12 && (c as usize) + 2 < starts.len() {
                c += 1;
            }
            lookup.push(c);
        }
        Chunks { starts, lookup }
    }

    fn len(&self) -> usize {
        *self.starts.last().unwrap()
    }

    #[inline]
    fn locate(&self, i: usize) -> (usize, usize) {
        let mut c = self.lookup[i >> 12] as usize;
        while self.starts[c + 1] <= i {
            c += 1;
        }
        (c, i - self.starts[c])
    }
}

// -- keys -----------------------------------------------------------------------------

enum Bytes {
    Utf8(arrow_array::StringArray),
    LargeUtf8(arrow_array::LargeStringArray),
    Utf8View(arrow_array::StringViewArray),
    Binary(arrow_array::BinaryArray),
    LargeBinary(arrow_array::LargeBinaryArray),
    BinaryView(arrow_array::BinaryViewArray),
}

impl Bytes {
    fn of(a: &ArrayRef) -> Option<Bytes> {
        Some(match a.data_type() {
            DataType::Utf8 => Bytes::Utf8(a.as_string::<i32>().clone()),
            DataType::LargeUtf8 => Bytes::LargeUtf8(a.as_string::<i64>().clone()),
            DataType::Utf8View => Bytes::Utf8View(a.as_string_view().clone()),
            DataType::Binary => Bytes::Binary(a.as_binary::<i32>().clone()),
            DataType::LargeBinary => Bytes::LargeBinary(a.as_binary::<i64>().clone()),
            DataType::BinaryView => Bytes::BinaryView(a.as_binary_view().clone()),
            _ => return None,
        })
    }

    #[inline]
    fn value(&self, i: usize) -> &[u8] {
        match self {
            Bytes::Utf8(a) => a.value(i).as_bytes(),
            Bytes::LargeUtf8(a) => a.value(i).as_bytes(),
            Bytes::Utf8View(a) => a.value(i).as_bytes(),
            Bytes::Binary(a) => a.value(i),
            Bytes::LargeBinary(a) => a.value(i),
            Bytes::BinaryView(a) => a.value(i),
        }
    }
}

/// A string or binary key column, in place.
pub struct ArrowKeys {
    chunks: Vec<Bytes>,
    at: Chunks,
}

impl Keys for ArrowKeys {
    fn len(&self) -> usize {
        self.at.len()
    }
    #[inline]
    fn key(&self, i: usize) -> &[u8] {
        let (c, j) = self.at.locate(i);
        self.chunks[c].value(j)
    }
}

fn column(batches: &[RecordBatch], name: &str) -> Result<Vec<ArrayRef>> {
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let Some(a) = b.column_by_name(name) else {
            return err(format!("no column {name:?}"));
        };
        if a.null_count() > 0 {
            return err(format!("column {name:?} has nulls"));
        }
        out.push(a.clone());
    }
    Ok(out)
}

/// The key column: strings and binaries in place, integers as their decimal
/// text (packed once, like `str(key)`).
pub fn keys(batches: &[RecordBatch], name: &str) -> Result<Box<dyn Keys + Send>> {
    let cols = column(batches, name)?;
    let at = Chunks::new(cols.iter().map(|a| a.len()));
    if let Some(chunks) = cols.iter().map(Bytes::of).collect::<Option<Vec<_>>>() {
        return Ok(Box::new(ArrowKeys { chunks, at }));
    }
    let mut arena = Arena::default();
    let mut buf = Vec::new();
    for a in &cols {
        for i in 0..a.len() {
            buf.clear();
            if !text(a, i, &mut buf)? || !a.data_type().is_integer() {
                return err(format!(
                    "key column {name:?} must hold strings, binaries or integers, not {}",
                    a.data_type()
                ));
            }
            arena.push(&buf);
        }
    }
    Ok(Box::new(arena))
}

// -- versions ---------------------------------------------------------------------------

/// The raw integer of a date, time, timestamp or duration.
fn temporal(a: &dyn Array, i: usize) -> Option<i64> {
    use TimeUnit::*;
    Some(match a.data_type() {
        DataType::Date32 => a.as_primitive::<Date32Type>().value(i) as i64,
        DataType::Date64 => a.as_primitive::<Date64Type>().value(i),
        DataType::Time32(Second) => a.as_primitive::<Time32SecondType>().value(i) as i64,
        DataType::Time32(_) => a.as_primitive::<Time32MillisecondType>().value(i) as i64,
        DataType::Time64(Microsecond) => a.as_primitive::<Time64MicrosecondType>().value(i),
        DataType::Time64(_) => a.as_primitive::<Time64NanosecondType>().value(i),
        DataType::Timestamp(Second, _) => a.as_primitive::<TimestampSecondType>().value(i),
        DataType::Timestamp(Millisecond, _) => {
            a.as_primitive::<TimestampMillisecondType>().value(i)
        }
        DataType::Timestamp(Microsecond, _) => {
            a.as_primitive::<TimestampMicrosecondType>().value(i)
        }
        DataType::Timestamp(Nanosecond, _) => a.as_primitive::<TimestampNanosecondType>().value(i),
        DataType::Duration(Second) => a.as_primitive::<DurationSecondType>().value(i),
        DataType::Duration(Millisecond) => a.as_primitive::<DurationMillisecondType>().value(i),
        DataType::Duration(Microsecond) => a.as_primitive::<DurationMicrosecondType>().value(i),
        DataType::Duration(Nanosecond) => a.as_primitive::<DurationNanosecondType>().value(i),
        _ => return None,
    })
}

/// A value's text, for a declared revision column: strings and binaries as
/// they are, numbers in decimal, temporal values as their raw integer.
fn text(a: &ArrayRef, i: usize, out: &mut Vec<u8>) -> Result<bool> {
    use std::io::Write;
    if let Some(b) = Bytes::of(a) {
        out.extend_from_slice(b.value(i));
        return Ok(true);
    }
    macro_rules! num {
        ($t:ty) => {
            write!(out, "{}", a.as_primitive::<$t>().value(i)).unwrap()
        };
    }
    if let Some(v) = temporal(a.as_ref(), i) {
        write!(out, "{v}").unwrap();
        return Ok(true);
    }
    match a.data_type() {
        DataType::Int8 => num!(Int8Type),
        DataType::Int16 => num!(Int16Type),
        DataType::Int32 => num!(Int32Type),
        DataType::Int64 => num!(Int64Type),
        DataType::UInt8 => num!(UInt8Type),
        DataType::UInt16 => num!(UInt16Type),
        DataType::UInt32 => num!(UInt32Type),
        DataType::UInt64 => num!(UInt64Type),
        DataType::Float32 => num!(Float32Type),
        DataType::Float64 => num!(Float64Type),
        DataType::Boolean => write!(out, "{}", a.as_boolean().value(i)).unwrap(),
        DataType::Decimal128(_, _) => out.extend_from_slice(
            a.as_primitive::<Decimal128Type>()
                .value_as_string(i)
                .as_bytes(),
        ),
        _ => return Ok(false),
    }
    Ok(true)
}

/// A declared revision column: each row's version is its value's text.
pub struct Revision {
    chunks: Vec<ArrayRef>,
    at: Chunks,
}

impl Revision {
    pub fn new(batches: &[RecordBatch], name: &str) -> Result<Revision> {
        let chunks = column(batches, name)?;
        let mut probe = Vec::new();
        for a in &chunks {
            if !a.is_empty() && !text(a, 0, &mut probe)? {
                return err(format!(
                    "revision column {name:?} must hold strings, binaries, numbers or times, not {}",
                    a.data_type()
                ));
            }
        }
        let at = Chunks::new(chunks.iter().map(|a| a.len()));
        Ok(Revision { chunks, at })
    }
}

impl Versions for Revision {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> Result<()> {
        let mut buf = Vec::new();
        for &r in rows {
            let (c, j) = self.at.locate(r as usize);
            buf.clear();
            text(&self.chunks[c], j, &mut buf)?;
            out.push(&buf);
        }
        Ok(())
    }
}

/// Row digests: see the module documentation.
pub struct RowDigest {
    batches: Vec<RecordBatch>,
    columns: Vec<(Vec<u8>, usize)>, // name, index, in name order
    at: Chunks,
}

impl RowDigest {
    pub fn new(batches: Vec<RecordBatch>) -> Result<RowDigest> {
        let mut columns: Vec<(Vec<u8>, usize)> = match batches.first() {
            Some(b) => b
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| (f.name().as_bytes().to_vec(), i))
                .collect(),
            None => Vec::new(),
        };
        columns.sort();
        if let Some(b) = batches.first() {
            for (_, i) in &columns {
                check(b.column(*i).data_type())?;
            }
        }
        let at = Chunks::new(batches.iter().map(|b| b.num_rows()));
        Ok(RowDigest {
            batches,
            columns,
            at,
        })
    }

    fn digest(&self, row: usize) -> [u8; 16] {
        let (c, j) = self.at.locate(row);
        let b = &self.batches[c];
        let mut h = Xxh3::new();
        let mut buf = Vec::with_capacity(256);
        for (name, i) in &self.columns {
            put_varint(&mut buf, name.len() as u64);
            buf.extend_from_slice(name);
            value(b.column(*i).as_ref(), j, &mut buf);
            if buf.len() > 4096 {
                h.update(&buf);
                buf.clear();
            }
        }
        h.update(&buf);
        h.digest128().to_le_bytes()
    }
}

impl Versions for RowDigest {
    fn fill(&mut self, rows: &[u32], out: &mut Arena) -> Result<()> {
        let digests: Vec<[u8; 16]> = rows.par_iter().map(|&r| self.digest(r as usize)).collect();
        for d in &digests {
            out.push(d);
        }
        Ok(())
    }
}

/// Whether `value` can encode a type.
fn check(t: &DataType) -> Result<()> {
    match t {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            check(f.data_type())
        }
        DataType::Struct(fs) => fs.iter().try_for_each(|f| check(f.data_type())),
        DataType::Map(f, _) => check(f.data_type()),
        DataType::Dictionary(_, v) => check(v),
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => Ok(()),
        other => err(format!("cannot digest a column of type {other}")),
    }
}

fn nanos(unit: &TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

fn bytes(out: &mut Vec<u8>, tag: u8, b: &[u8]) {
    out.push(tag);
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// A value's canonical encoding: a tag byte, then the value.
///
/// | tag | value |
/// |---|---|
/// | 0 | null |
/// | 1 | boolean: 1 byte |
/// | 2 | signed integer, any width: i64 |
/// | 3 | unsigned integer, any width: u64 |
/// | 4 | float, any width: f64 bits |
/// | 5 | string, any layout: varint length, UTF-8 |
/// | 6 | binary, any layout: varint length, bytes |
/// | 7 | decimal, any width: scale (i8), then i256 |
/// | 8 | date, either unit: milliseconds since the epoch, i64 |
/// | 9 | time of day, any unit: nanoseconds, i64 |
/// | 10 | timestamp, any unit: nanoseconds (i128), then the time zone as a string |
/// | 11 | duration, any unit: nanoseconds, i128 |
/// | 12 | interval: its unit (u8), then the raw value |
/// | 13 | list, any layout: varint length, then each element |
/// | 14 | struct: each field in name order, as name (varint length, bytes) and value |
/// | 15 | map: varint length, then each key and value |
///
/// Integers are little-endian; dictionary values encode as the value they
/// point at.
fn value(a: &dyn Array, i: usize, out: &mut Vec<u8>) {
    if a.is_null(i) {
        out.push(0);
        return;
    }
    macro_rules! int {
        ($tag:expr, $t:ty, $as:ty) => {{
            out.push($tag);
            out.extend_from_slice(&(a.as_primitive::<$t>().value(i) as $as).to_le_bytes());
        }};
    }
    match a.data_type() {
        DataType::Null => out.push(0),
        DataType::Boolean => {
            out.push(1);
            out.push(a.as_boolean().value(i) as u8);
        }
        DataType::Int8 => int!(2, Int8Type, i64),
        DataType::Int16 => int!(2, Int16Type, i64),
        DataType::Int32 => int!(2, Int32Type, i64),
        DataType::Int64 => int!(2, Int64Type, i64),
        DataType::UInt8 => int!(3, UInt8Type, u64),
        DataType::UInt16 => int!(3, UInt16Type, u64),
        DataType::UInt32 => int!(3, UInt32Type, u64),
        DataType::UInt64 => int!(3, UInt64Type, u64),
        DataType::Float16 => {
            out.push(4);
            out.extend_from_slice(
                &a.as_primitive::<Float16Type>()
                    .value(i)
                    .to_f64()
                    .to_bits()
                    .to_le_bytes(),
            );
        }
        DataType::Float32 => int!(4, Float32Type, f64),
        DataType::Float64 => int!(4, Float64Type, f64),
        DataType::Utf8 => bytes(out, 5, a.as_string::<i32>().value(i).as_bytes()),
        DataType::LargeUtf8 => bytes(out, 5, a.as_string::<i64>().value(i).as_bytes()),
        DataType::Utf8View => bytes(out, 5, a.as_string_view().value(i).as_bytes()),
        DataType::Binary => bytes(out, 6, a.as_binary::<i32>().value(i)),
        DataType::LargeBinary => bytes(out, 6, a.as_binary::<i64>().value(i)),
        DataType::BinaryView => bytes(out, 6, a.as_binary_view().value(i)),
        DataType::FixedSizeBinary(_) => bytes(out, 6, a.as_fixed_size_binary().value(i)),
        DataType::Decimal128(_, s) => {
            out.push(7);
            out.push(*s as u8);
            let v = arrow_buffer::i256::from_i128(a.as_primitive::<Decimal128Type>().value(i));
            out.extend_from_slice(&v.to_le_bytes());
        }
        DataType::Decimal256(_, s) => {
            out.push(7);
            out.push(*s as u8);
            out.extend_from_slice(&a.as_primitive::<Decimal256Type>().value(i).to_le_bytes());
        }
        DataType::Date32 => {
            out.push(8);
            out.extend_from_slice(&(temporal(a, i).unwrap() * 86_400_000).to_le_bytes());
        }
        DataType::Date64 => int!(8, Date64Type, i64),
        DataType::Time32(u) | DataType::Time64(u) => {
            out.push(9);
            let v = temporal(a, i).unwrap() as i128 * nanos(u);
            out.extend_from_slice(&(v as i64).to_le_bytes());
        }
        DataType::Timestamp(u, tz) => {
            out.push(10);
            let raw = temporal(a, i).unwrap();
            out.extend_from_slice(&(raw as i128 * nanos(u)).to_le_bytes());
            let tz = tz.as_deref().unwrap_or("");
            put_varint(out, tz.len() as u64);
            out.extend_from_slice(tz.as_bytes());
        }
        DataType::Duration(u) => {
            out.push(11);
            let raw = temporal(a, i).unwrap();
            out.extend_from_slice(&(raw as i128 * nanos(u)).to_le_bytes());
        }
        DataType::Interval(u) => {
            out.push(12);
            match u {
                IntervalUnit::YearMonth => {
                    out.push(0);
                    out.extend_from_slice(
                        &a.as_primitive::<IntervalYearMonthType>()
                            .value(i)
                            .to_le_bytes(),
                    );
                }
                IntervalUnit::DayTime => {
                    out.push(1);
                    let v = a.as_primitive::<IntervalDayTimeType>().value(i);
                    out.extend_from_slice(&v.days.to_le_bytes());
                    out.extend_from_slice(&v.milliseconds.to_le_bytes());
                }
                IntervalUnit::MonthDayNano => {
                    out.push(2);
                    let v = a.as_primitive::<IntervalMonthDayNanoType>().value(i);
                    out.extend_from_slice(&v.months.to_le_bytes());
                    out.extend_from_slice(&v.days.to_le_bytes());
                    out.extend_from_slice(&v.nanoseconds.to_le_bytes());
                }
            }
        }
        DataType::List(_) => list(out, a.as_list::<i32>().value(i).as_ref()),
        DataType::LargeList(_) => list(out, a.as_list::<i64>().value(i).as_ref()),
        DataType::FixedSizeList(_, _) => list(out, a.as_fixed_size_list().value(i).as_ref()),
        DataType::Struct(fields) => {
            out.push(14);
            let s = a.as_struct();
            let mut order: Vec<usize> = (0..fields.len()).collect();
            order.sort_by(|&x, &y| fields[x].name().cmp(fields[y].name()));
            for f in order {
                let name = fields[f].name().as_bytes();
                put_varint(out, name.len() as u64);
                out.extend_from_slice(name);
                value(s.column(f).as_ref(), i, out);
            }
        }
        DataType::Map(_, _) => {
            out.push(15);
            let m = a.as_map();
            let (start, end) = (
                m.value_offsets()[i] as usize,
                m.value_offsets()[i + 1] as usize,
            );
            put_varint(out, (end - start) as u64);
            for j in start..end {
                value(m.keys().as_ref(), j, out);
                value(m.values().as_ref(), j, out);
            }
        }
        DataType::Dictionary(k, _) => {
            let d = a.as_any_dictionary();
            let key = match k.as_ref() {
                DataType::Int8 => a.as_dictionary::<Int8Type>().keys().value(i) as usize,
                DataType::Int16 => a.as_dictionary::<Int16Type>().keys().value(i) as usize,
                DataType::Int32 => a.as_dictionary::<Int32Type>().keys().value(i) as usize,
                DataType::Int64 => a.as_dictionary::<Int64Type>().keys().value(i) as usize,
                DataType::UInt8 => a.as_dictionary::<UInt8Type>().keys().value(i) as usize,
                DataType::UInt16 => a.as_dictionary::<UInt16Type>().keys().value(i) as usize,
                DataType::UInt32 => a.as_dictionary::<UInt32Type>().keys().value(i) as usize,
                _ => a.as_dictionary::<UInt64Type>().keys().value(i) as usize,
            };
            value(d.values().as_ref(), key, out);
        }
        _ => unreachable!("checked by `check`"),
    }
}

fn list(out: &mut Vec<u8>, items: &dyn Array) {
    out.push(13);
    put_varint(out, items.len() as u64);
    for j in 0..items.len() {
        value(items, j, out);
    }
}
