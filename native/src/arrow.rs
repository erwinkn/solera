//! Written content as Arrow record batches, read in place: the key column
//! alone — strings as they are, integers as their decimal text.

use crate::error::{Error, Result};
use crate::rows::Arena;
use crate::sort::{is_sorted, order as sort_order, Keys};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;

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
            while (c as usize) + 2 < starts.len() && starts[c as usize + 1] <= j << 12 {
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

/// The key column: strings in place, integers as their decimal text (packed
/// once) — the rule Python rows follow (`solera.stores.key_text`). Binary
/// columns hold index keys as they are, so they are keys only with `binary`.
pub fn keys(batches: &[RecordBatch], name: &str, binary: bool) -> Result<Box<dyn Keys + Send>> {
    let cols = column(batches, name)?;
    let at = Chunks::new(cols.iter().map(|a| a.len()));
    let is_text = |a: &ArrayRef| {
        matches!(
            a.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    };
    if cols.iter().all(|a| binary || is_text(a)) {
        if let Some(chunks) = cols.iter().map(Bytes::of).collect::<Option<Vec<_>>>() {
            return Ok(Box::new(ArrowKeys { chunks, at }));
        }
    }
    let mut arena = Arena::default();
    let mut buf = Vec::new();
    for a in &cols {
        if !a.data_type().is_integer() {
            return err(format!(
                "key column {name:?} must hold strings or integers, not {}",
                a.data_type()
            ));
        }
        for i in 0..a.len() {
            buf.clear();
            integer(a.as_ref(), i, &mut buf);
            arena.push(&buf);
        }
    }
    Ok(Box::new(arena))
}

/// An integer column's value `i`, as decimal text.
fn integer(a: &dyn Array, i: usize, out: &mut Vec<u8>) {
    let text = match a.data_type() {
        DataType::Int8 => a.as_primitive::<Int8Type>().value(i).to_string(),
        DataType::Int16 => a.as_primitive::<Int16Type>().value(i).to_string(),
        DataType::Int32 => a.as_primitive::<Int32Type>().value(i).to_string(),
        DataType::Int64 => a.as_primitive::<Int64Type>().value(i).to_string(),
        DataType::UInt8 => a.as_primitive::<UInt8Type>().value(i).to_string(),
        DataType::UInt16 => a.as_primitive::<UInt16Type>().value(i).to_string(),
        DataType::UInt32 => a.as_primitive::<UInt32Type>().value(i).to_string(),
        DataType::UInt64 => a.as_primitive::<UInt64Type>().value(i).to_string(),
        other => unreachable!("an integer column, not {other}"),
    };
    out.extend_from_slice(text.as_bytes());
}

/// Arrow data's top-level column names, each once: a name twice would let
/// the key be read from one column while a store keeps the other. Checked
/// where Arrow data comes in, before any is chosen.
pub fn unique_columns(schema: &arrow_schema::Schema) -> Result<()> {
    let mut names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    names.sort_unstable();
    if let Some(w) = names.windows(2).find(|w| w[0] == w[1]) {
        return err(format!("column {:?} appears twice", w[0]));
    }
    Ok(())
}

// -- a source's table -------------------------------------------------------------------

/// What a keyed source served as one Arrow table (`Loaded(table, version=…)`),
/// read from its key and version columns alone: `keys`, those asked (with
/// none asked, the table's own), each with the version it was served at, or
/// None when the table has no row for it; `rows`, the table row of each key
/// served, in the order of `keys`, and `at`, which of `keys` each is.
pub struct ServedTable {
    pub keys: Arena,
    pub versions: Vec<Option<Box<str>>>,
    pub rows: Vec<u32>,
    pub at: Vec<usize>,
}

/// A table that cannot say what it served is an error, never a pick: a key
/// twice, a key or version missing (no column, or a null), a version that
/// is not a string; checked on the schema too, so an empty table is held to
/// it. At most `u32::MAX` rows, the row numbers' width.
pub fn served(
    schema: &arrow_schema::Schema,
    batches: &[RecordBatch],
    key: &str,
    version: &str,
    asked: Option<Arena>,
) -> Result<ServedTable> {
    if key == version {
        return err(format!(
            "the version column cannot be the key column {key:?}"
        ));
    }
    let is_text =
        |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
    let type_of = |name: &str| match schema.field_with_name(name) {
        Ok(f) => Ok(f.data_type().clone()),
        Err(_) => err(format!("no column {name:?}")),
    };
    let t = type_of(key)?;
    if !is_text(&t) && !t.is_integer() {
        return err(format!(
            "key column {key:?} must hold strings or integers, not {t}"
        ));
    }
    let t = type_of(version)?;
    if !is_text(&t) {
        return err(format!(
            "version column {version:?} must hold strings, not {t}"
        ));
    }
    let keys = self::keys(batches, key, false)?;
    let n = keys.len();
    if n > u32::MAX as usize {
        return err(format!(
            "a table of {n} rows: at most {} are read",
            u32::MAX
        ));
    }
    let cols = column(batches, version)?;
    let chunks: Vec<Bytes> = cols.iter().filter_map(Bytes::of).collect();
    let at = Chunks::new(cols.iter().map(|a| a.len()));
    let version_of = |i: usize| -> Box<str> {
        let (c, j) = at.locate(i);
        String::from_utf8_lossy(chunks[c].value(j)).into()
    };
    let order: Vec<u32> = if is_sorted(&*keys) {
        (0..n as u32).collect()
    } else {
        sort_order(&*keys)
    };
    if let Some(w) = order
        .windows(2)
        .find(|w| keys.key(w[0] as usize) == keys.key(w[1] as usize))
    {
        return err(format!(
            "key {:?} has two rows",
            String::from_utf8_lossy(keys.key(w[0] as usize))
        ));
    }
    let Some(asked) = asked else {
        let mut all = Arena::default();
        for i in 0..n {
            all.push(keys.key(i));
        }
        return Ok(ServedTable {
            keys: all,
            versions: (0..n).map(|i| Some(version_of(i))).collect(),
            rows: (0..n as u32).collect(),
            at: (0..n).collect(),
        });
    };
    let mut by: Vec<usize> = (0..asked.len()).collect();
    by.sort_by(|&a, &b| asked.get(a).cmp(asked.get(b)));
    let mut row_of = vec![None; asked.len()];
    let mut t = 0;
    for &a in &by {
        let k = asked.get(a);
        while t < n && keys.key(order[t] as usize) < k {
            t += 1;
        }
        if t < n && keys.key(order[t] as usize) == k {
            row_of[a] = Some(order[t]);
        }
    }
    let (mut versions, mut rows, mut at) = (Vec::with_capacity(asked.len()), vec![], vec![]);
    for (a, r) in row_of.into_iter().enumerate() {
        versions.push(r.map(|r| version_of(r as usize)));
        if let Some(r) = r {
            rows.push(r);
            at.push(a);
        }
    }
    Ok(ServedTable {
        keys: asked,
        versions,
        rows,
        at,
    })
}
