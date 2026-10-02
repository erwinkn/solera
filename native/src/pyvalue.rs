//! Python values in the digest grammar (docs/row-digest.md): the walker
//! that `Rows.records` (row by row, as the rows lie in their list) and
//! `Rows.values` (a window of keys at a time) use.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{
    IntoPyDict, PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyMapping,
    PyMemoryView, PyString, PyTuple, PyType,
};
use pyo3::{ffi, intern};

use crate::digest::{self, Digest, Entries, Scalar};

pub struct Walker<'py> {
    datetime: Bound<'py, PyType>,
    date: Bound<'py, PyType>,
    time: Bound<'py, PyType>,
    timedelta: Bound<'py, PyType>,
    decimal: Bound<'py, PyType>,
    utc: Bound<'py, PyAny>,
    epochs: (Bound<'py, PyAny>, Bound<'py, PyAny>), // 1970-01-01T00:00, naive and UTC
    pool: Vec<Entries>,                             // one per nesting depth
    buf: Vec<u8>,
    plan: Plan,
}

/// The columns of the rows last read, sorted once: a write's rows share
/// their column names (the same `str` objects, usually), so a row whose
/// names match is written in the plan's order without sorting its own.
#[derive(Default)]
struct Plan {
    /// The column names in the dict's order, held so their addresses stay theirs.
    keys: Vec<Py<PyString>>,
    /// Each one's UTF-8 name, in `names[bounds[i]..bounds[i + 1]]`.
    names: Vec<u8>,
    bounds: Vec<usize>,
    /// Each one's place in the record (sorted by name), or `SKIP`.
    rank: Vec<u32>,
    /// By place: `len(name) ‖ name`, in `heads[ends[r - 1]..ends[r]]`.
    heads: Vec<u8>,
    ends: Vec<usize>,
    /// The columns left out that the plan was made for.
    skip: Vec<String>,
    /// A row's encoded values, by place: `None` for a null.
    values: Vec<u8>,
    spans: Vec<Option<(usize, usize)>>,
}

const SKIP: u32 = u32::MAX;

/// A `str`'s UTF-8 bytes, borrowed from its own cache; None when it has lone surrogates.
///
/// # Safety
/// `s` is a live `str` and outlives the slice.
unsafe fn utf8<'a>(s: *mut ffi::PyObject) -> Option<&'a [u8]> {
    let mut n: ffi::Py_ssize_t = 0;
    let p = ffi::PyUnicode_AsUTF8AndSize(s, &mut n);
    if p.is_null() {
        ffi::PyErr_Clear();
        return None;
    }
    Some(std::slice::from_raw_parts(p as *const u8, n as usize))
}

/// Encodes an exact `str`, `int` (within i64), `float` or `bool` — false
/// for anything else. Runs no Python code, so the caller's borrowed
/// references stay good.
///
/// # Safety
/// `v` is a live object.
unsafe fn fast(v: *mut ffi::PyObject, out: &mut Vec<u8>) -> bool {
    if ffi::PyUnicode_CheckExact(v) != 0 {
        match utf8(v) {
            Some(b) => digest::str(out, b),
            None => return false,
        }
    } else if ffi::PyLong_CheckExact(v) != 0 {
        let mut overflow = 0;
        let n = ffi::PyLong_AsLongLongAndOverflow(v, &mut overflow);
        if overflow != 0 || (n == -1 && !ffi::PyErr_Occurred().is_null()) {
            ffi::PyErr_Clear();
            return false;
        }
        digest::int(out, n);
    } else if ffi::PyFloat_CheckExact(v) != 0 {
        digest::float(out, ffi::PyFloat_AsDouble(v));
    } else if v == ffi::Py_True() || v == ffi::Py_False() {
        out.extend_from_slice(&[b'o', (v == ffi::Py_True()) as u8]);
    } else {
        return false;
    }
    true
}

fn unsupported(v: &Bound<'_, PyAny>) -> PyErr {
    let name = v
        .get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_default();
    PyValueError::new_err(format!(
        "cannot digest a value of type {name}: declare `revision=` on the output"
    ))
}

fn value_err(e: crate::format::Error) -> PyErr {
    match e {
        crate::format::Error::Value(m)
        | crate::format::Error::Format(m)
        | crate::format::Error::Limit(m) => PyValueError::new_err(m),
        crate::format::Error::Callback(e) => PyValueError::new_err(e.to_string()),
    }
}

fn int_attr(v: &Bound<'_, PyAny>, name: &Bound<'_, PyString>) -> PyResult<i64> {
    v.getattr(name)?.extract()
}

impl<'py> Walker<'py> {
    pub fn new(py: Python<'py>) -> PyResult<Walker<'py>> {
        let dt = py.import("datetime")?;
        let ty = |m: &Bound<'py, PyModule>, n: &str| -> PyResult<Bound<'py, PyType>> {
            Ok(m.getattr(n)?.cast_into::<PyType>()?)
        };
        let (datetime, utc) = (
            ty(&dt, "datetime")?,
            dt.getattr("timezone")?.getattr("utc")?,
        );
        Ok(Walker {
            datetime: datetime.clone(),
            date: ty(&dt, "date")?,
            time: ty(&dt, "time")?,
            timedelta: ty(&dt, "timedelta")?,
            decimal: ty(&py.import("decimal")?, "Decimal")?,
            utc: utc.clone(),
            epochs: (
                datetime.call1((1970, 1, 1))?,
                datetime.call((1970, 1, 1), Some(&[("tzinfo", &utc)].into_py_dict(py)?))?,
            ),
            pool: Vec::new(),
            buf: Vec::new(),
            plan: Plan::default(),
        })
    }

    /// `enc(v)`.
    pub fn value(
        &mut self,
        v: &Bound<'py, PyAny>,
        out: &mut Vec<u8>,
        depth: usize,
    ) -> PyResult<()> {
        if let Ok(d) = v.cast_exact::<PyDict>() {
            return self.mapping(d.iter(), &[] as &[&str], out, depth);
        }
        if let Ok(l) = v.cast_exact::<PyList>() {
            digest::list(out, l.len());
            for e in l.iter() {
                self.value(&e, out, depth + 1)?;
            }
            return Ok(());
        }
        if let Ok(t) = v.cast_exact::<PyTuple>() {
            digest::list(out, t.len());
            for e in t.iter() {
                self.value(&e, out, depth + 1)?;
            }
            return Ok(());
        }
        if let Some(()) = self.scalar(v, &mut |s| s.encode(out))? {
            return Ok(());
        }
        if let Ok(m) = v.cast::<PyMapping>() {
            let items = m.items()?;
            let pairs: Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)> =
                items.iter().map(|p| p.extract()).collect::<PyResult<_>>()?;
            return self.mapping(pairs.into_iter(), &[] as &[&str], out, depth);
        }
        if v.cast::<PyList>().is_ok() || v.cast::<PyTuple>().is_ok() {
            let items: Vec<Bound<'py, PyAny>> = v.try_iter()?.collect::<PyResult<_>>()?;
            digest::list(out, items.len());
            for e in &items {
                self.value(e, out, depth + 1)?;
            }
            return Ok(());
        }
        Err(unsupported(v))
    }

    /// A record (all keys strings) or a map, leaving out the fields in `skip`.
    fn mapping(
        &mut self,
        items: impl Iterator<Item = (Bound<'py, PyAny>, Bound<'py, PyAny>)>,
        skip: &[impl AsRef<str>],
        out: &mut Vec<u8>,
        depth: usize,
    ) -> PyResult<()> {
        if self.pool.len() <= depth {
            self.pool.resize_with(depth + 1, Entries::default);
        }
        let mut e = std::mem::take(&mut self.pool[depth]);
        e.clear();
        let items: Vec<_> = items.collect();
        let record = items.iter().all(|(k, _)| k.cast::<PyString>().is_ok());
        let mut key = Vec::new();
        for (k, v) in &items {
            let name = if record {
                let s = k.cast::<PyString>()?.to_str()?;
                if skip.iter().any(|x| x.as_ref() == s) {
                    continue;
                }
                key.clear();
                key.extend_from_slice(s.as_bytes());
                &key
            } else {
                if k.is_none() {
                    return Err(PyValueError::new_err("a map key is None"));
                }
                key.clear();
                self.value(k, &mut key, depth + 1)?;
                &key
            };
            self.value(v, e.name(name), depth + 1)?;
            e.end();
        }
        let r = e.write(out, record).map_err(value_err);
        self.pool[depth] = e;
        r
    }

    /// Calls `f` with `v` as a scalar; `None` when `v` is a container.
    pub fn scalar(&self, v: &Bound<'py, PyAny>, f: &mut dyn FnMut(Scalar)) -> PyResult<Option<()>> {
        if v.get_type().is(&self.datetime) {
            // A plain datetime (checked first: rows are full of them), as its
            // distance from the epoch, aware or naive as it is: one subtraction.
            let py = v.py();
            let tz = v.getattr(intern!(py, "tzinfo"))?;
            let aware = !tz.is_none()
                && (tz.is(&self.utc) || !v.call_method0(intern!(py, "utcoffset"))?.is_none());
            let since = v.sub(if aware {
                &self.epochs.1
            } else {
                &self.epochs.0
            })?;
            f(Scalar::Timestamp(self.delta(&since)?, aware));
        } else if v.is_none() {
            f(Scalar::Null);
        } else if let Ok(s) = v.cast_exact::<PyString>() {
            f(Scalar::Str(s.to_str()?.as_bytes()));
        } else if let Ok(b) = v.cast::<PyBool>() {
            f(Scalar::Bool(b.is_true()));
        } else if let Ok(i) = v.cast::<PyInt>() {
            match i.extract::<i64>() {
                Ok(n) => f(Scalar::Int(n as i128)),
                Err(_) => match i.extract::<i128>() {
                    Ok(n) => f(Scalar::Int(n)),
                    Err(_) => f(Scalar::BigInt(i.str()?.to_string())),
                },
            }
        } else if let Ok(x) = v.cast::<PyFloat>() {
            f(Scalar::Float(x.value()));
        } else if let Ok(s) = v.cast::<PyString>() {
            f(Scalar::Str(s.to_str()?.as_bytes()));
        } else if let Ok(b) = v.cast::<PyBytes>() {
            f(Scalar::Bytes(b.as_bytes()));
        } else if let Ok(b) = v.cast::<PyByteArray>() {
            f(Scalar::Bytes(&b.to_vec()));
        } else if v.cast::<PyMemoryView>().is_ok() {
            // Its bytes in logical (C) order, whatever its shape, strides or format.
            let b = v.call_method0("tobytes")?;
            f(Scalar::Bytes(b.cast::<PyBytes>()?.as_bytes()));
        } else if v.is_instance(&self.datetime)? {
            let tname = v.get_type().name()?;
            if tname == "NaTType" {
                f(Scalar::Null);
                return Ok(Some(()));
            }
            let py = v.py();
            let days = digest::days(
                int_attr(v, intern!(py, "year"))?,
                int_attr(v, intern!(py, "month"))? as u32,
                int_attr(v, intern!(py, "day"))? as u32,
            );
            let mut ns = days as i128 * 86_400_000_000_000
                + self.clock(v)? as i128
                + self.extra(v, &self.datetime, intern!(py, "nanosecond"))? as i128;
            let tz = v.getattr(intern!(py, "tzinfo"))?;
            let offset = if tz.is_none() {
                None
            } else if tz.is(&self.utc) {
                Some(0)
            } else {
                let o = v.call_method0(intern!(py, "utcoffset"))?;
                (!o.is_none()).then(|| self.delta(&o)).transpose()?
            };
            if let Some(o) = offset {
                ns -= o;
            }
            f(Scalar::Timestamp(ns, offset.is_some()));
        } else if v.is_instance(&self.date)? {
            let py = v.py();
            f(Scalar::Date(digest::days(
                int_attr(v, intern!(py, "year"))?,
                int_attr(v, intern!(py, "month"))? as u32,
                int_attr(v, intern!(py, "day"))? as u32,
            )));
        } else if v.is_instance(&self.time)? {
            if !v.getattr(intern!(v.py(), "tzinfo"))?.is_none() {
                return Err(PyValueError::new_err(
                    "cannot digest a time of day with a timezone: declare `revision=` on the output",
                ));
            }
            f(Scalar::Time(self.clock(v)?));
        } else if v.is_instance(&self.timedelta)? {
            f(Scalar::Duration(self.delta(v)?));
        } else if v.is_instance(&self.decimal)? {
            let t = v.call_method0("as_tuple")?;
            let (sign, digits, exp): (i64, Bound<'py, PyTuple>, Bound<'py, PyAny>) = t.extract()?;
            let Ok(exp) = exp.extract::<i64>() else {
                return Err(PyValueError::new_err(
                    "cannot digest a NaN or infinite Decimal: declare `revision=` on the output",
                ));
            };
            let mut s = String::from(if sign == 1 { "-" } else { "" });
            for d in digits.iter() {
                s.push(char::from(b'0' + d.extract::<u8>()?));
            }
            f(Scalar::Decimal(s, -exp));
        } else if let Some(item) = self.foreign(v)? {
            match item {
                // A builtin bool, int or float: classified above, so this ends.
                Some(x) => return self.scalar(&x, f),
                None => f(Scalar::Null),
            }
        } else {
            return Ok(None);
        }
        Ok(Some(()))
    }

    /// A NumPy scalar as the builtin it equals — a bool, an int, or a float of
    /// at most 64 bits — and pandas' missing markers as None; other NumPy
    /// scalars (extended precision, complex, datetime64, …) are an error.
    #[allow(clippy::type_complexity)]
    fn foreign(&self, v: &Bound<'py, PyAny>) -> PyResult<Option<Option<Bound<'py, PyAny>>>> {
        let t = v.get_type();
        let module = t.module()?.to_string();
        if module.starts_with("pandas") && matches!(&*t.name()?.to_string(), "NAType" | "NaTType") {
            return Ok(Some(None));
        }
        if module != "numpy" || !v.hasattr("dtype")? {
            return Ok(None);
        }
        let dtype = v.getattr("dtype")?;
        let kind: String = dtype.getattr("kind")?.extract()?;
        let size: usize = dtype.getattr("itemsize")?.extract()?;
        let py = v.py();
        let builtin = match kind.as_str() {
            "b" => py.get_type::<PyBool>().call1((v.is_truthy()?,))?,
            "i" | "u" => py.get_type::<PyInt>().call1((v,))?,
            "f" if size <= 8 => py.get_type::<PyFloat>().call1((v,))?,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "cannot digest a NumPy {}: declare `revision=` on the output",
                    dtype.str()?
                )))
            }
        };
        Ok(Some(Some(builtin)))
    }

    /// A subclass's extra integer attribute (pandas' nanoseconds), else 0.
    fn extra(
        &self,
        v: &Bound<'py, PyAny>,
        base: &Bound<'py, PyType>,
        name: &Bound<'py, PyString>,
    ) -> PyResult<i64> {
        if v.get_type().is(base) || !v.hasattr(name)? {
            return Ok(0);
        }
        int_attr(v, name)
    }

    /// Nanoseconds since midnight of a datetime or time.
    fn clock(&self, v: &Bound<'py, PyAny>) -> PyResult<i64> {
        let py = v.py();
        let (h, m) = (
            int_attr(v, intern!(py, "hour"))?,
            int_attr(v, intern!(py, "minute"))?,
        );
        let s = int_attr(v, intern!(py, "second"))?;
        Ok(((h * 60 + m) * 60 + s) * 1_000_000_000
            + int_attr(v, intern!(py, "microsecond"))? * 1_000)
    }

    /// Nanoseconds of a timedelta (pandas' with its own nanoseconds).
    fn delta(&self, v: &Bound<'py, PyAny>) -> PyResult<i128> {
        let py = v.py();
        let ns = self.extra(v, &self.timedelta, intern!(py, "nanoseconds"))?;
        let days = int_attr(v, intern!(py, "days"))? as i128;
        let secs = int_attr(v, intern!(py, "seconds"))? as i128;
        let micros = int_attr(v, intern!(py, "microseconds"))? as i128;
        Ok(((days * 86_400 + secs) * 1_000_000 + micros) * 1_000 + ns as i128)
    }

    /// `row(r)` of a row — a mapping of column names — without the columns in `skip`.
    pub fn row(&mut self, r: &Bound<'py, PyAny>, skip: &[impl AsRef<str>]) -> PyResult<Digest> {
        let mut buf = std::mem::take(&mut self.buf);
        buf.clear();
        buf.extend_from_slice(&digest::ROW);
        let res = match r.cast_exact::<PyDict>() {
            Ok(d) => match self.planned(d, skip, &mut buf) {
                Ok(true) => Ok(()),
                Ok(false) => self.mapping(d.iter(), skip, &mut buf, 0),
                Err(e) => Err(e),
            },
            Err(_) => match r.cast::<PyMapping>() {
                Ok(m) => {
                    let pairs: Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)> = m
                        .items()?
                        .iter()
                        .map(|p| p.extract())
                        .collect::<PyResult<_>>()?;
                    self.mapping(pairs.into_iter(), skip, &mut buf, 0)
                }
                Err(_) => Err(PyValueError::new_err(format!(
                    "a row must be a mapping of column names, not {}",
                    r.get_type().name()?
                ))),
            },
        };
        if res.is_ok() && buf.get(digest::ROW.len()) != Some(&b'r') {
            return Err(PyValueError::new_err(
                "a row's column names must be strings",
            ));
        }
        let d = digest::framed(&buf);
        self.buf = buf;
        res.map(|_| d)
    }

    /// Writes a dict row's record in the plan's order, made anew when its
    /// column names are not the plan's. False when a name is not an exact
    /// `str`: the general walk takes the row.
    fn planned(
        &mut self,
        d: &Bound<'py, PyDict>,
        skip: &[impl AsRef<str>],
        out: &mut Vec<u8>,
    ) -> PyResult<bool> {
        let fits = self.plan.skip.len() == skip.len()
            && self
                .plan
                .skip
                .iter()
                .zip(skip)
                .all(|(a, b)| a == b.as_ref());
        if (!fits || self.plan.keys.len() != d.len()) && !self.replan(d, skip) {
            return Ok(false);
        }
        let mut deferred: Vec<(usize, Bound<'py, PyAny>)> = Vec::new();
        let plan = &mut self.plan;
        plan.values.clear();
        plan.spans.clear();
        plan.spans.resize(plan.ends.len(), None);
        let (mut pos, mut k, mut v) = (0, std::ptr::null_mut(), std::ptr::null_mut());
        let mut i = 0;
        // Borrowed references, read without running Python code: values that
        // need it are held, and encoded once the dict is done with.
        while unsafe { ffi::PyDict_Next(d.as_ptr(), &mut pos, &mut k, &mut v) } != 0 {
            if i == plan.keys.len() || k != plan.keys[i].as_ptr() {
                // Another object: the same name, or another row shape.
                let same = i < plan.keys.len()
                    && unsafe { ffi::PyUnicode_CheckExact(k) } != 0
                    && unsafe { utf8(k) } == Some(&plan.names[plan.bounds[i]..plan.bounds[i + 1]]);
                if !same {
                    if !self.replan(d, skip) {
                        return Ok(false);
                    }
                    return self.planned(d, skip, out);
                }
            }
            let rank = plan.rank[i];
            i += 1;
            if rank == SKIP || v == unsafe { ffi::Py_None() } {
                continue;
            }
            let start = plan.values.len();
            if unsafe { fast(v, &mut plan.values) } {
                plan.spans[rank as usize] = Some((start, plan.values.len()));
            } else {
                deferred.push((rank as usize, unsafe {
                    Bound::from_borrowed_ptr(d.py(), v)
                }));
            }
        }
        let mut values = std::mem::take(&mut self.plan.values);
        for (rank, v) in deferred {
            let start = values.len();
            let r = self.value(&v, &mut values, 1);
            if let Err(e) = r {
                self.plan.values = values;
                return Err(e);
            }
            if &values[start..] != b"n" {
                self.plan.spans[rank] = Some((start, values.len()));
            }
        }
        let plan = &mut self.plan;
        plan.values = values;
        out.push(b'r');
        crate::format::put_varint(out, plan.spans.iter().flatten().count() as u64);
        for (r, span) in plan.spans.iter().enumerate() {
            if let Some((a, b)) = *span {
                let start = if r == 0 { 0 } else { plan.ends[r - 1] };
                out.extend_from_slice(&plan.heads[start..plan.ends[r]]);
                out.extend_from_slice(&plan.values[a..b]);
            }
        }
        Ok(true)
    }

    /// Makes the plan for a dict's column names; false unless all are exact
    /// `str`s, leaving no plan.
    fn replan(&mut self, d: &Bound<'py, PyDict>, skip: &[impl AsRef<str>]) -> bool {
        self.plan = Plan {
            values: std::mem::take(&mut self.plan.values),
            spans: std::mem::take(&mut self.plan.spans),
            ..Plan::default()
        };
        let plan = &mut self.plan;
        plan.bounds.push(0);
        for k in d.keys() {
            let name = match k.cast_exact::<PyString>() {
                Ok(s) => unsafe { utf8(s.as_ptr()) },
                Err(_) => None,
            };
            let Some(name) = name else {
                plan.keys.clear();
                plan.bounds.clear();
                return false;
            };
            plan.names.extend_from_slice(name);
            plan.bounds.push(plan.names.len());
            plan.keys.push(k.cast_into::<PyString>().unwrap().unbind());
        }
        let name = |i: usize| &plan.names[plan.bounds[i]..plan.bounds[i + 1]];
        let skipped = |i: usize| skip.iter().any(|s| s.as_ref().as_bytes() == name(i));
        let mut order: Vec<usize> = (0..plan.keys.len()).filter(|&i| !skipped(i)).collect();
        order.sort_unstable_by(|&a, &b| name(a).cmp(name(b)));
        let mut rank = vec![SKIP; plan.keys.len()];
        let (mut heads, mut ends) = (Vec::new(), Vec::new());
        for (r, &i) in order.iter().enumerate() {
            rank[i] = r as u32;
            digest::put_len(&mut heads, name(i));
            ends.push(heads.len());
        }
        (plan.rank, plan.heads, plan.ends) = (rank, heads, ends);
        plan.skip = skip.iter().map(|s| s.as_ref().to_string()).collect();
        true
    }

    /// A declared revision's text.
    pub fn render(&self, v: &Bound<'py, PyAny>, out: &mut Vec<u8>) -> PyResult<()> {
        let mut res = Ok(());
        match self.scalar(v, &mut |s| res = s.render(out))? {
            Some(()) => res.map_err(value_err),
            None => Err(PyValueError::new_err(format!(
                "a revision must be a scalar, not {}",
                v.get_type().name()?
            ))),
        }
    }
}
