//! Python values in the digest grammar (docs/row-digest.md): the walker
//! that `Rows.records` and `Rows.values` use, a window of rows at a time.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyMapping, PyMemoryView,
    PyString, PyTuple, PyType,
};

use crate::digest::{self, Digest, Entries, Scalar};

pub struct Walker<'py> {
    datetime: Bound<'py, PyType>,
    date: Bound<'py, PyType>,
    time: Bound<'py, PyType>,
    timedelta: Bound<'py, PyType>,
    decimal: Bound<'py, PyType>,
    pool: Vec<Entries>, // one per nesting depth
    buf: Vec<u8>,
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
        crate::format::Error::Value(m) | crate::format::Error::Format(m) => {
            PyValueError::new_err(m)
        }
        crate::format::Error::Callback(e) => PyValueError::new_err(e.to_string()),
    }
}

fn int_attr(v: &Bound<'_, PyAny>, name: &str) -> PyResult<i64> {
    v.getattr(name)?.extract()
}

impl<'py> Walker<'py> {
    pub fn new(py: Python<'py>) -> PyResult<Walker<'py>> {
        let dt = py.import("datetime")?;
        let ty = |m: &Bound<'py, PyModule>, n: &str| -> PyResult<Bound<'py, PyType>> {
            Ok(m.getattr(n)?.cast_into::<PyType>()?)
        };
        Ok(Walker {
            datetime: ty(&dt, "datetime")?,
            date: ty(&dt, "date")?,
            time: ty(&dt, "time")?,
            timedelta: ty(&dt, "timedelta")?,
            decimal: ty(&py.import("decimal")?, "Decimal")?,
            pool: Vec::new(),
            buf: Vec::new(),
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
            return self.mapping(d.iter(), &[], out, depth);
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
            return self.mapping(pairs.into_iter(), &[], out, depth);
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
        skip: &[&str],
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
                if skip.contains(&s) {
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
        if v.is_none() {
            f(Scalar::Null);
        } else if let Ok(s) = v.cast_exact::<PyString>() {
            f(Scalar::Str(s.to_str()?.as_bytes()));
        } else if let Ok(b) = v.cast::<PyBool>() {
            f(Scalar::Bool(b.is_true()));
        } else if let Ok(i) = v.cast::<PyInt>() {
            match i.extract::<i128>() {
                Ok(n) => f(Scalar::Int(n)),
                Err(_) => f(Scalar::BigInt(i.str()?.to_string())),
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
            let days = digest::days(
                int_attr(v, "year")?,
                int_attr(v, "month")? as u32,
                int_attr(v, "day")? as u32,
            );
            let mut ns = days as i128 * 86_400_000_000_000
                + self.clock(v)? as i128
                + self.extra(v, &self.datetime, "nanosecond")? as i128;
            let offset = if v.getattr("tzinfo")?.is_none() {
                None
            } else {
                let o = v.call_method0("utcoffset")?;
                (!o.is_none()).then(|| self.delta(&o)).transpose()?
            };
            if let Some(o) = offset {
                ns -= o;
            }
            f(Scalar::Timestamp(ns, offset.is_some()));
        } else if v.is_instance(&self.date)? {
            f(Scalar::Date(digest::days(
                int_attr(v, "year")?,
                int_attr(v, "month")? as u32,
                int_attr(v, "day")? as u32,
            )));
        } else if v.is_instance(&self.time)? {
            if !v.getattr("tzinfo")?.is_none() {
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
    fn extra(&self, v: &Bound<'py, PyAny>, base: &Bound<'py, PyType>, name: &str) -> PyResult<i64> {
        if v.get_type().is(base) || !v.hasattr(name)? {
            return Ok(0);
        }
        int_attr(v, name)
    }

    /// Nanoseconds since midnight of a datetime or time.
    fn clock(&self, v: &Bound<'py, PyAny>) -> PyResult<i64> {
        Ok(
            ((int_attr(v, "hour")? * 60 + int_attr(v, "minute")?) * 60 + int_attr(v, "second")?)
                * 1_000_000_000
                + int_attr(v, "microsecond")? * 1_000,
        )
    }

    /// Nanoseconds of a timedelta (pandas' with its own nanoseconds).
    fn delta(&self, v: &Bound<'py, PyAny>) -> PyResult<i128> {
        let ns = self.extra(v, &self.timedelta, "nanoseconds")?;
        Ok(
            ((int_attr(v, "days")? as i128 * 86_400 + int_attr(v, "seconds")? as i128) * 1_000_000
                + int_attr(v, "microseconds")? as i128)
                * 1_000
                + ns as i128,
        )
    }

    /// `row(r)` of a row — a mapping of column names — without the columns in `skip`.
    pub fn row(&mut self, r: &Bound<'py, PyAny>, skip: &[&str]) -> PyResult<Digest> {
        let mut buf = std::mem::take(&mut self.buf);
        buf.clear();
        let res = match r.cast_exact::<PyDict>() {
            Ok(d) => self.mapping(d.iter(), skip, &mut buf, 0),
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
        if res.is_ok() && buf.first() != Some(&b'r') {
            return Err(PyValueError::new_err(
                "a row's column names must be strings",
            ));
        }
        let d = digest::row(&buf);
        self.buf = buf;
        res.map(|_| d)
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
