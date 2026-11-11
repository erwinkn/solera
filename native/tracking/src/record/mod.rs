//! The batch record (docs/tracking-design.md §7): one per committed batch per
//! edge, about 200 bytes plus the slice of progress it replaced. It stands
//! alone: the batch's exact input is the diff from that slice to its head,
//! over its range, so lineage needs no earlier record, and the digest makes
//! the recomputation honest.

use std::sync::Arc;

use crate::codec::{Dec, Enc, Ver};
use crate::error::{format, Result};
use crate::progress::{Conditions, Key, KeyRange, Reason, Slice};
use crate::state::Id;

pub const RECORD_FORMAT: u8 = 3;

/// What a batch did with one key it did not load as planned and settle:
/// failed, held back by the planner, held back at commit (`Unsettled`), or
/// loaded at another version (`Deviation`). `loaded` is the version the
/// worker loaded where it was not the one planned, a failed or unsettled
/// key's included (HIS-3, HIS-8).
#[derive(Clone, Debug, PartialEq)]
pub struct Mark {
    pub reason: Reason,
    pub loaded: Option<Ver>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    /// Run, batch, attempt, as the engine encodes them.
    pub producer: Vec<u8>,
    pub edge: Id,
    pub output: Id,
    /// 0 incremental, 1 all, 2 key list.
    pub mode: u8,
    /// A full run's first batch: progress was reset.
    pub reset: bool,
    pub range: KeyRange,
    pub head: u64,
    pub conditions: Arc<Conditions>,
    /// The progress the batch replaced: its range's slice, or with a reset all of it.
    pub replaced: Slice,
    /// Sorted by key: every key the batch did not load as planned and
    /// settle (§7, "Points written"), whether or not progress needed a point
    /// for it.
    pub marks: Vec<(Key, Mark)>,
    /// Keys by class: added, updated, removed, unchanged.
    pub counts: [u64; 4],
    /// The digest of the batch's keys, classes and versions.
    pub digest: u64,
    pub planner: u64,
    /// The consumer's own head at plan, read where the replaced slice is held.
    pub own_head: Option<u64>,
    /// The output commit the batch made, if it wrote anything.
    pub output_commit: Option<u64>,
    /// A key list's keys: the one input a record cannot derive.
    pub keys: Option<Vec<Key>>,
    /// Failing keys the plan met and left to their retry, tried at what
    /// upstream has now (B6): lineage leaves them out.
    pub waiting: Vec<Key>,
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u8(RECORD_FORMAT);
        e.bytes(&self.producer);
        e.bytes(&self.edge);
        e.bytes(&self.output);
        e.u8(self.mode);
        e.u8(self.reset as u8);
        self.range.encode(&mut e);
        e.uv(self.head);
        self.conditions.encode(&mut e);
        self.replaced.encode(&mut e);
        e.uv(self.marks.len() as u64);
        for (k, m) in &self.marks {
            e.bytes(k);
            e.u8(m.reason as u8);
            match &m.loaded {
                None => e.u8(0),
                Some(v) => {
                    e.u8(1);
                    e.version(v.as_version());
                }
            }
        }
        for c in self.counts {
            e.uv(c);
        }
        e.uv(self.digest);
        e.uv(self.planner);
        e.opt_u64(self.own_head);
        e.opt_u64(self.output_commit);
        match &self.keys {
            None => e.u8(0),
            Some(keys) => {
                e.u8(1);
                e.uv(keys.len() as u64);
                for k in keys {
                    e.bytes(k);
                }
            }
        }
        e.uv(self.waiting.len() as u64);
        for k in &self.waiting {
            e.bytes(k);
        }
        e.buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Record> {
        let mut d = Dec::new(bytes);
        let f = d.u8()?;
        if f != RECORD_FORMAT {
            return format(format!("record format {f}, expected {RECORD_FORMAT}"));
        }
        let producer = d.bytes()?.to_vec();
        let edge = d.bytes()?.into();
        let output = d.bytes()?.into();
        let mode = d.u8()?;
        let reset = d.u8()? == 1;
        let range = KeyRange::decode(&mut d)?;
        let head = d.uv()?;
        let conditions = Arc::new(Conditions::decode(&mut d)?);
        let replaced = Slice::decode(&mut d)?;
        let marks: Vec<(Key, Mark)> = (0..d.count()?)
            .map(|_| {
                let k = Key::from(d.bytes()?);
                let reason = Reason::from_u8(d.u8()?)?;
                let loaded = match d.u8()? {
                    0 => None,
                    1 => Some(d.ver()?),
                    t => return format(format!("loaded tag {t}")),
                };
                Ok((k, Mark { reason, loaded }))
            })
            .collect::<Result<_>>()?;
        let mut counts = [0; 4];
        for c in &mut counts {
            *c = d.uv()?;
        }
        let digest = d.uv()?;
        let planner = d.uv()?;
        let own_head = d.opt_u64()?;
        let output_commit = d.opt_u64()?;
        if marks.windows(2).any(|w| w[0].0 >= w[1].0) {
            return format("marks out of order");
        }
        let keys: Option<Vec<Key>> = match d.u8()? {
            0 => None,
            1 => Some(
                (0..d.count()?)
                    .map(|_| d.bytes().map(Key::from))
                    .collect::<Result<_>>()?,
            ),
            t => return format(format!("keys tag {t}")),
        };
        if keys.as_ref().is_some_and(|ks| ks.windows(2).any(|w| w[0] >= w[1])) {
            return format("key list out of order");
        }
        let waiting = (0..d.count()?)
            .map(|_| d.bytes().map(Key::from))
            .collect::<Result<_>>()?;
        d.end()?;
        Ok(Record {
            producer,
            edge,
            output,
            mode,
            reset,
            range,
            head,
            conditions,
            replaced,
            marks,
            counts,
            digest,
            planner,
            own_head,
            output_commit,
            keys,
            waiting,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::{Piece, Value};

    fn k(s: &str) -> Key {
        s.as_bytes().into()
    }

    fn rec() -> Record {
        Record {
            producer: b"run/batch/attempt".to_vec(),
            edge: k("e"),
            output: k("items"),
            mode: 0,
            reset: false,
            range: KeyRange::all(),
            head: 1,
            conditions: Arc::default(),
            replaced: Slice::default(),
            marks: vec![],
            counts: [0; 4],
            digest: 0,
            planner: 0,
            own_head: None,
            output_commit: None,
            keys: None,
            waiting: vec![],
        }
    }

    /// Lineage binary-searches marks and keys and walks pieces in order, so a
    /// record whose lists are out of order is malformed, not a wrong answer.
    #[test]
    fn a_record_out_of_order_does_not_decode() {
        let mark = Mark {
            reason: Reason::Failed,
            loaded: None,
        };
        let piece = |lo: Option<&str>, hi: Option<&str>| Piece {
            range: KeyRange {
                lo: lo.map(k),
                hi: hi.map(k),
            },
            value: Value::Empty,
        };
        let mut bad = vec![];
        for ks in [["k3", "k1"], ["k1", "k1"]] {
            let mut r = rec();
            r.marks = ks.iter().map(|s| (k(s), mark.clone())).collect();
            bad.push(r);
            let mut r = rec();
            r.keys = Some(ks.iter().map(|s| k(s)).collect());
            bad.push(r);
        }
        for pieces in [
            vec![piece(None, None), piece(Some("a"), Some("b"))],
            vec![piece(None, Some("c")), piece(Some("b"), None)],
            vec![piece(Some("b"), Some("b"))],
        ] {
            let mut r = rec();
            r.replaced.pieces = pieces;
            bad.push(r);
        }
        for r in bad {
            assert!(
                matches!(Record::decode(&r.encode()), Err(crate::Error::Format(_))),
                "{r:?}"
            );
        }
        let mut r = rec();
        r.marks = vec![(k("k1"), mark.clone()), (k("k3"), mark)];
        r.keys = Some(vec![k("k1"), k("k3")]);
        r.replaced.pieces = vec![piece(None, Some("b")), piece(Some("c"), None)];
        assert_eq!(Record::decode(&r.encode()).unwrap(), r);
    }
}
