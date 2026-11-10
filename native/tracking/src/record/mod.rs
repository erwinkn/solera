//! The batch record (docs/tracking-design.md §7): one per committed batch per
//! edge, about 200 bytes plus the slice of progress it replaced. It stands
//! alone: the batch's exact input is the diff from that slice to its head,
//! over its range, so lineage needs no earlier record, and the digest makes
//! the recomputation honest.

use std::sync::Arc;

use crate::codec::{Dec, Enc};
use crate::error::{format, Result};
use crate::progress::{Conditions, Key, KeyRange, Point, Slice};
use crate::state::Id;

pub const RECORD_FORMAT: u8 = 2;

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
    /// The points the batch wrote, each with its reason.
    pub points: Vec<(Key, Point)>,
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
        Slice {
            pieces: vec![],
            points: self.points.clone(),
        }
        .encode(&mut e);
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
        let points = Slice::decode(&mut d)?.points;
        let mut counts = [0; 4];
        for c in &mut counts {
            *c = d.uv()?;
        }
        let digest = d.uv()?;
        let planner = d.uv()?;
        let own_head = d.opt_u64()?;
        let output_commit = d.opt_u64()?;
        let keys = match d.u8()? {
            0 => None,
            1 => Some(
                (0..d.count()?)
                    .map(|_| d.bytes().map(Key::from))
                    .collect::<Result<_>>()?,
            ),
            t => return format(format!("keys tag {t}")),
        };
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
            points,
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
