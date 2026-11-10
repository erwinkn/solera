//! Progress: what a consumer holds of one input (docs/tracking-design.md §5).
//! A keyed edge holds pieces (key ranges with one value, covering the key
//! space) and points (single keys overriding their piece). An unkeyed edge
//! holds "consumed through commit c".

use crate::codec::{Dec, Enc};
use crate::error::{format, Result};
use crate::state::Id;

pub const TAG_EDGE_OPEN: u8 = 0x01;
pub const TAG_EDGE_CLOSE: u8 = 0x02;

#[derive(Clone, Debug, PartialEq)]
pub enum Progress {
    Keyed(Keyed),
    /// Consumed through this commit of the input (0: nothing).
    Unkeyed(u64),
}

/// Pieces and points; filled in by the progress module.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Keyed {}

impl Progress {
    pub fn new(keyed: bool) -> Progress {
        if keyed {
            Progress::Keyed(Keyed::default())
        } else {
            Progress::Unkeyed(0)
        }
    }

    pub fn encode(&self, e: &mut Enc) {
        match self {
            Progress::Keyed(_) => e.u8(0),
            Progress::Unkeyed(c) => {
                e.u8(1);
                e.uv(*c);
            }
        }
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Progress> {
        match d.u8()? {
            0 => Ok(Progress::Keyed(Keyed::default())),
            1 => Ok(Progress::Unkeyed(d.uv()?)),
            t => format(format!("progress tag {t}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EdgeOpen {
    pub edge: Id,
    pub input: Id,
    pub keyed: bool,
}

impl EdgeOpen {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
        e.bytes(&self.input);
        e.u8(self.keyed as u8);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<EdgeOpen> {
        Ok(EdgeOpen {
            edge: d.bytes()?.into(),
            input: d.bytes()?.into(),
            keyed: match d.u8()? {
                0 => false,
                1 => true,
                b => return format(format!("bool {b}")),
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EdgeClose {
    pub edge: Id,
}

impl EdgeClose {
    pub fn encode(&self, e: &mut Enc) {
        e.bytes(&self.edge);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<EdgeClose> {
        Ok(EdgeClose {
            edge: d.bytes()?.into(),
        })
    }
}
