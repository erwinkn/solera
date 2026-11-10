//! A failing key's stored outcome, its transitions and when its retry is
//! due (docs/per-key-processing.md §9, the authoritative table). This is the
//! one definition: the core calls it at a batch commit, and Python's
//! `solera.key_outcomes` calls it through the bindings.

use crate::codec::{Dec, Enc};
use crate::error::{format, Result};

pub const REJECTED: u8 = 1;
pub const FAILED: u8 = 2;
pub const RETRYING: u8 = 3;
pub const CANCELED: u8 = 4;
pub const TIMED_OUT: u8 = 5;
/// The names of the stored outcomes, by code (index 0 unused).
pub const NAMES: [&str; 6] = [
    "",
    "rejected",
    "failed",
    "retrying",
    "canceled",
    "timed_out",
];

/// Bytes of an error message kept with its record.
pub const MESSAGE_MAX: usize = 200;
/// Seconds before a transient error's first retry; the wait doubles up to
/// `BACKOFF_MAX` (solera.errors).
pub const BACKOFF_FIRST: f64 = 60.0;
pub const BACKOFF_MAX: f64 = 6.0 * 3600.0;

/// What one key's call came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Ok,
    Removed,
    Unmatched,
    Rejected,
    Failed,
    Transient,
    Canceled,
    TimedOut,
}

impl Kind {
    pub fn parse(s: &str) -> Option<Kind> {
        Some(match s {
            "ok" => Kind::Ok,
            "removed" => Kind::Removed,
            "unmatched" => Kind::Unmatched,
            "rejected" => Kind::Rejected,
            "failed" => Kind::Failed,
            "transient" => Kind::Transient,
            "canceled" => Kind::Canceled,
            "timed_out" => Kind::TimedOut,
            _ => return None,
        })
    }

    /// The stored outcome it becomes; None for ok, removed and unmatched.
    pub fn code(self) -> Option<u8> {
        match self {
            Kind::Ok | Kind::Removed | Kind::Unmatched => None,
            Kind::Rejected => Some(REJECTED),
            Kind::Failed => Some(FAILED),
            Kind::Transient => Some(RETRYING),
            Kind::Canceled => Some(CANCELED),
            Kind::TimedOut => Some(TIMED_OUT),
        }
    }
}

/// What a worker reports for one key.
#[derive(Clone, Debug, PartialEq)]
pub struct Reported {
    pub kind: Kind,
    pub message: Box<[u8]>,
    /// A transient error's earliest next try, in seconds.
    pub retry_after: Option<f64>,
    /// How long a transient key keeps retrying from its first failure, in seconds.
    pub retry_for: Option<f64>,
}

/// One failing key's stored outcome. Times are whole seconds. `deploy` and
/// `forced` are the engine's counters the last try ran under, so whether a
/// key had its deploy or forced retry is decided causally; `next_at` and
/// `until` schedule, `since` and `last` display.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stored {
    pub outcome: u8,
    pub tries: u64,
    pub deploy: u64,
    pub forced: u64,
    pub since: u64,
    pub last: u64,
    /// A retrying or timed-out key's due time; 0 otherwise.
    pub next_at: u64,
    /// When a retrying key turns failed; 0 otherwise.
    pub until: u64,
    pub message: Box<[u8]>,
}

impl Stored {
    pub fn encode(&self, e: &mut Enc) {
        e.u8(self.outcome);
        for n in [
            self.tries,
            self.deploy,
            self.forced,
            self.since,
            self.last,
            self.next_at,
            self.until,
        ] {
            e.uv(n);
        }
        e.bytes(&self.message);
    }

    pub fn decode(d: &mut Dec<'_>) -> Result<Stored> {
        let outcome = d.u8()?;
        if !(REJECTED..=TIMED_OUT).contains(&outcome) {
            return format(format!("outcome {outcome}"));
        }
        Ok(Stored {
            outcome,
            tries: d.uv()?,
            deploy: d.uv()?,
            forced: d.uv()?,
            since: d.uv()?,
            last: d.uv()?,
            next_at: d.uv()?,
            until: d.uv()?,
            message: d.bytes()?.into(),
        })
    }

    /// Whether its retry comes due by time.
    pub fn timed(&self) -> bool {
        matches!(self.outcome, RETRYING | TIMED_OUT)
    }
}

/// Seconds before the next try after `tries` transient failures: one minute,
/// doubling, at most six hours.
pub fn backoff(tries: u64) -> f64 {
    let doublings = tries.saturating_sub(1).min(32) as i32;
    (BACKOFF_FIRST * 2f64.powi(doublings)).min(BACKOFF_MAX)
}

fn clip(message: &[u8]) -> Box<[u8]> {
    if message.len() <= MESSAGE_MAX {
        return message.into();
    }
    let mut end = MESSAGE_MAX;
    while end > 0 && (message[end] & 0xC0) == 0x80 {
        end -= 1;
    }
    message[..end].into()
}

fn ceil(x: f64) -> u64 {
    x.ceil().max(0.0) as u64
}

/// The key's record after `r` (§9's transition table): None for no record
/// (nothing, or the record goes). `fresh`: the try ran on other input than
/// the prior record's (or there is none), so its counts start over.
/// `deploy` and `forced` are the batch's counters; `retries` the asset's
/// `retries=`, which bounds timeouts.
pub fn transition(
    prior: Option<&Stored>,
    fresh: bool,
    r: &Reported,
    now: f64,
    deploy: u64,
    forced: u64,
    retries: u64,
) -> Option<Stored> {
    let code = r.kind.code()?;
    let t = now as u64;
    let prior = prior.filter(|_| !fresh);
    let counted = u64::from(code != CANCELED);
    let tries = prior.map_or(counted, |p| p.tries + counted);
    let mut s = Stored {
        outcome: code,
        tries,
        deploy,
        forced,
        since: prior.map_or(t, |p| p.since),
        last: t,
        next_at: 0,
        until: 0,
        message: clip(&r.message),
    };
    match code {
        RETRYING => {
            let until = match prior {
                Some(p) if p.outcome == RETRYING => p.until,
                _ => ceil(now + r.retry_for.unwrap_or(0.0)),
            };
            if now >= until as f64 {
                s.outcome = FAILED;
                return Some(s);
            }
            s.until = until;
            s.next_at = ceil(now + r.retry_after.unwrap_or_else(|| backoff(tries)));
        }
        TIMED_OUT => {
            if tries > retries {
                s.outcome = FAILED;
                return Some(s);
            }
            s.next_at = ceil(now + backoff(tries));
        }
        _ => {}
    }
    Some(s)
}

/// The counters a retry runs under: the deploy number, and per stored
/// outcome the latest forced request's event counter (0: none).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub deploy: u64,
    pub forced: [u64; 6],
}

impl Counters {
    /// The counter a record written under these carries: the newest request.
    pub fn forced_at(&self) -> u64 {
        self.forced.iter().copied().max().unwrap_or(0)
    }

    /// Each counter's least of the two.
    pub fn least(&self, o: &Counters) -> Counters {
        let mut c = *self;
        c.deploy = c.deploy.min(o.deploy);
        for (f, g) in c.forced.iter_mut().zip(o.forced) {
            *f = (*f).min(g);
        }
        c
    }
}

/// Whether a retry takes `s`'s key at `now` (§9). Each clause retires
/// itself: a retried key's `next_at` moves on, its `deploy` becomes the
/// retry's, its `forced` the retry's counter. A canceled key matches only a
/// forced request.
pub fn eligible(s: &Stored, now: f64, c: &Counters) -> bool {
    (s.timed() && s.next_at as f64 <= now)
        || (s.outcome == FAILED && s.deploy < c.deploy)
        || s.forced < c.forced[s.outcome as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rep(kind: Kind) -> Reported {
        Reported {
            kind,
            message: b"ValueError: x".as_slice().into(),
            retry_after: None,
            retry_for: None,
        }
    }

    fn step(prior: Option<&Stored>, r: &Reported, now: f64) -> Option<Stored> {
        transition(prior, false, r, now, 3, 0, 2)
    }

    #[test]
    fn the_transition_table() {
        assert_eq!(step(None, &rep(Kind::Ok), 1000.0), None);
        let fresh = step(None, &rep(Kind::Failed), 1000.0).unwrap();
        assert_eq!(
            (fresh.outcome, fresh.tries, fresh.since, fresh.last),
            (FAILED, 1, 1000, 1000)
        );
        let again = transition(Some(&fresh), false, &rep(Kind::Failed), 2000.0, 4, 0, 2).unwrap();
        assert_eq!(
            (again.tries, again.since, again.last, again.deploy),
            (2, 1000, 2000, 4)
        );
        assert_eq!(step(Some(&again), &rep(Kind::Removed), 3000.0), None);
        let other = transition(Some(&again), true, &rep(Kind::Failed), 3000.0, 4, 0, 2).unwrap();
        assert_eq!((other.tries, other.since), (1, 3000));
        let rejected = step(Some(&again), &rep(Kind::Rejected), 3000.0).unwrap();
        assert_eq!(
            (rejected.outcome, rejected.tries, rejected.since),
            (REJECTED, 3, 1000)
        );
    }

    #[test]
    fn transient_backs_off_within_its_budget() {
        let mut r = rep(Kind::Transient);
        r.retry_for = Some(7200.0);
        let first = step(None, &r, 1000.0).unwrap();
        assert_eq!(
            (first.outcome, first.next_at, first.until),
            (RETRYING, 1060, 8200)
        );
        let second = step(Some(&first), &r, 1100.0).unwrap();
        assert_eq!(
            (second.tries, second.until, second.next_at),
            (2, 8200, 1220)
        );
        r.retry_after = Some(5.0);
        let told = step(Some(&second), &r, 1300.0).unwrap();
        assert_eq!(told.next_at, 1305);
        assert_eq!(step(Some(&told), &r, 8200.0).unwrap().outcome, FAILED);
    }

    #[test]
    fn interruptions_and_timeouts() {
        let canceled = step(None, &rep(Kind::Canceled), 1000.0).unwrap();
        assert_eq!(
            (canceled.outcome, canceled.tries, canceled.next_at),
            (CANCELED, 0, 0)
        );
        let failed = step(None, &rep(Kind::Failed), 1000.0).unwrap();
        assert_eq!(
            step(Some(&failed), &rep(Kind::Canceled), 1000.0)
                .unwrap()
                .tries,
            1
        );
        let t = rep(Kind::TimedOut);
        let timed = step(None, &t, 1000.0).unwrap();
        assert_eq!(
            (timed.outcome, timed.tries, timed.next_at),
            (TIMED_OUT, 1, 1060)
        );
        let timed = step(Some(&step(Some(&timed), &t, 1000.0).unwrap()), &t, 1000.0).unwrap();
        assert_eq!(timed.outcome, FAILED);
    }

    #[test]
    fn eligibility_is_causal() {
        let mut r = rep(Kind::Transient);
        r.retry_for = Some(7200.0);
        let retrying = step(None, &r, 1000.0).unwrap();
        let c = Counters {
            deploy: 3,
            forced: [0; 6],
        };
        assert!(!eligible(&retrying, 1059.0, &c) && eligible(&retrying, 1060.0, &c));
        let failed = step(None, &rep(Kind::Failed), 1000.0).unwrap();
        assert!(!eligible(&failed, 1e9, &c));
        assert!(eligible(
            &failed,
            0.0,
            &Counters {
                deploy: 4,
                forced: [0; 6]
            }
        ));
        let tried = transition(None, false, &rep(Kind::Failed), 1e9, 3, 4031, 2).unwrap();
        let mut f = Counters {
            deploy: 3,
            forced: [0; 6],
        };
        f.forced[FAILED as usize] = 4031;
        assert!(!eligible(&tried, 0.0, &f));
        f.forced[FAILED as usize] = 4032;
        assert!(eligible(&tried, 0.0, &f));
        f.forced = [0, 9999, 0, 0, 0, 0];
        assert!(!eligible(&tried, 0.0, &f));
    }

    #[test]
    fn messages_are_clipped_on_a_character() {
        let mut r = rep(Kind::Failed);
        r.message = "é".repeat(300).into_bytes().into();
        let s = step(None, &r, 0.0).unwrap();
        assert_eq!(s.message.len(), 200);
        assert!(std::str::from_utf8(&s.message).is_ok());
        let mut e = Enc::new();
        s.encode(&mut e);
        assert_eq!(Stored::decode(&mut Dec::new(&e.buf)).unwrap(), s);
    }
}
