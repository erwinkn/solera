#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version {
    pub generation: u64,
    pub deleted: bool,
    pub payload: Option<Vec<u8>>,
    pub predecessor: Option<u64>,
    /// The predecessor's payload, on a payload-bearing index.
    pub prior: Option<Vec<u8>>,
}
pub fn retain(versions: &[Version], endpoints: &[u64], base: bool) -> Vec<Version> {
    let Some(oldest) = versions.last() else {
        return Vec::new();
    };
    let (span_predecessor, span_prior) = (oldest.predecessor, oldest.prior.clone());
    let mut kept: Vec<Version> = Vec::new();
    for (i, v) in versions.iter().enumerate() {
        let seen = i > 0
            && endpoints
                .iter()
                .any(|&g| v.generation < g && g <= versions[i - 1].generation);
        if i == 0 || seen {
            kept.push(Version {
                predecessor: None,
                prior: None,
                ..v.clone()
            });
        }
    }
    if base {
        let first = endpoints.iter().copied().min().unwrap_or(u64::MAX);
        if kept
            .last()
            .is_some_and(|v| v.generation < first && v.deleted)
        {
            kept.pop();
        }
    } else if let Some(v) = kept.last_mut() {
        v.predecessor = span_predecessor;
        v.prior = span_prior;
    }
    kept
}
pub fn change(versions: &[Version], g_p: u64, g_n1: u64) -> Option<(u8, &Version)> {
    let at_n = versions.iter().find(|v| older(v.generation, g_n1))?;
    if at_n.generation < g_p {
        return None; // nothing in the range
    }
    // The state before P: its version, else what the oldest version replaced.
    let (before, payload) = match versions.iter().find(|v| v.generation < g_p) {
        Some(v) => (!v.deleted, v.payload.as_deref()),
        None => match versions.last() {
            Some(v) if v.predecessor.is_some() => (true, v.prior.as_deref()),
            _ => (false, None),
        },
    };
    let class = match (before, !at_n.deleted) {
        (false, true) => ADDED,
        (true, true) if payload.is_some() && payload == at_n.payload.as_deref() => NEITHER,
        (true, true) => UPDATED,
        (true, false) => REMOVED,
        (false, false) => NEITHER,
    };
    Some((class, at_n))
}
pub fn older(g: u64, bound: u64) -> bool {
    bound == u64::MAX || g < bound
}
pub const ADDED:u8=0; pub const UPDATED:u8=1; pub const REMOVED:u8=2; pub const NEITHER:u8=3;

fn main() {
    let mut n=0;
    for before in [None,Some(b"v1".to_vec()),Some(b"v2".to_vec()),Some(vec![])] {
        for after in [None,Some(b"v1".to_vec()),Some(b"v2".to_vec()),Some(vec![])] {
            let expected=match (before.is_some(),after.is_some()) {
                (false,true)=>ADDED, (true,false)=>REMOVED, (false,false)=>NEITHER,
                (true,true)=>if before==after {NEITHER} else {UPDATED},
            };
            let vs=vec![Version {generation:20,deleted:after.is_none(),payload:after.clone(),predecessor:before.as_ref().map(|_|1),prior:before.clone()}];
            assert_eq!(change(&vs,10,30).unwrap().0,expected);
            assert_eq!(change(&retain(&vs,&[],false),10,30).unwrap().0,expected);
            let held=vec![Version {generation:20,deleted:after.is_none(),payload:after.clone(),predecessor:None,prior:None},
                Version {generation:1,deleted:before.is_none(),payload:before.clone(),predecessor:None,prior:None}];
            assert_eq!(change(&held,10,30).unwrap().0,expected);
            n+=3;
        }
    }
    let unversioned=vec![Version {generation:20,deleted:false,payload:None,predecessor:Some(1),prior:None}];
    assert_eq!(change(&unversioned,10,30).unwrap().0,UPDATED);
    println!("{} exact-source net-rule checks passed",n+1);
}
