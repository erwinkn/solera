//! Runs of any blocks merged newest-wins, as a range and as pages: an
//! error, never a panic. The bytes are cut into up to three runs of up to
//! four blocks each; the codec is any byte.
#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use solera_native::format::{merge_page, merge_range};

#[derive(Arbitrary, Debug)]
struct Input<'a> {
    runs: Vec<(u8, Vec<&'a [u8]>)>,
    after: Option<&'a [u8]>,
    upto: Option<&'a [u8]>,
    limit: u8,
    drop_deleted: bool,
}

fuzz_target!(|input: Input| {
    let runs: Vec<Vec<&[u8]>> = input
        .runs
        .iter()
        .take(3)
        .map(|(_, blocks)| blocks.iter().take(4).copied().collect())
        .collect();
    let codecs: Vec<u8> = input.runs.iter().take(3).map(|(c, _)| *c).collect();
    let _ = merge_range(&runs, &codecs, input.after, input.upto, input.drop_deleted);
    let _ = merge_page(
        &runs,
        &codecs,
        input.after,
        input.upto,
        input.limit as usize,
        input.drop_deleted,
        u64::MAX,
    );
});
