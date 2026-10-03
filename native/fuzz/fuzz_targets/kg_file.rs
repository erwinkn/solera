//! Any bytes read as a garbage file (`.kg`): an error, never a panic.
#![no_main]

use libfuzzer_sys::fuzz_target;
use solera_native::garbage;

fuzz_target!(|data: &[u8]| {
    let _ = garbage::decode(data);
});
