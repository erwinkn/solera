//! Kani proofs on the `.kx` readers that Kani can finish (docs/verification.md,
//! "Kani", has what it could not): the varint reader on any input, and the
//! filters' parser on any bytes and offsets. Run with `cargo kani -Z stubbing`
//! here: about 40 s.
//!
//! CRC-32 is stubbed: its SIMD paths are out of Kani's reach, and a
//! checksum is a value the input chooses anyway (a hostile file carries a
//! matching one). zlib is out of scope: every harness uses codec 0. The
//! writer is too: it compresses blocks on rayon's threads, which Kani does
//! not model, so round trips are left to the property tests and fuzzing.

#[cfg(kani)]
mod proofs {
    use solera_native::format::{
        get_varint, parse_filters, put_varint, CODEC_NONE, FOOTER_SIZE, FORMAT_VERSION, MAGIC,
    };

    fn crc(data: &[u8]) -> u32 {
        data.len() as u32 // cheap, and the same for the writer and the reader
    }

    /// Any bytes, of any length up to `N`.
    fn any_bytes<const N: usize>(buf: &[u8; N]) -> &[u8] {
        let len: usize = kani::any_where(|&l| l <= N);
        &buf[..len]
    }

    // -- varints ---------------------------------------------------------------------

    #[kani::proof]
    #[kani::unwind(13)]
    fn a_varint_read_stays_in_bounds() {
        let buf: [u8; 12] = kani::any();
        let data = any_bytes(&buf);
        let start: usize = kani::any_where(|&p| p <= data.len());
        let mut pos = start;
        if get_varint(data, &mut pos).is_ok() {
            assert!(pos > start && pos <= data.len() && pos - start <= 10);
        }
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn a_varint_round_trips() {
        let n: u64 = kani::any();
        let mut out = Vec::new();
        put_varint(&mut out, n);
        let mut pos = 0;
        assert!(get_varint(&out, &mut pos).ok() == Some(n));
        assert!(pos == out.len());
    }

    /// A varint the reader accepts is read whole: its value is the bytes'
    /// LEB128 value, no bit dropped (F23), so any reader that refuses what
    /// does not fit 64 bits reads the same number.
    #[kani::proof]
    #[kani::unwind(12)]
    fn a_varint_read_loses_no_bits() {
        let buf: [u8; 11] = kani::any();
        let mut pos = 0;
        if let Ok(n) = get_varint(&buf, &mut pos) {
            let mut exact: u128 = 0;
            for (i, b) in buf[..pos].iter().enumerate() {
                exact |= ((b & 0x7F) as u128) << (7 * i);
            }
            assert!(exact == n as u128, "a varint read with bits dropped");
        }
    }

    // -- footers, indexes, filters ------------------------------------------------------

    /// `n` bytes of anything, then a footer whose magic, version and codec
    /// (none) are right and whose every other field is anything. Error
    /// messages that format a symbolic value are out of CBMC's reach in
    /// practice, and zlib is out of scope: those fields are fixed.
    fn with_footer<const N: usize>() -> [u8; N] {
        let mut buf: [u8; N] = kani::any();
        let f = N - FOOTER_SIZE;
        buf[f..f + 4].copy_from_slice(MAGIC);
        buf[f + 4..f + 6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf[f + 6] = CODEC_NONE;
        buf[f + 44..f + 48].copy_from_slice(MAGIC);
        buf
    }

    /// Two filters in 16 bytes and their footer, anywhere in a file of any
    /// size: an error, or filters whose bits are exactly `nbits / 8` bytes.
    #[kani::proof]
    #[kani::stub(crc32fast::hash, crc)]
    #[kani::unwind(18)]
    fn any_filters_parse_or_err() {
        let tail = with_footer::<{ 16 + FOOTER_SIZE }>();
        let file_size: u64 = kani::any();
        if let Ok(filters) = parse_filters(&tail, file_size) {
            for (nbits, _, bits) in filters {
                assert!(bits.len() as u64 == nbits / 8);
            }
        }
    }
}
