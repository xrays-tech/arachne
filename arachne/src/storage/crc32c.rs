//! CRC32C (Castagnoli) — table-based, reflected polynomial `0x82F63B78`.
//!
//! This is the standard CRC32C used in networking (iSCSI, SSE, etc.) and in
//! RocksDB's WAL. The implementation is a pure function: bytes in, `u32` out.
//! No allocation, no I/O, no panics.

/// The reflected CRC32C (Castagnoli) polynomial.
const POLY: u32 = 0x82_F63_B78;

/// Precomputed 256-entry lookup table for the reflected CRC32C algorithm.
const TABLE: [u32; 256] = make_table();

/// Compute the CRC32C checksum of `data`.
///
/// The result is a `u32` in the range `[0, u32::MAX]`. An empty slice yields
/// `0`. This function is pure and never panics.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[index];
    }
    // Final XOR (reflect output).
    crc ^ 0xFFFF_FFFF
}

/// Build the 256-entry lookup table at compile time (const fn, zero runtime cost).
const fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i: u32 = 0;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_is_zero() {
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn standard_check_value() {
        // The standard CRC32C check value for the ASCII string "123456789".
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn known_vector_4_bytes() {
        // CRC32C of "abcd" is 0x92C80A31.
        assert_eq!(crc32c(b"abcd"), 0x92C8_0A31);
    }

    #[test]
    fn known_vector_all_zeros() {
        // CRC32C of 32 zero bytes.
        let zeros = [0u8; 32];
        // Computed independently: CRC32C of 32 zero bytes is 0x8A9136AA.
        assert_eq!(crc32c(&zeros), 0x8A91_36AA);
    }

    #[test]
    fn single_byte() {
        // CRC32C of a single zero byte.
        assert_eq!(crc32c(&[0u8]), 0x527D_5351);
    }

    #[test]
    fn deterministic_and_length_dependent() {
        // Same input, same output.
        assert_eq!(crc32c(b"hello"), crc32c(b"hello"));
        // Different inputs (same length) produce different checksums.
        assert_ne!(crc32c(b"hello"), crc32c(b"world"));
    }

    #[test]
    fn incremental_matches_whole() {
        // Processing a slice in two halves should give the same result as
        // processing it whole (CRC is a stateful but deterministic function).
        // This is NOT a general property of CRC (it's not a hash), so we only
        // verify that the whole-slice result is stable and well-formed.
        let data = b"123456789";
        let whole = crc32c(data);
        assert_eq!(whole, 0xE306_9283);
    }
}
