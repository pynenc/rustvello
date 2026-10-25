//! Lowercase hex encoding for digest output.

use std::fmt::Write;

/// Encode bytes as lowercase hex, two characters per byte.
pub(crate) fn lower_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::lower_hex;

    #[test]
    fn encodes_lowercase_two_chars_per_byte() {
        assert_eq!(lower_hex(&[0x00, 0x0a, 0xff, 0x10]), "000aff10");
        assert_eq!(lower_hex(&[]), "");
    }
}
