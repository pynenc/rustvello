use std::hash::{Hash, Hasher};
use twox_hash::{XxHash32, XxHash64};

/// Converts a `&str` to a deterministic `u32` using the `XxHash32` hashing algorithm.
pub fn deterministic_u32_from_str(input: &str) -> u32 {
    let mut hasher = XxHash32::default();
    input.hash(&mut hasher);
    hasher.finish() as u32
}

/// Converts a `&str` to a deterministic `u64` using the `XxHash64` hashing algorithm.
pub fn deterministic_u64_from_str(input: &str) -> u64 {
    let mut hasher = XxHash64::default();
    input.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::deterministic_u32_from_str;

    #[test]
    fn test_deterministic_u32_from_str() {
        let code = "test_metric";
        let id1 = deterministic_u32_from_str(code);
        let id2 = deterministic_u32_from_str(code);

        assert_eq!(id1, id2, "The IDs should be consistent for the same input");
    }
}
