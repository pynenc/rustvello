//! Text formatting utilities.

/// Truncate a string to at most `n` characters, respecting char boundaries.
pub fn truncate_chars(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

/// Truncate an ID to 7 characters (like git short SHA).
pub fn truncate_id(id: &str) -> String {
    truncate_chars(id, 7).to_owned()
}

/// Shorten an ID to first 8 chars for display, appending `…` if truncated.
pub fn short_id(id: &str) -> String {
    if id.chars().count() > 8 {
        format!("{}…", truncate_chars(id, 8))
    } else {
        id.to_owned()
    }
}

/// Format a duration in seconds to a human-readable string.
pub fn format_duration_secs(seconds: f64) -> String {
    if seconds < 0.001 {
        format!("{:.0}µs", seconds * 1_000_000.0)
    } else if seconds < 1.0 {
        format!("{:.1}ms", seconds * 1000.0)
    } else if seconds < 60.0 {
        format!("{seconds:.1}s")
    } else if seconds < 3600.0 {
        let mins = (seconds / 60.0).floor();
        let secs = seconds % 60.0;
        format!("{mins:.0}m {secs:.0}s")
    } else {
        let hours = (seconds / 3600.0).floor();
        let mins = ((seconds % 3600.0) / 60.0).floor();
        format!("{hours:.0}h {mins:.0}m")
    }
}

/// Convert common cron patterns to human-readable English (pynmon-compatible format).
pub fn cron_to_human(cron: &str) -> String {
    match cron.trim() {
        "* * * * *" => "every minute".to_owned(),
        "*/5 * * * *" => "every 5 min".to_owned(),
        "*/10 * * * *" => "every 10 min".to_owned(),
        "*/15 * * * *" => "every 15 min".to_owned(),
        "*/30 * * * *" => "every 30 min".to_owned(),
        "0 * * * *" => "every hour".to_owned(),
        "0 */2 * * *" => "every 2 hours".to_owned(),
        "0 */6 * * *" => "every 6 hours".to_owned(),
        "0 */12 * * *" => "every 12 hours".to_owned(),
        "0 0 * * *" => "daily at midnight".to_owned(),
        "0 0 * * 0" => "weekly on Sunday".to_owned(),
        "0 0 1 * *" => "monthly on the 1st".to_owned(),
        other => other.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_id() {
        assert_eq!(truncate_id("abcdefghijk"), "abcdefg");
        assert_eq!(truncate_id("abc"), "abc");
        assert_eq!(truncate_id(""), "");
    }

    #[test]
    fn test_short_id() {
        assert_eq!(short_id("abcdefghijklm"), "abcdefgh…");
        assert_eq!(short_id("abcdefgh"), "abcdefgh");
        assert_eq!(short_id("abc"), "abc");
        assert_eq!(short_id(""), "");
        // char-safe: multi-byte chars don't panic
        assert_eq!(short_id("αβγδεζηθικ"), "αβγδεζηθ…");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration_secs(0.5), "500.0ms");
        assert_eq!(format_duration_secs(1.5), "1.5s");
        assert_eq!(format_duration_secs(90.0), "1m 30s");
        assert_eq!(format_duration_secs(3700.0), "1h 1m");
    }

    #[test]
    fn test_cron_to_human() {
        assert_eq!(cron_to_human("* * * * *"), "every minute");
        assert_eq!(cron_to_human("*/5 * * * *"), "every 5 min");
        assert_eq!(cron_to_human("*/15 * * * *"), "every 15 min");
        assert_eq!(cron_to_human("0 0 * * *"), "daily at midnight");
        assert_eq!(cron_to_human("0 3 * * 1-5"), "0 3 * * 1-5");
    }
}
