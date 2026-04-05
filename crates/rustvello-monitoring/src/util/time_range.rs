//! Time range and resolution parsing.

use chrono::Duration;

/// Parse a time range string like "5m", "1h", "1d" into a `chrono::Duration`.
pub fn parse_time_range(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: i64 = num_str.parse().ok()?;
    match unit {
        "s" => Some(Duration::seconds(num)),
        "m" => Some(Duration::minutes(num)),
        "h" => Some(Duration::hours(num)),
        "d" => Some(Duration::days(num)),
        "w" => Some(Duration::weeks(num)),
        _ => None,
    }
}

/// Parse a resolution string like "1s", "1m", "auto" into seconds.
pub fn parse_resolution(s: &str) -> Option<f64> {
    match s.trim() {
        "auto" | "" => None,
        other => {
            let dur = parse_time_range(other)?;
            Some(dur.num_milliseconds() as f64 / 1000.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_time_range() {
        assert_eq!(parse_time_range("5m"), Some(Duration::minutes(5)));
        assert_eq!(parse_time_range("1h"), Some(Duration::hours(1)));
        assert_eq!(parse_time_range("1d"), Some(Duration::days(1)));
        assert_eq!(parse_time_range("2w"), Some(Duration::weeks(2)));
        assert!(parse_time_range("invalid").is_none());
        assert!(parse_time_range("").is_none());
    }

    #[test]
    fn test_parse_resolution() {
        assert_eq!(parse_resolution("1s"), Some(1.0));
        assert_eq!(parse_resolution("1m"), Some(60.0));
        assert!(parse_resolution("auto").is_none());
    }
}
