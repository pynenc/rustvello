use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash, Default, Deserialize, Serialize)]
pub enum TimestampResolution {
    Years,
    Months,
    Weeks,
    Days,
    Hours,
    Minutes,
    #[default]
    Seconds,
    Milliseconds,
    Microseconds,
}

impl FromStr for TimestampResolution {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TimestampResolution::iter_ordered()
            .find(|&res| res.as_str().eq_ignore_ascii_case(s))
            .ok_or_else(|| format!("Invalid resolution: {}", s))
    }
}

impl fmt::Display for TimestampResolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TimestampResolution {
    // Method to return an iterator over TimestampResolution from finest to coarsest
    pub fn iter_ordered() -> impl Iterator<Item = TimestampResolution> {
        [
            TimestampResolution::Microseconds,
            TimestampResolution::Milliseconds,
            TimestampResolution::Seconds,
            TimestampResolution::Minutes,
            TimestampResolution::Hours,
            TimestampResolution::Days,
            TimestampResolution::Weeks,
            TimestampResolution::Months,
            TimestampResolution::Years,
        ]
        .iter()
        .copied()
    }

    /// Method to get the string representation of the enum variant
    pub fn as_str(&self) -> &'static str {
        match *self {
            TimestampResolution::Years => "Years",
            TimestampResolution::Months => "Months",
            TimestampResolution::Weeks => "Weeks",
            TimestampResolution::Days => "Days",
            TimestampResolution::Hours => "Hours",
            TimestampResolution::Minutes => "Minutes",
            TimestampResolution::Seconds => "Seconds",
            TimestampResolution::Milliseconds => "Milliseconds",
            TimestampResolution::Microseconds => "Microseconds",
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            TimestampResolution::Years => 0,
            TimestampResolution::Months => 1,
            TimestampResolution::Weeks => 2,
            TimestampResolution::Days => 3,
            TimestampResolution::Hours => 4,
            TimestampResolution::Minutes => 5,
            TimestampResolution::Seconds => 6,
            TimestampResolution::Milliseconds => 7,
            TimestampResolution::Microseconds => 8,
        }
    }

    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => TimestampResolution::Years,
            1 => TimestampResolution::Months,
            2 => TimestampResolution::Weeks,
            3 => TimestampResolution::Days,
            4 => TimestampResolution::Hours,
            5 => TimestampResolution::Minutes,
            6 => TimestampResolution::Seconds,
            7 => TimestampResolution::Milliseconds,
            8 => TimestampResolution::Microseconds,
            _ => panic!("Unexpected value: {value}"),
        }
    }

    /// Converts the `TimestampResolution` to a `Duration`.
    pub fn to_duration(&self) -> Duration {
        match *self {
            TimestampResolution::Years => Duration::from_secs(31_536_000), // 365 days
            TimestampResolution::Months => Duration::from_secs(2_592_000), // 30 days
            TimestampResolution::Weeks => Duration::from_secs(604_800),    // 7 days
            TimestampResolution::Days => Duration::from_secs(86_400),      // 24 hours
            TimestampResolution::Hours => Duration::from_secs(3_600),      // 1 hour
            TimestampResolution::Minutes => Duration::from_secs(60),       // 1 minute
            TimestampResolution::Seconds => Duration::from_secs(1),        // 1 second
            TimestampResolution::Milliseconds => Duration::from_millis(1), // 1 millisecond
            TimestampResolution::Microseconds => Duration::from_micros(1), // 1 microsecond
        }
    }
}
