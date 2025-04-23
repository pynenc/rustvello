use chrono::{DateTime, Utc};

/// Converts a DateTime<Utc> to a timestamp in i64 from chronos library
pub fn datetime_to_i64(datetime: DateTime<Utc>) -> i64 {
    datetime.timestamp_micros()
}

/// Gets current UTC timestamp i64
pub fn utc_now_i64() -> i64 {
    datetime_to_i64(utc_now())
}

/// Gets current UTC timestamp i64
pub fn utc_now() -> DateTime<Utc> {
    Utc::now()
}
