use std::cmp::Ordering;
use std::ops::{RangeBounds, RangeInclusive};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct NodeElementRange {
    pub node_id: Uuid,
    pub range: OrdRangeInc,
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct GetRangesRequest {
    pub ini: Option<u64>,
    pub end: Option<u64>,
}

/// Wrapper around `RangeInclusive<u64>` to implement `Ord` and `PartialOrd`.
#[derive(Debug, Clone, Eq)]
pub struct OrdRangeInc(pub RangeInclusive<u64>);

impl OrdRangeInc {
    pub const MIN_SIZE: u64 = 10;

    pub fn new(start: u64, end: u64) -> Self {
        if (end - start) + 1 < OrdRangeInc::MIN_SIZE {
            panic!("Range len cannot be smaller than {}", OrdRangeInc::MIN_SIZE)
        }
        OrdRangeInc(start..=end)
    }

    pub fn new_bound(bound: u64) -> Self {
        OrdRangeInc(bound..=bound)
    }

    pub fn len(&self) -> u64 {
        self.end() - self.start() + 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn start(&self) -> &u64 {
        self.0.start()
    }

    pub fn end(&self) -> &u64 {
        self.0.end()
    }

    pub fn contains(&self, item: &u64) -> bool {
        self.0.contains(item)
    }

    pub fn is_bound(&self) -> bool {
        *self.start() == *self.end()
    }
}

impl From<RangeInclusive<u64>> for OrdRangeInc {
    fn from(range: RangeInclusive<u64>) -> Self {
        OrdRangeInc(range)
    }
}

// Implement Serialize for OrdRangeInc
impl Serialize for OrdRangeInc {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (start, end) = self.0.clone().into_inner();
        let tuple = (start, end);
        tuple.serialize(serializer)
    }
}

// Implement Deserialize for OrdRangeInc
impl<'de> Deserialize<'de> for OrdRangeInc {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (start, end) = <(u64, u64)>::deserialize(deserializer)?;
        Ok(OrdRangeInc::new(start, end))
    }
}

fn check_bound(bound: &u64, range: &OrdRangeInc, inverted: bool) -> Ordering {
    if bound < range.start() {
        if inverted {
            return Ordering::Greater;
        }
        return Ordering::Less;
    }
    if bound > range.end() {
        if inverted {
            return Ordering::Less;
        }
        return Ordering::Greater;
    }
    Ordering::Equal
}

impl Ord for OrdRangeInc {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.is_bound() && other.is_bound() {
            panic!("Trying two reserved OrdRangeInc::bound");
        }
        if self.is_bound() {
            return check_bound(self.start(), other, false);
        }
        if other.is_bound() {
            return check_bound(other.start(), self, true);
        }
        // This works because in OrdRangeInc the ranges should never collide
        // u64 in one range will never exists in another range
        let start_cmp = self.start().cmp(other.start());
        if start_cmp == Ordering::Equal {
            self.end().cmp(other.end())
        } else {
            start_cmp
        }
    }
}

impl PartialOrd for OrdRangeInc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrdRangeInc {
    fn eq(&self, other: &Self) -> bool {
        if self.is_bound() {
            return other.contains(self.start());
        } else if other.is_bound() {
            return self.contains(other.start());
        }
        self.start() == other.start() && self.end() == other.end()
    }
}

impl RangeBounds<u64> for OrdRangeInc {
    fn start_bound(&self) -> std::ops::Bound<&u64> {
        std::ops::Bound::Included(self.start())
    }

    fn end_bound(&self) -> std::ops::Bound<&u64> {
        std::ops::Bound::Included(self.end())
    }
}
