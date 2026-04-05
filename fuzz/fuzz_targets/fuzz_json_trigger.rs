#![no_main]
use libfuzzer_sys::fuzz_target;
use rustvello_proto::trigger::TriggerCondition;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        // Attempt to parse as a TriggerCondition — should never panic.
        let _ = serde_json::from_str::<TriggerCondition>(s);
    }
});
