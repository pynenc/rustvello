#![no_main]
use libfuzzer_sys::fuzz_target;
use rustvello_proto::config::AppConfig;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        // Attempt to parse as TOML AppConfig — should never panic.
        let _ = toml::from_str::<AppConfig>(s);
    }
});
