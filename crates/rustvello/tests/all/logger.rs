use std::sync::Once;

static INIT: Once = Once::new();

/// Initializes the logger, ensuring it's only set up once.
pub fn init_logger() {
    INIT.call_once(|| {
        env_logger::builder()
            .is_test(true) // Only display logs when running tests
            .filter_level(log::LevelFilter::Debug) // Adjust log level as needed
            .init();
    });
}
