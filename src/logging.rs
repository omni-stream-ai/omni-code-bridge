use std::sync::OnceLock;

static DEBUG_LOGGING: OnceLock<bool> = OnceLock::new();

pub fn init(enabled: bool) {
    let env_enabled = std::env::var("OMNI_CODE_DEBUG")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let _ = DEBUG_LOGGING.set(enabled || env_enabled);
}

pub fn enabled() -> bool {
    *DEBUG_LOGGING.get().unwrap_or(&false)
}

#[macro_export]
macro_rules! debug_log {
    ($($arg:tt)*) => {
        if crate::logging::enabled() {
            eprintln!($($arg)*);
        }
    };
}
