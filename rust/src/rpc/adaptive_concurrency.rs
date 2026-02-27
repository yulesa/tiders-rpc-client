//! Global adaptive concurrency controller for RPC calls.
//!
//! A single [`AdaptiveConcurrency`] instance governs concurrency limits and
//! backoff across **all** RPC call sites. When any call site hits a rate limit,
//! every call site slows down; when calls succeed consistently, the system
//! gradually scales back up.
//!
//! All state is stored in atomics for lock-free access from concurrent tasks.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use log::info;
use once_cell::sync::Lazy;
// Re-export for callers that import from this module.
pub use super::shared_helpers::is_rate_limit_error;

/// Global adaptive concurrency controller shared across all RPC call sites.
/// The first time any call site reads current() or calls wait_for_backoff(),
/// Lazy runs the closure and constructs the singleton. After that, every
/// subsequent access returns the same instance.
pub static ADAPTIVE_CONCURRENCY: Lazy<AdaptiveConcurrency> = Lazy::new(|| {
    AdaptiveConcurrency::new(
        10,  // initial concurrent calls
        2,   // minimum
        200, // maximum
    )
});

/// Lock-free adaptive concurrency controller.
///
/// Tracks current concurrency level, consecutive successes, and backoff delay
/// using atomics. Concurrency scales up slowly on sustained success and scales
/// down aggressively on rate limits.
pub struct AdaptiveConcurrency {
    /// Current concurrency level (number of concurrent RPC calls allowed).
    current: AtomicUsize,
    /// Minimum concurrency level — never goes below this.
    min: usize,
    /// Maximum concurrency level — never exceeds this.
    max: usize,
    /// Number of consecutive successes since the last error/rate-limit.
    consecutive_successes: AtomicUsize,
    /// Current backoff delay in milliseconds (0 = no backoff).
    backoff_ms: AtomicU64,
    /// Number of consecutive successes required before scaling up.
    scale_up_threshold: usize,
}

impl AdaptiveConcurrency {
    /// Create a new controller with the given initial, minimum, and maximum
    /// concurrency levels.
    fn new(initial: usize, min: usize, max: usize) -> Self {
        Self {
            current: AtomicUsize::new(initial),
            min,
            max,
            consecutive_successes: AtomicUsize::new(0),
            backoff_ms: AtomicU64::new(0),
            scale_up_threshold: 10,
        }
    }

    /// Return the current concurrency level.
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Record a successful RPC call.
    ///
    /// - Reduces backoff by 25% (multiply by 3/4).
    /// - After `scale_up_threshold` consecutive successes, increases concurrency
    ///   by 20%.
    pub fn record_success(&self) {
        // Reduce backoff by 25%.
        let old_backoff = self.backoff_ms.load(Ordering::Relaxed);
        if old_backoff > 0 {
            let new_backoff = old_backoff * 3 / 4;
            // Best-effort CAS — if another thread changed it, that's fine.
            let _ = self.backoff_ms.compare_exchange(
                old_backoff,
                new_backoff,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }

        let prev = self.consecutive_successes.fetch_add(1, Ordering::Relaxed);
        if prev + 1 >= self.scale_up_threshold {
            self.consecutive_successes.store(0, Ordering::Relaxed);

            let old = self.current.load(Ordering::Relaxed);
            // +20%, at least +1.
            let increment = (old / 5).max(1);
            let new = (old + increment).min(self.max);
            self.current.store(new, Ordering::Relaxed);

            info!(
                "adaptive concurrency: scaled up {old} -> {new} after {threshold} consecutive successes",
                threshold = self.scale_up_threshold,
            );
        }
    }

    /// Record a rate-limit response (HTTP 429 or provider rate-limit message).
    ///
    /// - Resets consecutive success counter.
    /// - Doubles backoff (starting from 500 ms, capped at 30 s).
    /// - Halves concurrency.
    pub fn record_rate_limit(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);

        // Double backoff, starting from 500 ms.
        let old_backoff = self.backoff_ms.load(Ordering::Relaxed);
        let new_backoff = if old_backoff == 0 {
            500
        } else {
            (old_backoff * 2).min(30_000)
        };
        self.backoff_ms.store(new_backoff, Ordering::Relaxed);

        // Halve concurrency.
        let old = self.current.load(Ordering::Relaxed);
        let new = (old / 2).max(self.min);
        self.current.store(new, Ordering::Relaxed);

        info!(
            "adaptive concurrency: rate limit — concurrency {old} -> {new}, backoff {new_backoff}ms",
        );
    }

    /// Record a general (non-rate-limit) error.
    ///
    /// - Resets consecutive success counter.
    /// - Reduces concurrency by 10% (gentler than rate limit).
    pub fn record_error(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);

        let old = self.current.load(Ordering::Relaxed);
        // -10%, at least -1.
        let decrement = (old / 10).max(1);
        let new = old.saturating_sub(decrement).max(self.min);
        self.current.store(new, Ordering::Relaxed);

        info!("adaptive concurrency: error — concurrency {old} -> {new}");
    }

    /// Sleep for the current backoff duration (no-op if backoff is 0).
    pub async fn wait_for_backoff(&self) {
        let ms = self.backoff_ms.load(Ordering::Relaxed);
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }
}

/// Report an RPC outcome to the adaptive concurrency controller.
///
/// Call this after every RPC call to feed success/error signals.
pub fn report_rpc_outcome(result: &Result<(), &str>) {
    match result {
        Ok(()) => ADAPTIVE_CONCURRENCY.record_success(),
        Err(err_str) => {
            if is_rate_limit_error(err_str) {
                ADAPTIVE_CONCURRENCY.record_rate_limit();
            } else {
                ADAPTIVE_CONCURRENCY.record_error();
            }
        }
    }
}


#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn scale_up_after_threshold() {
        let ac = AdaptiveConcurrency::new(10, 2, 200);
        for _ in 0..10 {
            ac.record_success();
        }
        // 10 + 20% = 12
        assert_eq!(ac.current(), 12);
    }

    #[test]
    fn scale_down_on_rate_limit() {
        let ac = AdaptiveConcurrency::new(10, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.current(), 5);
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn scale_down_on_error() {
        let ac = AdaptiveConcurrency::new(10, 2, 200);
        ac.record_error();
        // 10 - 10% = 9
        assert_eq!(ac.current(), 9);
    }

    #[test]
    fn respects_minimum() {
        let ac = AdaptiveConcurrency::new(2, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.current(), 2);
    }

    #[test]
    fn respects_maximum() {
        let ac = AdaptiveConcurrency::new(190, 2, 200);
        for _ in 0..10 {
            ac.record_success();
        }
        // 190 + 20% = 228, capped at 200
        assert_eq!(ac.current(), 200);
    }

    #[test]
    fn backoff_doubles_on_consecutive_rate_limits() {
        let ac = AdaptiveConcurrency::new(100, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 500);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 1000);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 2000);
    }

    #[test]
    fn backoff_reduces_on_success() {
        let ac = AdaptiveConcurrency::new(10, 2, 200);
        ac.record_rate_limit(); // backoff = 500
        ac.record_success(); // backoff = 500 * 3/4 = 375
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 375);
    }
}
