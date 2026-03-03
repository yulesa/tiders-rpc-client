//! Adaptive concurrency controller for single-block RPC calls.
//!
//! A single [`AdaptiveConcurrency`] instance governs concurrency limits and
//! backoff for per-block RPC calls (trace_block, debug_traceBlockByNumber).
//! When any call site hits a rate limit, every call site slows down; when
//! calls succeed consistently, the system gradually scales back up.
//!
//! All state is stored in atomics for lock-free access from concurrent tasks.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use log::info;
use std::sync::LazyLock;
// Re-export for callers that import from this module.
pub use super::shared_helpers::is_rate_limit_error;

/// Default chunk size for single-block RPC calls (e.g. trace_block).
/// Each chunk of blocks is processed as a semaphore-bounded parallel batch,
/// and one `ArrowResponse` is emitted per chunk.
pub const DEFAULT_SINGLE_BLOCK_CHUNK_SIZE: u64 = 200;

/// Global adaptive concurrency controller for single-block RPC calls.
/// The first time any call site reads current() or calls wait_for_backoff(),
/// Lazy runs the closure and constructs the singleton. After that, every
/// subsequent access returns the same instance.
pub static SINGLE_BLOCK_ADAPTIVE_CONCURRENCY: LazyLock<AdaptiveConcurrency> = LazyLock::new(|| {
    AdaptiveConcurrency::new(
        100,  // initial concurrent calls
        10,   // minimum
        2000, // maximum
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
            scale_up_threshold: 50,
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
    ///   by 33%.
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
            // +33%, at least +1.
            let increment = (old / 3).max(1);
            let new = (old + increment).min(self.max);
            self.current.store(new, Ordering::Relaxed);

            info!(
                "single-block adaptive concurrency: scaled up {old} -> {new} after {threshold} consecutive successes",
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
            "single-block adaptive concurrency: rate limit — concurrency {old} -> {new}, backoff {new_backoff}ms",
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

        info!("single-block adaptive concurrency: error — concurrency {old} -> {new}");
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
        Ok(()) => SINGLE_BLOCK_ADAPTIVE_CONCURRENCY.record_success(),
        Err(err_str) => {
            if is_rate_limit_error(err_str) {
                SINGLE_BLOCK_ADAPTIVE_CONCURRENCY.record_rate_limit();
            } else {
                SINGLE_BLOCK_ADAPTIVE_CONCURRENCY.record_error();
            }
        }
    }
}
