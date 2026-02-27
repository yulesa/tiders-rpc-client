//! Adaptive concurrency controller for block pipeline RPC calls.
//!
//! A dedicated [`BlockAdaptiveConcurrency`] instance governs concurrency limits
//! and backoff for **block batch** RPC calls (eth_getBlockByNumber batches).
//! This is separate from the per-block adaptive concurrency used for tx receipts
//! and traces.
//!
//! All state is stored in atomics for lock-free access from concurrent tasks.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use log::{info, warn};
use once_cell::sync::Lazy;
use rand::Rng;

use super::shared_helpers::{
    halved_block_range, is_fatal_error_lower, pick_min_range, truncate_and_lowercase,
};

/// Default chunk size for `eth_getBlockByNumber` batch calls.
/// Each chunk of block numbers is sent as a single JSON-RPC batch request.
pub const DEFAULT_BLOCK_CHUNK_SIZE: u64 = 200;

/// Global adaptive concurrency controller for block pipeline batch calls.
pub static BLOCK_ADAPTIVE_CONCURRENCY: Lazy<BlockAdaptiveConcurrency> = Lazy::new(|| {
    BlockAdaptiveConcurrency::new(
        10,  // initial concurrent calls
        2,   // minimum
        200, // maximum
    )
});

/// Lock-free adaptive concurrency controller for block pipeline.
///
/// Tracks current concurrency level, consecutive successes, and backoff delay
/// using atomics. Concurrency scales up slowly on sustained success and scales
/// down aggressively on rate limits.
pub struct BlockAdaptiveConcurrency {
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
    /// Current chunk size (block range per RPC batch call). Shrinks on errors,
    /// occasionally resets to `original_chunk_size` on success.
    chunk_size: AtomicU64,
    /// The original chunk size to reset to (set from config or default).
    original_chunk_size: AtomicU64,
}

impl BlockAdaptiveConcurrency {
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
            chunk_size: AtomicU64::new(DEFAULT_BLOCK_CHUNK_SIZE),
            original_chunk_size: AtomicU64::new(DEFAULT_BLOCK_CHUNK_SIZE),
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
                "block adaptive concurrency: scaled up {old} -> {new} after {threshold} consecutive successes",
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
            "block adaptive concurrency: rate limit — concurrency {old} -> {new}, backoff {new_backoff}ms",
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

        info!("block adaptive concurrency: error — concurrency {old} -> {new}");
    }

    /// Sleep for the current backoff duration (no-op if backoff is 0).
    pub async fn wait_for_backoff(&self) {
        let ms = self.backoff_ms.load(Ordering::Relaxed);
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }

    /// Return the current chunk size (block range per batch call).
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size.load(Ordering::Relaxed)
    }

    /// Set the original chunk size (from config). Also resets the current
    /// chunk size if it is larger than the new original.
    pub fn set_original_chunk_size(&self, size: u64) {
        self.original_chunk_size.store(size, Ordering::Relaxed);
        self.chunk_size.fetch_min(size, Ordering::Relaxed);
    }

    /// Reduce the chunk size to at most `new_max`.
    pub fn reduce_chunk_size(&self, new_max: u64) {
        self.chunk_size.fetch_min(new_max, Ordering::Relaxed);
    }

    /// With a 10% probability, reset chunk size to the original value.
    pub fn maybe_reset_chunk_size(&self) {
        let mut rng = rand::rng();
        if rng.random_bool(0.10) {
            let original = self.original_chunk_size.load(Ordering::Relaxed);
            self.chunk_size.store(original, Ordering::Relaxed);
        }
    }
}


/// Result of attempting to parse an error for a suggested block range.
#[derive(Debug)]
pub struct RetryBlockRange {
    pub from: u64,
    pub to: u64,
    /// If set, this should become the new `max_block_range` for subsequent
    /// requests.
    pub max_block_range: Option<u64>,
    /// If true, the caller should wait briefly before retrying (transient
    /// network overload rather than a block-range limit).
    pub backoff: bool,
}

/// Attempt to parse an RPC error from a block-fetching call and suggest a
/// smaller block range.
///
/// Returns `None` if the error is not recoverable by reducing the range
/// (i.e. fatal errors).
pub fn retry_block_with_block_range(
    error_message: &str,
    from_block: u64,
    to_block: u64,
    max_block_range: Option<u64>,
) -> Option<RetryBlockRange> {
    warn!("Attempt to parse an RPC block-batch error (blocks {from_block}-{to_block}): {error_message}");
    let error_lower = truncate_and_lowercase(error_message, 5000);

    if is_fatal_error_lower(&error_lower) {
        return None;
    }

    // Fallback: halve the range.
    if to_block > from_block {
        let halved = halved_block_range(from_block, to_block);
        let range = halved.saturating_sub(from_block);
        let suggested = pick_min_range(max_block_range, range);

        return Some(RetryBlockRange {
            from: from_block,
            to: from_block + suggested,
            max_block_range: Some(suggested),
            backoff: false,
        });
    }

    None
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn scale_up_after_threshold() {
        let ac = BlockAdaptiveConcurrency::new(10, 2, 200);
        for _ in 0..10 {
            ac.record_success();
        }
        // 10 + 20% = 12
        assert_eq!(ac.current(), 12);
    }

    #[test]
    fn scale_down_on_rate_limit() {
        let ac = BlockAdaptiveConcurrency::new(10, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.current(), 5);
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn scale_down_on_error() {
        let ac = BlockAdaptiveConcurrency::new(10, 2, 200);
        ac.record_error();
        // 10 - 10% = 9
        assert_eq!(ac.current(), 9);
    }

    #[test]
    fn respects_minimum() {
        let ac = BlockAdaptiveConcurrency::new(2, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.current(), 2);
    }

    #[test]
    fn respects_maximum() {
        let ac = BlockAdaptiveConcurrency::new(190, 2, 200);
        for _ in 0..10 {
            ac.record_success();
        }
        // 190 + 20% = 228, capped at 200
        assert_eq!(ac.current(), 200);
    }

    #[test]
    fn backoff_doubles_on_consecutive_rate_limits() {
        let ac = BlockAdaptiveConcurrency::new(100, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 500);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 1000);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 2000);
    }

    #[test]
    fn backoff_reduces_on_success() {
        let ac = BlockAdaptiveConcurrency::new(10, 2, 200);
        ac.record_rate_limit(); // backoff = 500
        ac.record_success(); // backoff = 500 * 3/4 = 375
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 375);
    }
}
