//! Adaptive concurrency controller for the log pipeline.
//!
//! A dedicated [`LogAdaptiveConcurrency`] instance governs concurrency limits
//! and backoff for **log batch** RPC calls (eth_getLogs batches).
//! This is separate from the block pipeline's adaptive concurrency and from
//! the global per-RPC-call adaptive concurrency.
//!
//! Also contains `retry_logs_with_block_range()` — provider-specific error
//! parsing for block-range errors returned by eth_getLogs calls.
//!
//! All state is stored in atomics for lock-free access from concurrent tasks.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use log::{debug, info, warn};
use rand::Rng;
use regex::Regex;
use std::sync::LazyLock;

use super::shared_helpers::{
    halved_block_range, is_fatal_error_lower, pick_min_range, truncate_and_lowercase,
};

/// Default chunk size for `eth_getLogs` batch calls (block range per chunk).
pub const DEFAULT_LOG_CHUNK_SIZE: u64 = 1000;

/// Global adaptive concurrency controller for log pipeline batch calls.
pub static LOG_ADAPTIVE_CONCURRENCY: LazyLock<LogAdaptiveConcurrency> = LazyLock::new(|| {
    LogAdaptiveConcurrency::new(
        10,  // initial concurrent calls
        2,   // minimum
        200, // maximum
    )
});

/// Lock-free adaptive concurrency controller for the log pipeline.
///
/// Tracks current concurrency level, consecutive successes, and backoff delay
/// using atomics. Concurrency scales up slowly on sustained success and scales
/// down aggressively on rate limits.
pub struct LogAdaptiveConcurrency {
    /// Current concurrency level (number of concurrent chunk fetches allowed).
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
    /// Current chunk size (block range per eth_getLogs call). Shrinks on errors,
    /// occasionally resets to `original_chunk_size` on success.
    chunk_size: AtomicU64,
    /// The original chunk size to reset to (set from config or default).
    original_chunk_size: AtomicU64,
}

impl LogAdaptiveConcurrency {
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
            chunk_size: AtomicU64::new(DEFAULT_LOG_CHUNK_SIZE),
            original_chunk_size: AtomicU64::new(DEFAULT_LOG_CHUNK_SIZE),
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
                "log adaptive concurrency: scaled up {old} -> {new} after {threshold} consecutive successes",
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
            "log adaptive concurrency: rate limit — concurrency {old} -> {new}, backoff {new_backoff}ms",
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

        info!("log adaptive concurrency: error — concurrency {old} -> {new}");
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
            info!("log adaptive concurrency: randomly reset chunk size to original {original}");
        }
    }
}

// ===========================================================================
// Log block-range retry logic
// ===========================================================================

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

/// Attempt to parse an RPC error and suggest a smaller block range.
///
/// Encodes hard-won knowledge about how Alchemy, Infura, QuickNode, Ankr,
/// Base, and other providers report block range errors.
///
/// Returns `None` if the error is not a block-range error.
pub fn retry_logs_with_block_range(
    error_message: &str,
    from_block: u64,
    to_block: u64,
    max_block_range: Option<u64>,
) -> Option<RetryBlockRange> {
    warn!("Attempt to parse an RPC block range error (blocks {from_block}-{to_block}): {error_message}");
    let error_lower = truncate_and_lowercase(error_message, 5000);

    // Fatal errors (connectivity or permanent auth) — no retry.
    if is_fatal_error_lower(&error_lower) {
        return None;
    }

    // --- Alchemy ---
    // "this block range should work: [0x..., 0x...]"
    if let Some(result) = try_parse_hex_range(
        &error_lower,
        r"this block range should work: \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)\]",
        from_block,
        max_block_range,
    ) {
        return Some(result);
    }

    // --- Infura, Thirdweb, zkSync, Tenderly ---
    // "try with this block range [0x..., 0x...]"
    if let Some(result) = try_parse_hex_range(
        &error_lower,
        r"try with this block range \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)\]",
        from_block,
        max_block_range,
    ) {
        return Some(result);
    }

    // --- Ankr ---
    // "block range is too wide"
    if error_lower.contains("block range is too wide") {
        let suggested = pick_min_range(max_block_range, 3000);
        return Some(RetryBlockRange {
            from: from_block,
            to: from_block + suggested,
            max_block_range: Some(suggested),
            backoff: false,
        });
    }

    // --- QuickNode, 1RPC, zkEVM, Blast, BlockPI ---
    // "limited to a 10,000" / "limited to a 10000"
    if let Some(result) = try_parse_limited_to(&error_lower, from_block, max_block_range) {
        return Some(result);
    }

    // --- Base ---
    // "block range too large"
    if error_lower.contains("block range too large") {
        let suggested = pick_min_range(max_block_range, 2000);
        return Some(RetryBlockRange {
            from: from_block,
            to: from_block + suggested,
            max_block_range: Some(suggested),
            backoff: false,
        });
    }

    // --- Transient: response too big / decode error ---
    if error_lower.contains("response is too big")
        || error_lower.contains("error decoding response body")
    {
        return Some(RetryBlockRange {
            from: from_block,
            to: halved_block_range(from_block, to_block),
            max_block_range,
            backoff: false,
        });
    }

    // --- Transient: sending error (overload) ---
    if error_lower.contains("error sending request") {
        return Some(RetryBlockRange {
            from: from_block,
            to: halved_block_range(from_block, to_block),
            max_block_range,
            backoff: true,
        });
    }

    // --- Fallback: tiered block range reduction ---
    if to_block > from_block {
        let diff = to_block - from_block;
        let mut fb = FallbackBlockRange::from_diff(diff);

        let mut next_to = from_block + fb.value();
        if next_to == to_block {
            fb = fb.lower();
            next_to = from_block + fb.value();
        }

        if next_to < from_block {
            return Some(RetryBlockRange {
                from: from_block,
                to: halved_block_range(from_block, to_block),
                max_block_range,
                backoff: false,
            });
        }

        let fallback_val = fb.value();
        let suggested = pick_min_range(max_block_range, fallback_val);

        warn!(
            "Computed fallback block range {suggested} (from diff {diff}). Provider did not give hints."
        );

        return Some(RetryBlockRange {
            from: from_block,
            to: from_block + suggested,
            max_block_range: Some(suggested),
            backoff: false,
        });
    }

    None
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn try_parse_hex_range(
    error_lower: &str,
    pattern: &str,
    from_block: u64,
    max_block_range: Option<u64>,
) -> Option<RetryBlockRange> {
    let re = Regex::new(pattern).ok()?;
    let captures = re.captures(error_lower)?;
    let start_hex = captures.get(1)?.as_str();
    let end_hex = captures.get(2)?.as_str();
    let from = u64::from_str_radix(start_hex, 16).ok()?;
    let to = u64::from_str_radix(end_hex, 16).ok()?;

    if from > to {
        debug!("Provider returned inverted block range {from}..{to}, using from_block..{from}");
        let suggested_range = from.saturating_sub(from_block);
        return Some(RetryBlockRange {
            from: from_block,
            to: from,
            max_block_range: Some(pick_min_range(max_block_range, suggested_range)),
            backoff: false,
        });
    }

    let suggested_range = to.saturating_sub(from);
    Some(RetryBlockRange {
        from,
        to,
        max_block_range: Some(pick_min_range(max_block_range, suggested_range)),
        backoff: false,
    })
}

fn try_parse_limited_to(
    error_lower: &str,
    from_block: u64,
    max_block_range: Option<u64>,
) -> Option<RetryBlockRange> {
    // Matches "limited to a 10,000" or "limited to a 5 range" (QuickNode
    // discover plan). When the word "range" follows, the number is an
    // inclusive block count (to - from + 1), so we subtract 1 to get the
    // exclusive diff used everywhere else.
    let re = Regex::new(r"limited to a ([\d,.]+)(\s+range)?").ok()?;
    let captures = re.captures(error_lower)?;
    let range_str = captures.get(1)?.as_str().replace(['.', ','], "");
    let range: u64 = range_str.parse().ok()?;
    let is_inclusive = captures.get(2).is_some();
    let diff = if is_inclusive {
        range.saturating_sub(1)
    } else {
        range
    };
    let suggested = pick_min_range(max_block_range, diff);
    Some(RetryBlockRange {
        from: from_block,
        to: from_block + suggested,
        max_block_range: Some(suggested),
        backoff: false,
    })
}

/// Tiered fallback block ranges: 5000 → 500 → 75 → 50 → 45 → ... → 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FallbackBlockRange {
    Range5000,
    Range500,
    Range75,
    Range50,
    Range45,
    Range40,
    Range35,
    Range30,
    Range25,
    Range20,
    Range15,
    Range10,
    Range5,
    Range1,
}

impl FallbackBlockRange {
    fn value(self) -> u64 {
        match self {
            Self::Range5000 => 5000,
            Self::Range500 => 500,
            Self::Range75 => 75,
            Self::Range50 => 50,
            Self::Range45 => 45,
            Self::Range40 => 40,
            Self::Range35 => 35,
            Self::Range30 => 30,
            Self::Range25 => 25,
            Self::Range20 => 20,
            Self::Range15 => 15,
            Self::Range10 => 10,
            Self::Range5 => 5,
            Self::Range1 => 1,
        }
    }

    fn lower(self) -> Self {
        match self {
            Self::Range5000 => Self::Range500,
            Self::Range500 => Self::Range75,
            Self::Range75 => Self::Range50,
            Self::Range50 => Self::Range45,
            Self::Range45 => Self::Range40,
            Self::Range40 => Self::Range35,
            Self::Range35 => Self::Range30,
            Self::Range30 => Self::Range25,
            Self::Range25 => Self::Range20,
            Self::Range20 => Self::Range15,
            Self::Range15 => Self::Range10,
            Self::Range10 => Self::Range5,
            Self::Range5 | Self::Range1 => Self::Range1,
        }
    }

    fn from_diff(diff: u64) -> Self {
        if diff >= 5000 {
            Self::Range5000
        } else if diff >= 500 {
            Self::Range500
        } else if diff >= 75 {
            Self::Range75
        } else if diff >= 50 {
            Self::Range50
        } else if diff >= 45 {
            Self::Range45
        } else if diff >= 40 {
            Self::Range40
        } else if diff >= 35 {
            Self::Range35
        } else if diff >= 30 {
            Self::Range30
        } else if diff >= 25 {
            Self::Range25
        } else if diff >= 20 {
            Self::Range20
        } else if diff >= 15 {
            Self::Range15
        } else if diff >= 10 {
            Self::Range10
        } else if diff >= 5 {
            Self::Range5
        } else {
            Self::Range1
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::super::shared_helpers::is_fatal_error;
    use super::*;

    #[test]
    fn scale_up_after_threshold() {
        let ac = LogAdaptiveConcurrency::new(10, 2, 200);
        for _ in 0..10 {
            ac.record_success();
        }
        assert_eq!(ac.current(), 12);
    }

    #[test]
    fn scale_down_on_rate_limit() {
        let ac = LogAdaptiveConcurrency::new(10, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.current(), 5);
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn scale_down_on_error() {
        let ac = LogAdaptiveConcurrency::new(10, 2, 200);
        ac.record_error();
        assert_eq!(ac.current(), 9);
    }

    #[test]
    fn respects_minimum() {
        let ac = LogAdaptiveConcurrency::new(2, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.current(), 2);
    }

    #[test]
    fn respects_maximum() {
        let ac = LogAdaptiveConcurrency::new(190, 2, 200);
        for _ in 0..10 {
            ac.record_success();
        }
        assert_eq!(ac.current(), 200);
    }

    #[test]
    fn backoff_doubles_on_consecutive_rate_limits() {
        let ac = LogAdaptiveConcurrency::new(100, 2, 200);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 500);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 1000);
        ac.record_rate_limit();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 2000);
    }

    #[test]
    fn backoff_reduces_on_success() {
        let ac = LogAdaptiveConcurrency::new(10, 2, 200);
        ac.record_rate_limit();
        ac.record_success();
        assert_eq!(ac.backoff_ms.load(Ordering::Relaxed), 375);
    }

    #[test]
    fn alchemy_error_parsing() {
        let msg = "this block range should work: [0x100, 0x200]";
        let result = retry_logs_with_block_range(msg, 100, 1000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(0x100));
        assert_eq!(r.map(|r| r.to), Some(0x200));
        assert_eq!(r.and_then(|r| r.max_block_range), Some(256));
    }

    #[test]
    fn infura_error_parsing() {
        let msg = "try with this block range [0xa, 0xff]";
        let result = retry_logs_with_block_range(msg, 5, 1000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(10));
        assert_eq!(r.map(|r| r.to), Some(255));
        assert_eq!(r.and_then(|r| r.max_block_range), Some(245));
    }

    #[test]
    fn ankr_error_parsing() {
        let msg = "block range is too wide";
        let result = retry_logs_with_block_range(msg, 100, 10000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(100));
        assert_eq!(r.map(|r| r.to), Some(3100));
        assert_eq!(r.and_then(|r| r.max_block_range), Some(3000));
    }

    #[test]
    fn quicknode_limited_to_parsing() {
        let msg = "limited to a 10,000";
        let result = retry_logs_with_block_range(msg, 100, 50000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(100));
        assert_eq!(r.map(|r| r.to), Some(10100));
    }

    #[test]
    fn quicknode_discover_plan_parsing() {
        // "eth_getLogs is limited to a 5 range" — inclusive count, so diff = 4.
        let msg = r#"eth_getLogs failed: HTTP error 413 with body: {"jsonrpc":"2.0","id":525,"error":{"code":-32615,"message":"eth_getLogs is limited to a 5 range, upgrade from discover plan at https://dashboard.quicknode.com/billing/plan to increase the limit"}}"#;
        let result = retry_logs_with_block_range(msg, 18000078, 18000099, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(18000078));
        assert_eq!(r.map(|r| r.to), Some(18000082)); // from + 4
        assert_eq!(r.and_then(|r| r.max_block_range), Some(4));
    }

    #[test]
    fn base_error_parsing() {
        let msg = "block range too large";
        let result = retry_logs_with_block_range(msg, 100, 10000, Some(1500));
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.to), Some(1600));
    }

    #[test]
    fn fallback_range_from_large_diff() {
        let msg = "some unknown error";
        let result = retry_logs_with_block_range(msg, 0, 10000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.and_then(|r| r.max_block_range), Some(5000));
    }

    #[test]
    fn fatal_errors_are_not_retried() {
        assert!(is_fatal_error("Connection refused"));
        assert!(is_fatal_error("No such host"));
        assert!(is_fatal_error(
            "server returned an error response: error code -32053: API key is not allowed to access method"
        ));
        assert!(is_fatal_error("Unauthorized"));
        assert!(is_fatal_error("Access Denied"));
        assert!(is_fatal_error("403 Forbidden"));
        assert!(is_fatal_error("method not found"));
        let result = retry_logs_with_block_range(
            "error code -32053: API key is not allowed to access method",
            100,
            200,
            None,
        );
        assert!(result.is_none(), "fatal error should not produce a retry");
    }
}
