//! Block range management and provider-specific error recovery.
//!
//! Ported from rindexer's `retry_with_block_range()` (fetch_logs.rs:725-931),
//! `FallbackBlockRange` (fetch_logs.rs:933-1022), `calculate_process_historic_log_to_block()`
//! (fetch_logs.rs:1024-1039), and `halved_block_number()` (helpers/evm_log.rs:141-144).

use log::{debug, warn};
use regex::Regex;

/// Result of attempting to parse an error for a suggested block range.
#[derive(Debug)]
pub struct RetryBlockRange {
    pub from: u64,
    pub to: u64,
    /// If set, this should become the new `max_block_range` for subsequent
    /// requests. Only populated when the provider does NOT give block range
    /// hints (i.e. the slow-provider path).
    pub max_block_range: Option<u64>,
    /// If true, the caller should wait briefly before retrying (transient
    /// network overload rather than a block-range limit).
    pub backoff: bool,
}

/// Clamp `to_block` based on `from_block + max_block_range`, never exceeding
/// `snapshot_to_block`.
///
/// Adapted from rindexer's `calculate_process_historic_log_to_block()`.
pub fn clamp_to_block(
    from_block: u64,
    snapshot_to_block: u64,
    max_block_range: Option<u64>,
) -> u64 {
    if let Some(max) = max_block_range {
        let to = from_block.saturating_add(max);
        to.min(snapshot_to_block)
    } else {
        snapshot_to_block
    }
}

/// Take either the halved block range, or at least advance by 2 blocks.
///
/// Ported from rindexer's `halved_block_number()`.
pub fn halved_block_range(from_block: u64, to_block: u64) -> u64 {
    let range = to_block.saturating_sub(from_block);
    let halved = from_block + range / 2;
    halved.max(from_block + 2)
}

/// Attempt to parse an RPC error and suggest a smaller block range.
///
/// Ported from rindexer's `retry_with_block_range()` — encodes hard-won
/// knowledge about how Alchemy, Infura, QuickNode, Ankr, Base, and other
/// providers report block range errors.
///
/// Returns `None` if the error is not a block-range error.
pub fn retry_with_block_range(
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
    // Sleep before retrying to avoid hammering an overloaded provider.
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

/// Return `true` if the error is fatal and cannot be resolved by changing the
/// block range or retrying.
///
/// Used by all stream error handlers to propagate immediately instead of
/// entering the block-range reduction loop.
pub fn is_fatal_error(err_str: &str) -> bool {
    is_fatal_error_lower(&truncate_and_lowercase(err_str, 5000))
}

/// Inner check on an already-lowercased string, used by `retry_with_block_range`.
fn is_fatal_error_lower(error_lower: &str) -> bool {
    error_lower.contains("connection refused")
        || error_lower.contains("no such host")
        || error_lower.contains("failed to lookup")
        || error_lower.contains("api key is not allowed")
        || error_lower.contains("not allowed to access method")
        || error_lower.contains("unauthorized")
        || error_lower.contains("authentication failed")
        || error_lower.contains("invalid api key")
        || error_lower.contains("access denied")
        || error_lower.contains("403 forbidden")
        || error_lower.contains("method not supported")
        || error_lower.contains("method not found")
        || error_lower.contains("not supported by this provider")
}

fn truncate_and_lowercase(s: &str, max_len: usize) -> String {
    s.chars().take(max_len).collect::<String>().to_lowercase()
}

fn pick_min_range(max_block_range: Option<u64>, suggested: u64) -> u64 {
    max_block_range.map_or(suggested, |orig| orig.min(suggested))
}

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
    let re = Regex::new(r"limited to a ([\d,.]+)").ok()?;
    let captures = re.captures(error_lower)?;
    let range_str = captures.get(1)?.as_str().replace(['.', ','], "");
    let range: u64 = range_str.parse().ok()?;
    let suggested = pick_min_range(max_block_range, range);
    Some(RetryBlockRange {
        from: from_block,
        to: from_block + suggested,
        max_block_range: Some(suggested),
        backoff: false,
    })
}

/// Tiered fallback block ranges: 5000 → 500 → 75 → 50 → 45 → ... → 1.
///
/// Ported from rindexer's `FallbackBlockRange`.
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
mod tests {
    use super::*;

    #[test]
    fn clamp_to_block_with_max_range() {
        assert_eq!(clamp_to_block(100, 500, Some(50)), 150);
        assert_eq!(clamp_to_block(100, 120, Some(50)), 120);
        assert_eq!(clamp_to_block(100, 500, None), 500);
    }

    #[test]
    fn halved_block_range_always_advances() {
        // Normal halving
        assert_eq!(halved_block_range(100, 200), 150);
        // Small range — at least advance by 2
        assert_eq!(halved_block_range(100, 101), 102);
        assert_eq!(halved_block_range(100, 100), 102);
    }

    #[test]
    fn alchemy_error_parsing() {
        // 0x100=256, 0x200=512, range=256 → max_block_range becomes 256
        let msg = "this block range should work: [0x100, 0x200]";
        let result = retry_with_block_range(msg, 100, 1000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(0x100));
        assert_eq!(r.map(|r| r.to), Some(0x200));
        assert_eq!(r.and_then(|r| r.max_block_range), Some(256));
    }

    #[test]
    fn infura_error_parsing() {
        // from=10, to=255, range=245 → max_block_range becomes 245
        let msg = "try with this block range [0xa, 0xff]";
        let result = retry_with_block_range(msg, 5, 1000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(10));
        assert_eq!(r.map(|r| r.to), Some(255));
        assert_eq!(r.and_then(|r| r.max_block_range), Some(245));
    }

    #[test]
    fn ankr_error_parsing() {
        let msg = "block range is too wide";
        let result = retry_with_block_range(msg, 100, 10000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(100));
        assert_eq!(r.map(|r| r.to), Some(3100));
        assert_eq!(r.and_then(|r| r.max_block_range), Some(3000));
    }

    #[test]
    fn quicknode_limited_to_parsing() {
        let msg = "limited to a 10,000";
        let result = retry_with_block_range(msg, 100, 50000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.map(|r| r.from), Some(100));
        assert_eq!(r.map(|r| r.to), Some(10100));
    }

    #[test]
    fn base_error_parsing() {
        let msg = "block range too large";
        let result = retry_with_block_range(msg, 100, 10000, Some(1500));
        let r = result.as_ref();
        assert!(r.is_some());
        // pick_min_range(Some(1500), 2000) => 1500
        assert_eq!(r.map(|r| r.to), Some(1600));
    }

    #[test]
    fn fallback_range_from_large_diff() {
        let msg = "some unknown error";
        let result = retry_with_block_range(msg, 0, 10000, None);
        let r = result.as_ref();
        assert!(r.is_some());
        assert_eq!(r.and_then(|r| r.max_block_range), Some(5000));
    }

    #[test]
    fn fatal_errors_are_not_retried() {
        // Connectivity
        assert!(is_fatal_error("Connection refused"));
        assert!(is_fatal_error("No such host"));
        // Ankr -32053
        assert!(is_fatal_error(
            "server returned an error response: error code -32053: API key is not allowed to access method"
        ));
        // Generic auth errors
        assert!(is_fatal_error("Unauthorized"));
        assert!(is_fatal_error("Access Denied"));
        assert!(is_fatal_error("403 Forbidden"));
        assert!(is_fatal_error("method not found"));
        // Fatal errors return None from retry_with_block_range (no retry).
        let result = retry_with_block_range(
            "error code -32053: API key is not allowed to access method",
            100,
            200,
            None,
        );
        assert!(result.is_none(), "fatal error should not produce a retry");
    }
}
