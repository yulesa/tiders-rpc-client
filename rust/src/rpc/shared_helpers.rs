//! Shared helper functions used across multiple RPC pipelines.
//!
//! Contains block-range utilities, error classification, and other common
//! helpers that are not specific to any single pipeline (logs, blocks, traces).

/// Clamp `to_block` based on `from_block + max_block_range`, never exceeding
/// `snapshot_to_block`.
pub fn clamp_to_block(
    from_block: u64,
    snapshot_to_block: u64,
    max_block_range: Option<u64>,
) -> u64 {
    if let Some(max) = max_block_range {
        let to = from_block.saturating_add(max.saturating_sub(1));
        to.min(snapshot_to_block)
    } else {
        snapshot_to_block
    }
}

/// Take either the halved block range, or at least advance by 2 blocks.
pub fn halved_block_range(from_block: u64, to_block: u64) -> u64 {
    let range = to_block.saturating_sub(from_block);
    let halved = from_block + range / 2;
    halved.max(from_block + 2)
}

/// Return `true` if the error is fatal and cannot be resolved by changing the
/// block range or retrying.
pub fn is_fatal_error(err_str: &str) -> bool {
    is_fatal_error_lower(&truncate_and_lowercase(err_str, 5000))
}

/// Inner check on an already-lowercased string.
pub(crate) fn is_fatal_error_lower(error_lower: &str) -> bool {
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

/// Returns `true` if the error string indicates a rate-limit response.
pub fn is_rate_limit_error(err_str: &str) -> bool {
    let lower = err_str.to_lowercase();
    lower.contains("429")
        || lower.contains("rate limit")
        || lower.contains("rate-limit")
        || lower.contains("too many requests")
        || lower.contains("request limit")
        || lower.contains("throttle")
}

/// Truncate a string and lowercase it for case-insensitive matching.
pub(crate) fn truncate_and_lowercase(s: &str, max_len: usize) -> String {
    s.chars().take(max_len).collect::<String>().to_lowercase()
}

/// Pick the minimum of `max_block_range` (if set) and `suggested`.
pub(crate) fn pick_min_range(max_block_range: Option<u64>, suggested: u64) -> u64 {
    max_block_range.map_or(suggested, |orig| orig.min(suggested))
}
