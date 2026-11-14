//! Ported from github.com/grafana/mimir/pkg/mimirpb/fnv.go and series_sharding.go

pub const FNV_OFFSET_32: u32 = 2_166_136_261;
pub const FNV_PRIME_32: u32 = 16_777_619;

/// Initializes a new FNV-1 32-bit hash value (`HashNew32` in `fnv.go`).
#[inline]
pub const fn hash_new_32() -> u32 {
    FNV_OFFSET_32
}

/// Adds bytes/string to an FNV-1 32-bit hash value (`HashAdd32` in `fnv.go`).
///
/// Note: This is FNV-1 (multiply THEN xor), matching Go `mimirpb.HashAdd32`.
#[inline]
pub fn hash_add_32(mut h: u32, s: impl AsRef<[u8]>) -> u32 {
    for &b in s.as_ref() {
        h = h.wrapping_mul(FNV_PRIME_32);
        h ^= b as u32;
    }
    h
}

/// Adds a single byte to an FNV-1 32-bit hash value (`HashAddByte32` in `fnv.go`).
#[inline]
pub const fn hash_add_byte_32(h: u32, b: u8) -> u32 {
    h.wrapping_mul(FNV_PRIME_32) ^ (b as u32)
}

/// Returns the shard token for `user_id` (`ShardByUser` in `series_sharding.go`).
#[inline]
pub fn shard_by_user(user_id: &str) -> u32 {
    hash_add_32(hash_new_32(), user_id)
}

/// Returns the shard token for `(user_id, metric_name)` (`ShardByMetricName` in `series_sharding.go`).
#[inline]
pub fn shard_by_metric_name(user_id: &str, metric_name: &str) -> u32 {
    hash_add_32(shard_by_user(user_id), metric_name)
}

/// Returns the shard token for `(user_id, labels)` (`ShardByAllLabels` in `series_sharding.go`).
///
/// Labels are hashed in iteration order (callers must pass sorted labels for canonical hashing,
/// matching `labels.Labels.Range`).
#[inline]
pub fn shard_by_all_labels<I, N, V>(user_id: &str, labels: I) -> u32
where
    I: IntoIterator<Item = (N, V)>,
    N: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    let mut h = shard_by_user(user_id);
    for (name, value) in labels {
        h = hash_add_32(h, name.as_ref());
        h = hash_add_32(h, value.as_ref());
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream: pkg/mimirpb/series_sharding_test.go::TestShardByAllLabelAdaptersReturnsWrongResultsForUnsortedLabels
    #[test]
    fn test_shard_by_all_labels_order_sensitive() {
        let val1 = shard_by_all_labels("test", [("__name__", "foo"), ("bar", "baz"), ("sample", "1")]);
        let val2 = shard_by_all_labels("test", [("__name__", "foo"), ("sample", "1"), ("bar", "baz")]);
        assert_ne!(val1, val2);
    }

    #[test]
    fn test_fnv1_known_values() {
        assert_eq!(hash_new_32(), 2_166_136_261);
        let h = hash_add_byte_32(hash_new_32(), b'a');
        assert_eq!(h, 2_166_136_261u32.wrapping_mul(16_777_619) ^ (b'a' as u32));
        assert_eq!(hash_add_32(hash_new_32(), "a"), h);
    }
}
