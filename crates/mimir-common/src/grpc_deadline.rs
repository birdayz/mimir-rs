//! Server-side gRPC request deadlines (the `grpc-timeout` request header).
//!
//! Ported from:
//! - google.golang.org/grpc/internal/transport/http_util.go (`decodeTimeout`)
//! - google.golang.org/grpc/internal/transport/http2_server.go (`operateHeaders`: the stream
//!   context gets `context.WithTimeout(ctx, timeout)`; malformed values abort the stream with
//!   `codes.Internal` "malformed grpc-timeout: ...")
//!
//! In Go handlers observe the deadline through their `context.Context`. Here the gRPC server runs
//! each RPC's handler future inside [`scope`], so code waiting on external progress (e.g. strong
//! read consistency) can bound its wait with [`current`] and fail like `ctx.Err()` would.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;

tokio::task_local! {
    static DEADLINE: Instant;
}

/// Go `decodeTimeout`: `<1..8 digits><unit>` with units `H`, `M`, `S`, `m`, `u`, `n`.
pub fn decode_timeout(s: &str) -> Result<Duration, String> {
    let size = s.len();
    if size < 2 {
        return Err(format!("transport: timeout string is too short: {s:?}"));
    }
    if size > 9 {
        // Spec allows for 8 digits plus the unit.
        return Err(format!("transport: timeout string is too long: {s:?}"));
    }
    let (digits, unit) = s.split_at(size - 1);
    let unit_nanos: u128 = match unit {
        "H" => 3_600_000_000_000,
        "M" => 60_000_000_000,
        "S" => 1_000_000_000,
        "m" => 1_000_000,
        "u" => 1_000,
        "n" => 1,
        _ => return Err(format!("transport: timeout unit is not recognized: {s:?}")),
    };
    // strconv.ParseUint(digits, 10, 64): ASCII digits only, no sign.
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.ParseUint: parsing {digits:?}: invalid syntax"));
    }
    let n = u128::from(
        digits
            .parse::<u64>()
            .map_err(|_| format!("strconv.ParseUint: parsing {digits:?}: value out of range"))?,
    );
    // Clamped to time.Duration(math.MaxInt64).
    let nanos = (n * unit_nanos).min(i64::MAX as u128);
    Ok(Duration::from_nanos(nanos as u64))
}

/// Runs `fut` with `deadline` visible to [`current`].
pub async fn scope<F: Future>(deadline: Instant, fut: F) -> F::Output {
    DEADLINE.scope(deadline, fut).await
}

/// The deadline of the gRPC request being served by the current task, if any.
pub fn current() -> Option<Instant> {
    DEADLINE.try_with(|d| *d).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_timeout_matches_grpc_go() {
        // Cases from grpc-go internal/transport/http_util_test.go TestDecodeTimeout.
        assert!(decode_timeout("").is_err());
        assert!(decode_timeout("m").is_err());
        assert_eq!(decode_timeout("0m"), Ok(Duration::ZERO));
        assert_eq!(decode_timeout("1n"), Ok(Duration::from_nanos(1)));
        assert_eq!(decode_timeout("1u"), Ok(Duration::from_micros(1)));
        assert_eq!(decode_timeout("1m"), Ok(Duration::from_millis(1)));
        assert_eq!(decode_timeout("1S"), Ok(Duration::from_secs(1)));
        assert_eq!(decode_timeout("2M"), Ok(Duration::from_secs(120)));
        assert_eq!(decode_timeout("1H"), Ok(Duration::from_secs(3600)));
        assert_eq!(decode_timeout("99999999H"), Ok(Duration::from_nanos(i64::MAX as u64)));
        assert_eq!(
            decode_timeout("1000000000S"),
            Err("transport: timeout string is too long: \"1000000000S\"".to_string())
        );
        assert_eq!(
            decode_timeout("1x"),
            Err("transport: timeout unit is not recognized: \"1x\"".to_string())
        );
        assert_eq!(
            decode_timeout("-1S"),
            Err("strconv.ParseUint: parsing \"-1\": invalid syntax".to_string())
        );
        assert_eq!(
            decode_timeout("1"),
            Err("transport: timeout string is too short: \"1\"".to_string())
        );
    }

    #[tokio::test]
    async fn scope_exposes_deadline() {
        assert_eq!(current(), None);
        let d = Instant::now() + Duration::from_secs(5);
        assert_eq!(scope(d, async { current() }).await, Some(d));
    }
}
