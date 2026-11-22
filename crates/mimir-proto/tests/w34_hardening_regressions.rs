//! Wave 34 deep-audit hardening regression tests for `mimir-proto`.

use std::io::Write;
use std::sync::Arc;

use mimir_proto::cortexpb;
use mimir_proto::prost::encoding::encode_varint;
use mimir_proto::prost::Message;
use mimir_proto::s2::{
    self, S2Error, Writer, WriterOptions, MIN_BLOCK_SIZE,
};
use mimir_proto::snappy::{
    compress_framed, decode_grpc_frame, decode_grpc_frame_bounded, encode_grpc_frame,
    DecompressLimitExceeded,
};
use mimir_proto::symbols::{V2_COMMON_SYMBOLS_LIST, V2_RECORD_SYMBOL_OFFSET};
use mimir_proto::zerocopy::{
    decode_rw2_stream, decode_rw2_stream_with_limit, decode_write_request,
    decode_write_request_with_limit, DecodeError, DecodeOptions, Rw2ResolvedScratch,
    WriteRequestScratch,
};

fn push_bytes_field(buf: &mut Vec<u8>, field_num: u32, payload: &[u8]) {
    encode_varint(((field_num as u64) << 3) | 2, buf);
    encode_varint(payload.len() as u64, buf);
    buf.extend_from_slice(payload);
}

fn push_varint_field(buf: &mut Vec<u8>, field_num: u32, val: u64) {
    encode_varint((field_num as u64) << 3, buf);
    encode_varint(val, buf);
}

fn encode_sample(value: f64, timestamp_ms: i64) -> Vec<u8> {
    cortexpb::Sample {
        value,
        timestamp_ms,
    }
    .encode_to_vec()
}

fn encode_meta_rw2(metric_type: i32, help_ref: u32, unit_ref: u32) -> Vec<u8> {
    let mut b = Vec::new();
    if metric_type != 0 {
        push_varint_field(&mut b, 1, metric_type as u64);
    }
    if help_ref != 0 {
        push_varint_field(&mut b, 3, help_ref as u64);
    }
    if unit_ref != 0 {
        push_varint_field(&mut b, 4, unit_ref as u64);
    }
    b
}

#[test]
fn w34_rw2_lossy_utf8_cached_once_per_symbol_and_miri_safe() {
    // One 8 KiB invalid-UTF8 symbol referenced 100 times by metadata & labels.
    // Without per-symbol caching, 100 * 8 KiB * 2 fields = ~1.6 MiB > 64 KiB cap.
    // With caching, the 8 KiB symbol is converted once (~16 KiB total) and fits inside 64 KiB.
    let mut bad_sym = vec![b'm'; 8 * 1024];
    bad_sym[0] = 0xff;
    bad_sym[1] = 0xfe;

    let mut wire = Vec::new();
    push_bytes_field(&mut wire, 4, b"");
    push_bytes_field(&mut wire, 4, b"__name__");
    push_bytes_field(&mut wire, 4, &bad_sym); // sym 2 (invalid UTF-8)

    for i in 0..100u32 {
        let sym_name = format!("metric_{i}");
        push_bytes_field(&mut wire, 4, sym_name.as_bytes());
        let name_idx = 3 + i;

        let mut ts = Vec::new();
        let mut packed = Vec::new();
        encode_varint(1, &mut packed); // "__name__"
        encode_varint(name_idx as u64, &mut packed);
        push_bytes_field(&mut ts, 1, &packed);
        push_bytes_field(&mut ts, 2, &encode_sample(1.0, 1000));
        // Metadata references invalid-UTF8 sym 2 for help_ref AND unit_ref on every series.
        push_bytes_field(&mut ts, 5, &encode_meta_rw2(1, 2, 2));
        push_bytes_field(&mut wire, 5, &ts);
    }

    // Streaming decoder: 64 KiB budget succeeds because lossy sym 2 is cached once.
    let mut scratch = WriteRequestScratch::new();
    let mut resolved = Rw2ResolvedScratch::new();
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();
    let mut count = 0usize;
    {
        let mut meta_out = Vec::new();
        decode_rw2_stream_with_limit(
            &wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta_out,
            64 * 1024,
            |_| count += 1,
        )
        .expect("cached per-symbol lossy UTF-8 must fit well within 64 KiB");
        assert_eq!(count, 100);
        assert_eq!(meta_out.len(), 100);
        assert!(meta_out[0].help.starts_with("\u{FFFD}\u{FFFD}mmmm"));
        assert_eq!(meta_out[0].help, meta_out[99].help);
        assert_eq!(meta_out[0].unit, meta_out[99].unit);
    }

    // Non-streaming decoder: also caches once per symbol within 64 KiB cap.
    let dec = decode_write_request_with_limit(&wire, DecodeOptions::rw2(), &mut scratch, 64 * 1024)
        .expect("non-streaming RW2 must also cache lossy UTF-8 per symbol");
    assert_eq!(dec.timeseries_len(), 100);
    assert_eq!(dec.metadata_len(), 100);
    let metas: Vec<_> = dec.metadata().collect();
    assert_eq!(metas[0].help, metas[99].help);

    // Multiple distinct invalid-UTF8 symbols force lossy_utf8_arena growth/reallocation
    // without Stacked-Borrows UB when materializing meta_out.
    let mut multi_wire = Vec::new();
    push_bytes_field(&mut multi_wire, 4, b"");
    push_bytes_field(&mut multi_wire, 4, b"__name__");
    for i in 0..32u8 {
        let s = [b'x', b'0' + (i / 10), b'0' + (i % 10), 0xff, 0xfe];
        push_bytes_field(&mut multi_wire, 4, &s);
        let sym_idx = 2 + i as u64;
        let mut ts = Vec::new();
        let mut packed = Vec::new();
        encode_varint(1, &mut packed);
        encode_varint(sym_idx, &mut packed);
        push_bytes_field(&mut ts, 1, &packed);
        push_bytes_field(&mut ts, 5, &encode_meta_rw2(2, sym_idx as u32, 0));
        push_bytes_field(&mut multi_wire, 5, &ts);
    }
    label_bytes.clear();
    samples.clear();
    {
        let mut meta_out = Vec::new();
        decode_rw2_stream(
            &multi_wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta_out,
            |_| {},
        )
        .expect("multiple invalid-UTF8 symbols must decode cleanly");
        assert_eq!(meta_out.len(), 32);
        assert_eq!(meta_out[0].metric_family_name, "x00\u{FFFD}\u{FFFD}");
        assert_eq!(meta_out[31].help, "x31\u{FFFD}\u{FFFD}");
    }
}

#[test]
fn w34_rw2_shared_cumulative_budget_across_labels_lossy_and_exemplars() {
    let big_valid = vec![b'a'; 20 * 1024];
    let mut big_bad = vec![b'b'; 20 * 1024];
    big_bad[0] = 0xff;
    let big_ex = vec![b'e'; 20 * 1024];

    // 1) Streaming: series labels (~20 KiB) + lossy metadata (~20 KiB) + exemplar (~20 KiB) = ~60 KiB > 48 KiB.
    let mut wire = Vec::new();
    push_bytes_field(&mut wire, 4, b"");
    push_bytes_field(&mut wire, 4, b"__name__"); // 1
    push_bytes_field(&mut wire, 4, &big_valid); // 2
    push_bytes_field(&mut wire, 4, &big_bad); // 3
    push_bytes_field(&mut wire, 4, b"trace_id"); // 4
    push_bytes_field(&mut wire, 4, &big_ex); // 5

    let mut ts = Vec::new();
    let mut packed = Vec::new();
    encode_varint(1, &mut packed);
    encode_varint(2, &mut packed);
    push_bytes_field(&mut ts, 1, &packed);
    push_bytes_field(&mut ts, 2, &encode_sample(1.0, 100));
    let mut ex = Vec::new();
    let mut ex_packed = Vec::new();
    encode_varint(4, &mut ex_packed);
    encode_varint(5, &mut ex_packed);
    push_bytes_field(&mut ex, 1, &ex_packed);
    push_bytes_field(&mut ts, 4, &ex);
    push_bytes_field(&mut ts, 5, &encode_meta_rw2(1, 3, 0));
    push_bytes_field(&mut wire, 5, &ts);

    let mut scratch = WriteRequestScratch::new();
    let mut resolved = Rw2ResolvedScratch::new();
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();

    {
        let mut meta_out = Vec::new();
        let err_stream = decode_rw2_stream_with_limit(
            &wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta_out,
            48 * 1024,
            |_| {},
        )
        .expect_err("cumulative budget across labels + exemplar + lossy metadata must reject");
        assert_eq!(err_stream, DecodeError::LabelArenaLimitExceeded { limit: 48 * 1024 });
        assert!(label_bytes.is_empty());
        assert!(samples.is_empty());
        assert!(meta_out.is_empty());
    }

    let err_nonstream = decode_write_request_with_limit(
        &wire,
        DecodeOptions::rw2(),
        &mut scratch,
        48 * 1024,
    )
    .err()
    .expect("non-streaming cumulative budget must reject");
    assert_eq!(err_nonstream, DecodeError::LabelArenaLimitExceeded { limit: 48 * 1024 });

    // Fits comfortably under 128 KiB limit in both decoders.
    let mut meta_out = Vec::new();
    decode_rw2_stream_with_limit(
        &wire,
        DecodeOptions::rw2(),
        &mut scratch,
        &mut resolved,
        &mut label_bytes,
        &mut samples,
        &mut meta_out,
        128 * 1024,
        |_| {},
    )
    .expect("128 KiB limit accommodates combined 60 KiB");
    let ok_ns = decode_write_request_with_limit(&wire, DecodeOptions::rw2(), &mut scratch, 128 * 1024)
        .expect("non-streaming 128 KiB limit accommodates combined 60 KiB");
    assert_eq!(ok_ns.timeseries_len(), 1);
    assert_eq!(ok_ns.metadata_len(), 1);
}

#[test]
fn w34_rw2_unpacked_and_split_packed_label_refs_and_odd_checks() {
    let mut wire = Vec::new();
    push_bytes_field(&mut wire, 4, b"");
    push_bytes_field(&mut wire, 4, b"__name__"); // 1
    push_bytes_field(&mut wire, 4, b"up"); // 2
    push_bytes_field(&mut wire, 4, b"job"); // 3
    push_bytes_field(&mut wire, 4, b"api"); // 4
    push_bytes_field(&mut wire, 4, b"trace_id"); // 5
    push_bytes_field(&mut wire, 4, b"t123"); // 6
    push_bytes_field(&mut wire, 4, b"span"); // 7
    push_bytes_field(&mut wire, 4, b"s456"); // 8

    // Unpacked wire_type=0 labels_refs MUST be rejected with OddNumberOfLabelRefs (Go mimir.pb.go parity).
    let mut unpacked_wire = wire.clone();
    let mut ts_unpacked = Vec::new();
    for r in [1u64, 2, 3, 4] {
        push_varint_field(&mut ts_unpacked, 1, r);
    }
    push_bytes_field(&mut ts_unpacked, 2, &encode_sample(1.0, 100));
    push_bytes_field(&mut unpacked_wire, 5, &ts_unpacked);
    let mut scratch = WriteRequestScratch::new();
    assert_eq!(
        decode_write_request(&unpacked_wire, DecodeOptions::rw2(), &mut scratch).err(),
        Some(DecodeError::OddNumberOfLabelRefs)
    );

    // Split packed chunks [1, 2, 3] + [4] and exemplar [5] + [6, 7, 8] decode cleanly.
    let mut ts2 = Vec::new();
    let mut p1 = Vec::new();
    for r in [1u64, 2, 3] {
        encode_varint(r, &mut p1);
    }
    let mut p2 = Vec::new();
    encode_varint(4, &mut p2);
    push_bytes_field(&mut ts2, 1, &p1);
    push_bytes_field(&mut ts2, 1, &p2);
    push_bytes_field(&mut ts2, 2, &encode_sample(2.0, 200));

    let mut ex2 = Vec::new();
    let mut ep1 = Vec::new();
    encode_varint(5, &mut ep1);
    let mut ep2 = Vec::new();
    for r in [6u64, 7, 8] {
        encode_varint(r, &mut ep2);
    }
    push_bytes_field(&mut ex2, 1, &ep1);
    push_bytes_field(&mut ex2, 1, &ep2);
    push_bytes_field(&mut ts2, 4, &ex2);
    push_bytes_field(&mut wire, 5, &ts2);

    let dec = decode_write_request(&wire, DecodeOptions::rw2(), &mut scratch)
        .expect("split packed RW2 label_refs must decode");
    let owned = dec.to_owned_write_request();
    assert_eq!(owned.timeseries.len(), 1);
    assert_eq!(owned.timeseries[0].labels.len(), 2);
    assert_eq!(owned.timeseries[0].labels[1].value.as_ref(), b"api");
    assert_eq!(owned.timeseries[0].exemplars[0].labels.len(), 2);

    let mut resolved = Rw2ResolvedScratch::new();
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();
    let mut streamed = 0usize;
    {
        let mut meta_out = Vec::new();
        decode_rw2_stream(
            &wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta_out,
            |s| {
                streamed += 1;
                assert!(!s.encoded_labels.is_empty());
                assert_eq!(s.exemplars_len(), 1);
            },
        )
        .expect("streaming unpacked and split packed RW2 label_refs must decode");
    }
    assert_eq!(streamed, 1);

    // Odd total across split packed + unpacked -> OddNumberOfLabelRefs AFTER loop.
    let mut odd_wire = Vec::new();
    push_bytes_field(&mut odd_wire, 4, b"");
    push_bytes_field(&mut odd_wire, 4, b"a");
    let mut ts_odd = Vec::new();
    let mut op1 = Vec::new();
    encode_varint(1, &mut op1);
    push_bytes_field(&mut ts_odd, 1, &op1);
    push_varint_field(&mut ts_odd, 1, 1);
    push_varint_field(&mut ts_odd, 1, 1);
    push_bytes_field(&mut odd_wire, 5, &ts_odd);
    assert_eq!(
        decode_write_request(&odd_wire, DecodeOptions::rw2(), &mut scratch).err(),
        Some(DecodeError::OddNumberOfLabelRefs)
    );
    let mut meta_out = Vec::new();
    assert_eq!(
        decode_rw2_stream(
            &odd_wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta_out,
            |_| {},
        )
        .err(),
        Some(DecodeError::OddNumberOfLabelRefs)
    );
}

#[test]
fn w34_rw2_metadata_before_label_refs_and_symbols_after_timeseries() {
    // MetadataRW2 (field 5) BEFORE labels_refs (field 1) inside TimeSeriesRW2,
    // AND symbols_rw2 (field 4) AFTER timeseries_rw2 (field 5) at top level,
    // in both RW2 and KafkaV2 modes.
    let mut ts = Vec::new();
    push_bytes_field(&mut ts, 5, &encode_meta_rw2(1, 66, 67)); // counter metadata FIRST!
    push_bytes_field(&mut ts, 2, &encode_sample(5.0, 1000));
    let mut packed = Vec::new();
    encode_varint(1, &mut packed); // common "__name__" in KafkaV2!
    encode_varint(65, &mut packed); // local "http_requests_total"
    push_bytes_field(&mut ts, 1, &packed); // labels_refs LAST!

    let mut kv2_wire = Vec::new();
    push_bytes_field(&mut kv2_wire, 4, b""); // 64
    push_bytes_field(&mut kv2_wire, 4, b"http_requests_total"); // 65
    push_bytes_field(&mut kv2_wire, 4, b"total requests"); // 66
    push_bytes_field(&mut kv2_wire, 4, b"requests"); // 67
    push_bytes_field(&mut kv2_wire, 5, &ts);

    let mut scratch = WriteRequestScratch::new();
    let dec = decode_write_request(&kv2_wire, DecodeOptions::kafka_v2(), &mut scratch)
        .expect("metadata before label_refs and symbols after timeseries must decode in kafka_v2");
    assert_eq!(dec.timeseries_len(), 1);
    let metas: Vec<_> = dec.metadata().collect();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].metric_family_name, "http_requests_total");
    assert_eq!(metas[0].help, "total requests");
    assert_eq!(metas[0].unit, "requests");

    let mut resolved = Rw2ResolvedScratch::new();
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();
    let mut got_names = Vec::new();
    {
        let mut meta_out = Vec::new();
        decode_rw2_stream(
            &kv2_wire,
            DecodeOptions::kafka_v2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut meta_out,
            |s| got_names.push(s.encoded_labels.to_vec()),
        )
        .expect("streaming metadata before label_refs and symbols after timeseries must decode");
        assert_eq!(got_names.len(), 1);
        assert_eq!(meta_out.len(), 1);
        assert_eq!(meta_out[0].metric_family_name, "http_requests_total");
        assert_eq!(meta_out[0].help, "total requests");
        assert_eq!(meta_out[0].unit, "requests");
    }

    // Verify RW2 custom/standard normalization with metadata before labels_refs too (_bucket stripped).
    let mut ts_hist = Vec::new();
    push_bytes_field(&mut ts_hist, 5, &encode_meta_rw2(3, 3, 0)); // Histogram
    let mut p_hist = Vec::new();
    encode_varint(1, &mut p_hist);
    encode_varint(2, &mut p_hist);
    push_bytes_field(&mut ts_hist, 1, &p_hist);

    let mut rw2_wire = Vec::new();
    push_bytes_field(&mut rw2_wire, 4, b""); // 0
    push_bytes_field(&mut rw2_wire, 4, b"__name__"); // 1
    push_bytes_field(&mut rw2_wire, 4, b"lat_bucket"); // 2
    push_bytes_field(&mut rw2_wire, 4, b"latency"); // 3
    push_bytes_field(&mut rw2_wire, 5, &ts_hist);

    label_bytes.clear();
    samples.clear();
    let mut meta_out = Vec::new();
    decode_rw2_stream(
        &rw2_wire,
        DecodeOptions::rw2(),
        &mut scratch,
        &mut resolved,
        &mut label_bytes,
        &mut samples,
        &mut meta_out,
        |_| {},
    )
    .expect("RW2 metadata before label_refs must normalize metric family name");
    assert_eq!(meta_out.len(), 1);
    assert_eq!(meta_out[0].metric_family_name, "lat");
    assert!(V2_RECORD_SYMBOL_OFFSET as usize >= V2_COMMON_SYMBOLS_LIST.len());
}

#[test]
fn w34_s2_and_snappy_untrusted_varint_caps_and_worker_panic_to_io_error() {
    // 1) S2 block decode with 4 GiB-1 untrusted uvarint header must reject without OOM.
    let malicious_4gib = [0xffu8, 0xff, 0xff, 0xff, 0x0f];
    assert_eq!(s2::decode(&malicious_4gib).unwrap_err(), S2Error::TooLarge);
    assert_eq!(
        s2::decode_bounded(&malicious_4gib, 1024).unwrap_err(),
        S2Error::TooLarge
    );

    // 2) S2 block decode with 16 MiB uvarint header and empty body must fail Corrupt without allocating 16 MiB.
    let malicious_16mib_empty = [0x80u8, 0x80, 0x80, 0x08];
    assert_eq!(
        s2::decode(&malicious_16mib_empty).unwrap_err(),
        S2Error::Corrupt
    );

    // 3) S2 Dict::decode with 4 GiB-1 untrusted uvarint header must reject without OOM.
    let dict = s2::Dict::make(b"0123456789abcdef0123456789abcdef", b"01234567").unwrap();
    assert_eq!(dict.decode(&malicious_4gib).unwrap_err(), S2Error::TooLarge);
    assert_eq!(
        dict.decode_bounded(&malicious_4gib, 1024).unwrap_err(),
        S2Error::TooLarge
    );

    // 4) Snappy decode_grpc_frame_bounded rejects zip bomb past limit.
    let bomb_raw = vec![b'z'; 64 * 1024];
    let frame = encode_grpc_frame(&bomb_raw, true).unwrap();
    assert!(decode_grpc_frame(&frame).is_ok());
    let err = decode_grpc_frame_bounded(&frame, 1024).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        err.get_ref()
            .and_then(|e| e.downcast_ref::<DecompressLimitExceeded>()),
        Some(&DecompressLimitExceeded { limit: 1024 })
    );
    let _ = compress_framed(b"ok").unwrap();

    // 5) S2 Writer with concurrency > 1 returns io::Error if a worker panics instead of panicking on join().
    if !cfg!(miri) {
        let mut out = Vec::new();
        let mut opts = WriterOptions {
            block_size: MIN_BLOCK_SIZE,
            concurrency: 2,
            custom_encoder: Some(Arc::new(|_, _| -> isize {
                panic!("simulated worker panic");
            })),
            ..Default::default()
        };
        opts.block_size = MIN_BLOCK_SIZE;
        let mut w = Writer::with_options(&mut out, opts);
        let payload = vec![b'a'; MIN_BLOCK_SIZE * 2];
        let write_res = w.write_all(&payload).and_then(|()| w.close());
        assert!(
            write_res.is_err(),
            "panicking s2 worker must return io::Error instead of panicking caller"
        );
    }
}
