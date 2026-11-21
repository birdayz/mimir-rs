//! Wave 26 Track 1 regression tests:
//! - S1-1: Closed TSDB queries return `QuerierError::Closed` instead of empty `Ok`.
//! - S4-02: Prometheus TSDB Index FormatV1 (`version == 1`) support across `IndexReader`,
//!   `Block::open`, `TsdbQuerier`, `Tsdb`, and compaction into FormatV2.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mimir_tsdb::block::{
    write_meta_file, Block, BlockMeta, BlockMetaCompaction, BlockStats, META_VERSION_1,
};
use mimir_tsdb::chunkenc::{EncodedChunk, XorChunk};
use mimir_tsdb::chunks_block::{BlockChunksWriter, ChunkMeta};
use mimir_tsdb::compact::compact_blocks;
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::encoding::Encbuf;
use mimir_tsdb::index::{
    IndexReader, INDEX_FILENAME, INDEX_FORMAT_V1, INDEX_FORMAT_V2, MAGIC_INDEX, SERIES_BYTE_ALIGN,
};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::{QuerierError, TsdbQuerier};
use mimir_tsdb::tombstones::{write_tombstones_file, Interval, MemTombstones};
use ulid::Ulid;

fn encode_xor(samples: &[(i64, f64)]) -> EncodedChunk {
    let mut c = XorChunk::new();
    {
        let mut app = c.appender().unwrap();
        for &(t, v) in samples {
            app.append(0, t, v).unwrap();
        }
    }
    EncodedChunk::Xor(c)
}

fn write_format_v1_index(
    path: &Path,
    extra_symbols: &[String],
    series: &[(Labels, Vec<ChunkMeta>)],
) -> Vec<u64> {
    let mut syms: Vec<String> = Vec::new();
    syms.push(String::new());
    syms.extend_from_slice(extra_symbols);
    for (lbls, _) in series {
        for (k, v) in lbls.iter() {
            syms.push(k.to_string());
            syms.push(v.to_string());
        }
    }
    syms.sort();
    syms.dedup();

    let mut buf = Vec::new();
    buf.extend_from_slice(&MAGIC_INDEX.to_be_bytes());
    buf.push(INDEX_FORMAT_V1);

    let toc_symbols = buf.len() as u64;
    let mut sym_payload = Encbuf::new();
    sym_payload.put_be32(syms.len() as u32);
    let mut sym_rel_offsets = Vec::with_capacity(syms.len());
    for s in &syms {
        sym_rel_offsets.push(sym_payload.len());
        sym_payload.put_uvarint_str(s);
    }
    let sym_crc = crc32c::crc32c(sym_payload.as_slice());
    let sym_payload_start = buf.len() + 4;
    buf.extend_from_slice(&(sym_payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(sym_payload.as_slice());
    buf.extend_from_slice(&sym_crc.to_be_bytes());

    let mut sym_file_offsets = HashMap::with_capacity(syms.len());
    for (s, rel) in syms.iter().zip(sym_rel_offsets) {
        sym_file_offsets.insert(s.clone(), (sym_payload_start + rel) as u32);
    }

    // 5-byte padding so series offsets start at non-16-byte-aligned positions.
    buf.extend_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
    let toc_series = buf.len() as u64;

    let mut series_refs = Vec::with_capacity(series.len());
    let mut postings_acc: BTreeMap<(String, String), Vec<u32>> = BTreeMap::new();

    for (lbls, chks) in series {
        let series_off = buf.len() as u64;
        assert!(
            !series_off.is_multiple_of(SERIES_BYTE_ALIGN as u64),
            "series_off {series_off} should not be 16-byte aligned"
        );
        series_refs.push(series_off);

        let mut sbuf = Encbuf::new();
        sbuf.put_uvarint(lbls.len());
        for (k, v) in lbls.iter() {
            sbuf.put_uvarint(sym_file_offsets[k] as usize);
            sbuf.put_uvarint(sym_file_offsets[v] as usize);
        }
        sbuf.put_uvarint(chks.len());
        if !chks.is_empty() {
            sbuf.put_varint64(chks[0].min_time);
            sbuf.put_uvarint64((chks[0].max_time - chks[0].min_time) as u64);
            sbuf.put_uvarint64(chks[0].ref_id);
            let mut prev_maxt = chks[0].max_time;
            let mut prev_ref = chks[0].ref_id as i64;
            for c in &chks[1..] {
                sbuf.put_uvarint64((c.min_time - prev_maxt) as u64);
                sbuf.put_uvarint64((c.max_time - c.min_time) as u64);
                sbuf.put_varint64((c.ref_id as i64) - prev_ref);
                prev_maxt = c.max_time;
                prev_ref = c.ref_id as i64;
            }
        }
        let mut len_hdr = Encbuf::new();
        len_hdr.put_uvarint(sbuf.len());
        let scrc = crc32c::crc32c(sbuf.as_slice());
        buf.extend_from_slice(len_hdr.as_slice());
        buf.extend_from_slice(sbuf.as_slice());
        buf.extend_from_slice(&scrc.to_be_bytes());
        buf.push(0x00);

        let sid = series_off as u32;
        postings_acc
            .entry((String::new(), String::new()))
            .or_default()
            .push(sid);
        for (k, v) in lbls.iter() {
            postings_acc
                .entry((k.to_string(), v.to_string()))
                .or_default()
                .push(sid);
        }
    }

    // Deliberately reverse postings order so the V1 postings offset table is unsorted on disk.
    let mut postings_entries: Vec<((String, String), Vec<u32>)> = postings_acc.into_iter().collect();
    postings_entries.reverse();

    let toc_label_indices = buf.len() as u64;
    let toc_postings = buf.len() as u64;
    let mut postings_offsets = Vec::with_capacity(postings_entries.len());
    for ((k, v), sids) in postings_entries {
        let p_off = buf.len() as u64;
        let mut pbuf = Encbuf::new();
        pbuf.put_be32(sids.len() as u32);
        for sid in sids {
            pbuf.put_be32(sid);
        }
        let pcrc = crc32c::crc32c(pbuf.as_slice());
        buf.extend_from_slice(&(pbuf.len() as u32).to_be_bytes());
        buf.extend_from_slice(pbuf.as_slice());
        buf.extend_from_slice(&pcrc.to_be_bytes());
        postings_offsets.push((k, v, p_off));
    }

    let toc_label_indices_table = buf.len() as u64;
    let toc_postings_table = buf.len() as u64;
    let mut pt_buf = Encbuf::new();
    pt_buf.put_be32(postings_offsets.len() as u32);
    for (k, v, off) in &postings_offsets {
        pt_buf.put_uvarint(2);
        pt_buf.put_uvarint_str(k);
        pt_buf.put_uvarint_str(v);
        pt_buf.put_uvarint64(*off);
    }
    let pt_crc = crc32c::crc32c(pt_buf.as_slice());
    buf.extend_from_slice(&(pt_buf.len() as u32).to_be_bytes());
    buf.extend_from_slice(pt_buf.as_slice());
    buf.extend_from_slice(&pt_crc.to_be_bytes());

    let mut toc = Encbuf::new();
    toc.put_be64(toc_symbols);
    toc.put_be64(toc_series);
    toc.put_be64(toc_label_indices);
    toc.put_be64(toc_label_indices_table);
    toc.put_be64(toc_postings);
    toc.put_be64(toc_postings_table);
    toc.put_crc32c();
    buf.extend_from_slice(toc.as_slice());

    fs::write(path, &buf).unwrap();
    series_refs
}

#[test]
fn s4_02_format_v1_block_open_query_tombstones_and_compact_to_v2() {
    let dir = tempfile::tempdir().unwrap();
    let v1_ulid = Ulid::from_parts(1_000, 1);
    let v1_dir = dir.path().join(v1_ulid.to_string());
    fs::create_dir_all(&v1_dir).unwrap();

    let l1 = Labels::from_pairs([("__name__", "v1_http_reqs"), ("job", "api")]);
    let l2 = Labels::from_pairs([("__name__", "v1_http_reqs"), ("job", "worker")]);

    let mut c1 = vec![ChunkMeta {
        ref_id: 0,
        min_time: 1_000,
        max_time: 3_000,
        chunk: Some(encode_xor(&[(1_000, 10.0), (2_000, 20.0), (3_000, 30.0)])),
    }];
    let mut c2 = vec![ChunkMeta {
        ref_id: 0,
        min_time: 1_500,
        max_time: 3_500,
        chunk: Some(encode_xor(&[(1_500, 1.0), (2_500, 2.0), (3_500, 3.0)])),
    }];

    {
        let mut cw = BlockChunksWriter::open(v1_dir.join("chunks")).unwrap();
        cw.write_chunks(&mut c1).unwrap();
        cw.write_chunks(&mut c2).unwrap();
        cw.close().unwrap();
    }
    for c in c1.iter_mut().chain(c2.iter_mut()) {
        c.chunk = None;
    }

    // Include > 32 symbols to exercise V1 symbol checkpointing and byte-offset ReverseLookup.
    let extra_syms: Vec<String> = (0..40).map(|i| format!("extra_sym_{i:02}")).collect();
    let series_refs = write_format_v1_index(
        &v1_dir.join(INDEX_FILENAME),
        &extra_syms,
        &[(l1.clone(), c1), (l2.clone(), c2)],
    );
    assert_eq!(series_refs.len(), 2);

    // Add a tombstone deleting [1_500, 2_500] on the first V1 series (using its unaligned SeriesRef).
    let tombs = MemTombstones::new();
    tombs.add_interval(series_refs[0], Interval::new(1_500, 2_500));
    write_tombstones_file(&v1_dir, &tombs).unwrap();

    let mut meta = BlockMeta {
        ulid: v1_ulid,
        min_time: 1_000,
        max_time: 4_000,
        stats: BlockStats {
            num_samples: 6,
            num_float_samples: 6,
            num_histogram_samples: 0,
            num_series: 2,
            num_chunks: 2,
            num_tombstones: 1,
        },
        compaction: BlockMetaCompaction {
            level: 1,
            sources: vec![v1_ulid],
            deletable: false,
            parents: Vec::new(),
            failed: false,
            hints: Vec::new(),
        },
        version: META_VERSION_1,
        out_of_order: false,
        thanos: None,
    };
    write_meta_file(&v1_dir, &mut meta).unwrap();

    // 1. Open via Block::open and query via TsdbQuerier.
    let blk = Arc::new(Block::open(&v1_dir).unwrap());
    assert_eq!(blk.index().version(), INDEX_FORMAT_V1);

    let q = TsdbQuerier::new(None, vec![Arc::clone(&blk)], 0, 5_000);
    assert_eq!(q.label_names(&[]).unwrap(), vec!["__name__", "job"]);
    assert_eq!(q.label_values("job", None, &[]).unwrap(), vec!["api", "worker"]);

    let m_all = [LabelMatcher::new(MatchType::Equal, "__name__", "v1_http_reqs").unwrap()];
    let res = q.select(true, &m_all).unwrap();
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].labels, l1);
    assert_eq!(res[0].samples.len(), 2); // t=2000 removed by tombstone [1500, 2500]
    assert_eq!(res[0].samples[0].t, 1_000);
    assert_eq!(res[0].samples[1].t, 3_000);
    assert_eq!(res[1].labels, l2);
    assert_eq!(res[1].samples.len(), 3);

    let m_worker = [LabelMatcher::new(MatchType::Equal, "job", "worker").unwrap()];
    let res_w = q.select(true, &m_worker).unwrap();
    assert_eq!(res_w.len(), 1);
    assert_eq!(res_w[0].labels, l2);

    // 2. Compact V1 block into a new V2 block and verify V2 index + tombstones materialized.
    let compact_out = dir.path().join("compacted");
    fs::create_dir_all(&compact_out).unwrap();
    let v2_meta = compact_blocks(&compact_out, &[blk.as_ref()])
        .unwrap()
        .expect("compaction should produce a V2 block");
    let v2_dir = compact_out.join(v2_meta.ulid.to_string());
    let v2_idx = IndexReader::open(v2_dir.join(INDEX_FILENAME)).unwrap();
    assert_eq!(v2_idx.version(), INDEX_FORMAT_V2);

    let v2_blk = Arc::new(Block::open(&v2_dir).unwrap());
    let q_v2 = TsdbQuerier::new(None, vec![v2_blk], 0, 5_000);
    let res_v2 = q_v2.select(true, &m_all).unwrap();
    assert_eq!(res_v2, res);

    // 3. Open the V1 block directly inside a Tsdb instance, query it, then close and assert Err(Closed).
    drop(q);
    drop(blk);
    let tsdb = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO)
        .unwrap();
    let res_db = tsdb.querier(0, 5_000).select(true, &m_all).unwrap();
    assert_eq!(res_db, res);
    tsdb.close().unwrap();
    assert!(matches!(
        tsdb.querier(0, 5_000).select(true, &m_all),
        Err(QuerierError::Closed)
    ));
}

#[test]
fn s1_1_closed_head_only_tsdb_prepared_and_lazy_stream_return_closed_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO)
        .unwrap();
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "s1_head"), ("job", "a")], 1_000, 1.0)
            .unwrap();
        app.commit().unwrap();
    }

    let m = [LabelMatcher::new(MatchType::Equal, "__name__", "s1_head").unwrap()];
    // Note: HeadReadGuard does not block Head::close(); capture PreparedChunkSelect before close().
    let q_pre = db.querier(i64::MIN, i64::MAX);
    let prepared1 = q_pre.prepare_select_chunks(&m, false).unwrap();
    let prepared2 = q_pre.prepare_select_chunks(&m, false).unwrap();
    let prepared3 = q_pre.prepare_select_chunks(&m, false).unwrap();

    db.close().unwrap();

    let q = db.querier(i64::MIN, i64::MAX);
    assert!(matches!(
        q.execute_prepared_select(prepared1, false),
        Err(QuerierError::Closed)
    ));
    assert!(matches!(
        q.execute_prepared_select_with_matchers(prepared2, &m, false),
        Err(QuerierError::Closed)
    ));
    assert!(matches!(
        q.stream_prepared_chunks_lazy(prepared3, &m, false, None, |_| Ok(true)),
        Err(QuerierError::Closed)
    ));
}
