//! Ported from github.com/grafana/mimir/pkg/mimirpb/split.go

use prost::Message as _;

use crate::cortexpb;
use crate::symbols::{CommonSymbols, FastSymbolsTable, MIN_PREALLOCATED_SYMBOLS_PER_REQUEST};

#[inline]
pub fn embedded_message_field_size(message_size: usize) -> usize {
    1 + message_size + prost::encoding::encoded_len_varint(message_size as u64)
}

fn new_partial_write_request(req: &cortexpb::WriteRequest) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        source: req.source,
        skip_label_validation: req.skip_label_validation,
        skip_label_count_validation: req.skip_label_count_validation,
        ..Default::default()
    }
}

/// Splits the `WriteRequest` into multiple ones where each partial `WriteRequest` marshalled size
/// is at most `max_size`. Ported from `SplitWriteRequestByMaxMarshalSize` in `pkg/mimirpb/split.go`.
pub fn split_write_request_by_max_marshal_size(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: isize,
) -> Vec<cortexpb::WriteRequest> {
    if max_size <= 0 {
        return vec![req.clone()];
    }
    let max_size = max_size as usize;
    if req_size <= max_size {
        return vec![req.clone()];
    }

    let mut partial_reqs_with_timeseries =
        split_timeseries_by_max_marshal_size(req, req_size, max_size);
    let mut partial_reqs_with_metadata =
        split_metadata_by_max_marshal_size(req, req_size, max_size);

    if partial_reqs_with_metadata.is_empty() {
        return partial_reqs_with_timeseries;
    }
    if partial_reqs_with_timeseries.is_empty() {
        return partial_reqs_with_metadata;
    }

    let mut merged = Vec::with_capacity(
        partial_reqs_with_timeseries.len() + partial_reqs_with_metadata.len(),
    );
    merged.append(&mut partial_reqs_with_timeseries);
    merged.append(&mut partial_reqs_with_metadata);
    merged
}

fn split_timeseries_by_max_marshal_size(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
) -> Vec<cortexpb::WriteRequest> {
    if req.timeseries.is_empty() {
        return Vec::new();
    }

    let req_size_without_metadata = req_size.saturating_sub(req.metadata_size());
    if req_size_without_metadata <= max_size {
        let mut partial = new_partial_write_request(req);
        partial.timeseries = req.timeseries.clone();
        return vec![partial];
    }

    let estimated_partial_reqs =
        ((req_size_without_metadata / max_size) + 2).min(req.timeseries.len());
    let mut partial_reqs = Vec::with_capacity(estimated_partial_reqs);

    let mut next_req = new_partial_write_request(req);
    let mut next_req_size = next_req.encoded_len();
    let mut start = 0usize;
    let mut len = 0usize;

    for i in 0..req.timeseries.len() {
        let series_size = req.timeseries[i].encoded_len();
        let series_field_size = embedded_message_field_size(series_size);

        if next_req_size + series_field_size > max_size && len > 0 {
            next_req.timeseries = req.timeseries[start..start + len].to_vec();
            partial_reqs.push(next_req);

            next_req = new_partial_write_request(req);
            next_req_size = next_req.encoded_len();
            start = i;
            len = 0;
        }

        next_req_size += series_field_size;
        len += 1;
    }

    if len > 0 {
        next_req.timeseries = req.timeseries[start..start + len].to_vec();
        partial_reqs.push(next_req);
    }

    partial_reqs
}

fn split_metadata_by_max_marshal_size(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: usize,
) -> Vec<cortexpb::WriteRequest> {
    if req.metadata.is_empty() {
        return Vec::new();
    }

    let req_size_without_timeseries = req_size.saturating_sub(req.timeseries_size());
    if req_size_without_timeseries <= max_size {
        let mut partial = new_partial_write_request(req);
        partial.metadata = req.metadata.clone();
        return vec![partial];
    }

    let estimated_partial_reqs =
        ((req_size_without_timeseries / max_size) + 2).min(req.metadata.len());
    let mut partial_reqs = Vec::with_capacity(estimated_partial_reqs);

    let mut next_req = new_partial_write_request(req);
    let mut next_req_size = next_req.encoded_len();
    let mut start = 0usize;
    let mut len = 0usize;

    for i in 0..req.metadata.len() {
        let metadata_size = req.metadata[i].encoded_len();
        let metadata_field_size = embedded_message_field_size(metadata_size);

        if next_req_size + metadata_field_size > max_size && len > 0 {
            next_req.metadata = req.metadata[start..start + len].to_vec();
            partial_reqs.push(next_req);

            next_req = new_partial_write_request(req);
            next_req_size = next_req.encoded_len();
            start = i;
            len = 0;
        }

        next_req_size += metadata_field_size;
        len += 1;
    }

    if len > 0 {
        next_req.metadata = req.metadata[start..start + len].to_vec();
        partial_reqs.push(next_req);
    }

    partial_reqs
}

/// Splits RW2 write requests into multiple ones where each partial `WriteRequest` marshalled size
/// is at most `max_size`. Does not mutate `req`.
/// Ported from `SplitWriteRequestByMaxMarshalSizeRW2` in `pkg/mimirpb/split.go`.
pub fn split_write_request_by_max_marshal_size_rw2(
    req: &cortexpb::WriteRequest,
    req_size: usize,
    max_size: isize,
    offset: u32,
    common_symbols: Option<&CommonSymbols>,
) -> Vec<cortexpb::WriteRequest> {
    if max_size <= 0 {
        return vec![req.clone()];
    }
    let max_size = max_size as usize;
    if req_size <= max_size {
        return vec![req.clone()];
    }
    if req.timeseries_rw2.is_empty() {
        return Vec::new();
    }

    let estimated_partial_reqs = ((req_size / max_size) + 2).min(req.timeseries_rw2.len());
    let mut partial_reqs = Vec::with_capacity(estimated_partial_reqs);
    let estimated_timeseries_per_partial_req =
        (req.timeseries_rw2.len() / estimated_partial_reqs) + 1;

    let new_partial_req = || -> (cortexpb::WriteRequest, usize) {
        let mut r = new_partial_write_request(req);
        r.timeseries_rw2 = Vec::with_capacity(estimated_timeseries_per_partial_req);
        let size = r.encoded_len();
        (r, size)
    };

    let mut next_req_symbols = FastSymbolsTable::new(MIN_PREALLOCATED_SYMBOLS_PER_REQUEST);
    next_req_symbols.configure_common_symbols(offset, common_symbols);
    let (mut next_req, mut next_req_size) = new_partial_req();
    let mut next_req_symbols_size = 0usize;

    for ts in &req.timeseries_rw2 {
        let (series_size, symbols_size) =
            max_rw2_series_size_after_resymbolization(ts, &req.symbols_rw2, offset);
        let series_field_size = embedded_message_field_size(series_size);

        if next_req_size + series_field_size + symbols_size > max_size
            && !next_req.timeseries_rw2.is_empty()
        {
            next_req.symbols_rw2 = next_req_symbols.symbols();
            partial_reqs.push(next_req);

            (next_req, next_req_size) = new_partial_req();
            next_req_symbols_size = 0;
            next_req_symbols.reset();
            next_req_symbols.configure_common_symbols(offset, common_symbols);
        }

        let new_ts = resymbolize_timeseries_rw2(ts, &req.symbols_rw2, common_symbols, &mut next_req_symbols);
        next_req_size += embedded_message_field_size(new_ts.encoded_len());
        let new_symbols_size = next_req_symbols.symbols_size_proto();
        next_req_size += new_symbols_size - next_req_symbols_size;
        next_req_symbols_size = new_symbols_size;
        next_req.timeseries_rw2.push(new_ts);
    }

    if !next_req.timeseries_rw2.is_empty() {
        next_req.symbols_rw2 = next_req_symbols.symbols();
        partial_reqs.push(next_req);
    }

    partial_reqs
}

/// Calculates an upper bound for the size of `ts` and its referenced symbols.
/// Ported from `maxRW2SeriesSizeAfterResymbolization` in `pkg/mimirpb/split.go`.
pub fn max_rw2_series_size_after_resymbolization(
    ts: &cortexpb::TimeSeriesRw2,
    symbols: &[String],
    symbol_offset: u32,
) -> (usize, usize) {
    let highest_possible_symbol = (symbols.len() as u64) + (symbol_offset as u64);
    let symbol_size_upper_bound =
        1 + prost::encoding::encoded_len_varint(highest_possible_symbol);
    let mut series_size = ts.encoded_len();
    let mut symbols_size = 0usize;

    for &e in &ts.labels_refs {
        series_size += symbol_size_upper_bound - prost::encoding::encoded_len_varint(e as u64);
        symbols_size += resolved_symbol_size(e, symbols, symbol_offset);
    }
    for ex in &ts.exemplars {
        for &e in &ex.labels_refs {
            series_size += symbol_size_upper_bound - prost::encoding::encoded_len_varint(e as u64);
            symbols_size += resolved_symbol_size(e, symbols, symbol_offset);
        }
    }
    let help_ref = ts.metadata.as_ref().map_or(0, |m| m.help_ref);
    let unit_ref = ts.metadata.as_ref().map_or(0, |m| m.unit_ref);
    series_size += symbol_size_upper_bound - prost::encoding::encoded_len_varint(help_ref as u64);
    symbols_size += resolved_symbol_size(help_ref, symbols, symbol_offset);
    series_size += symbol_size_upper_bound - prost::encoding::encoded_len_varint(unit_ref as u64);
    symbols_size += resolved_symbol_size(unit_ref, symbols, symbol_offset);

    (series_size, symbols_size)
}

fn resolved_symbol_size(ref_id: u32, symbols: &[String], offset: u32) -> usize {
    if ref_id == 0 {
        return 1 + prost::encoding::encoded_len_varint(0);
    }
    if ref_id > 0 && ref_id < offset {
        return 0;
    }
    let l = symbols[(ref_id - offset) as usize].len();
    1 + l + prost::encoding::encoded_len_varint(l as u64)
}

/// Resolves and re-symbolizes a `TimeSeriesRw2` in the context of a new request.
/// Ported from `resymbolizeTimeSeriesRW2` in `pkg/mimirpb/split.go`.
pub fn resymbolize_timeseries_rw2(
    ts: &cortexpb::TimeSeriesRw2,
    orig_symbols: &[String],
    common_symbols: Option<&CommonSymbols>,
    symbols: &mut FastSymbolsTable<'_>,
) -> cortexpb::TimeSeriesRw2 {
    let mut new = ts.clone();
    let offset = symbols.offset();
    let common_slice = common_symbols.map(|c| c.get_slice()).unwrap_or(&[]);

    new.labels_refs = ts
        .labels_refs
        .iter()
        .map(|&old_ref| symbols.symbolize(resolve_ref(old_ref, orig_symbols, common_slice, offset)))
        .collect();

    for (i, ex) in ts.exemplars.iter().enumerate() {
        new.exemplars[i].labels_refs = ex
            .labels_refs
            .iter()
            .map(|&old_ref| {
                symbols.symbolize(resolve_ref(old_ref, orig_symbols, common_slice, offset))
            })
            .collect();
    }

    if let Some(ref mut meta) = new.metadata {
        meta.help_ref =
            symbols.symbolize(resolve_ref(meta.help_ref, orig_symbols, common_slice, offset));
        meta.unit_ref =
            symbols.symbolize(resolve_ref(meta.unit_ref, orig_symbols, common_slice, offset));
    }

    new
}

fn resolve_ref<'a>(
    ref_id: u32,
    symbols: &'a [String],
    common_symbols: &'a [String],
    offset: u32,
) -> &'a str {
    if ref_id < offset {
        common_symbols[ref_id as usize].as_str()
    } else {
        symbols[(ref_id - offset) as usize].as_str()
    }
}
