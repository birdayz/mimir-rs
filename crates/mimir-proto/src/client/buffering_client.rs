//! Ported from github.com/grafana/mimir/pkg/ingester/client/buffering_client.go
//! and github.com/grafana/mimir/pkg/util/pool/fast_releasing_pool.go + tracking.go.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use parking_lot::Mutex;
use prost::Message as _;
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::codegen::{http, Body, GrpcMethod, StdError};

use crate::cortexpb;
use crate::ingesterpb::ingester_client::IngesterClient;

const FREE_SLAB_CHECKS: usize = 3;

pub trait PoolDelegate: Send + Sync {
    fn get(&self) -> Option<Vec<u8>>;
    fn put(&self, slab: Vec<u8>);
}

#[derive(Default)]
pub struct TrackedPool {
    slabs: Mutex<Vec<Vec<u8>>>,
    pub balance: AtomicI64,
    pub gets: AtomicI64,
}

impl TrackedPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&self) {
        self.balance.store(0, Ordering::SeqCst);
        self.gets.store(0, Ordering::SeqCst);
    }
}

impl PoolDelegate for TrackedPool {
    fn get(&self) -> Option<Vec<u8>> {
        self.balance.fetch_add(1, Ordering::SeqCst);
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.slabs.lock().pop()
    }

    fn put(&self, slab: Vec<u8>) {
        self.balance.fetch_sub(1, Ordering::SeqCst);
        self.slabs.lock().push(slab);
    }
}

struct TrackedSlab {
    slab: Vec<u8>,
    references: usize,
    next_free_index: usize,
}

/// Ported from `pool.FastReleasingSlabPool[byte]` in `pkg/util/pool/fast_releasing_pool.go`.
pub struct FastReleasingSlabPool {
    delegate: Arc<dyn PoolDelegate>,
    slab_size: usize,
    slabs: Mutex<Vec<Option<TrackedSlab>>>,
}

impl FastReleasingSlabPool {
    pub fn new(delegate: Arc<dyn PoolDelegate>, slab_size: usize) -> Self {
        Self {
            delegate,
            slab_size,
            slabs: Mutex::new(vec![None]), // slabID = 0 is invalid
        }
    }

    /// Acquires a mutable region of length `size` and returns `(slab_id, region_range, slab_bytes_copy)`
    /// or writes into the slab using a closure.
    pub fn with_slice<R>(&self, size: usize, f: impl FnOnce(&mut [u8]) -> R) -> (R, usize) {
        if size == 0 {
            let r = f(&mut []);
            return (r, 0);
        }
        if size > self.slab_size {
            let mut own = vec![0u8; size];
            let r = f(&mut own);
            return (r, 0);
        }

        let mut slabs = self.slabs.lock();
        let mut slab_id = 0usize;
        let len = slabs.len();
        let min_sid = len.saturating_sub(FREE_SLAB_CHECKS);
        for sid in (min_sid..len).rev() {
            if let Some(ref s) = slabs[sid] {
                if s.slab.len() - s.next_free_index >= size {
                    slab_id = sid;
                    break;
                }
            }
        }

        if slab_id == 0 {
            let slab = self
                .delegate
                .get()
                .unwrap_or_else(|| vec![0u8; self.slab_size]);
            slab_id = slabs.len();
            slabs.push(Some(TrackedSlab {
                slab,
                references: 0,
                next_free_index: 0,
            }));
        }

        let ts = slabs[slab_id].as_mut().unwrap();
        let start = ts.next_free_index;
        let end = start + size;
        ts.next_free_index = end;
        ts.references += 1;
        let r = f(&mut ts.slab[start..end]);
        (r, slab_id)
    }

    pub fn release(&self, slab_id: usize) {
        if slab_id == 0 {
            return;
        }
        let slab_to_release = {
            let mut slabs = self.slabs.lock();
            if slab_id >= slabs.len() {
                panic!("invalid slab id: {slab_id}");
            }
            let ts = slabs[slab_id].as_mut().expect("nil slab");
            if ts.references == 0 {
                panic!("invalid reference count: {}", ts.references);
            }
            ts.references -= 1;
            if ts.references == 0 {
                let owned = slabs[slab_id].take().unwrap();
                Some(owned.slab)
            } else {
                None
            }
        };

        if let Some(slab) = slab_to_release {
            self.delegate.put(slab);
        }
    }
}

/// Ported from `wrappedRequest` in `pkg/ingester/client/buffering_client.go`.
pub struct WrappedRequest {
    pub request: Arc<cortexpb::WriteRequest>,
    pub slab_pool: Arc<FastReleasingSlabPool>,
    pub slab_id: usize,
    pub more_slab_ids: Vec<usize>,
}

impl WrappedRequest {
    pub fn new(request: Arc<cortexpb::WriteRequest>, slab_pool: Arc<FastReleasingSlabPool>) -> Self {
        Self {
            request,
            slab_pool,
            slab_id: 0,
            more_slab_ids: Vec::new(),
        }
    }

    pub fn marshal(&mut self) -> Result<Vec<u8>, prost::EncodeError> {
        let size = self.request.encoded_len();
        self.marshal_with_size(size)
    }

    pub fn marshal_with_size(&mut self, size: usize) -> Result<Vec<u8>, prost::EncodeError> {
        let (res, slab_id) = self.slab_pool.with_slice(size, |buf| {
            let mut slice = &mut buf[..size];
            self.request.encode(&mut slice)?;
            Ok::<Vec<u8>, prost::EncodeError>(buf[..size].to_vec())
        });
        if self.slab_id == 0 {
            self.slab_id = slab_id;
        } else {
            self.more_slab_ids.push(slab_id);
        }
        res
    }

    pub fn return_buffers_to_pool(&mut self) {
        if self.slab_id != 0 {
            self.slab_pool.release(self.slab_id);
            self.slab_id = 0;
        }
        for s in self.more_slab_ids.drain(..) {
            self.slab_pool.release(s);
        }
    }
}

#[derive(Clone, Default)]
struct WrappedRequestCodec;

struct WrappedRequestEncoder;

impl Encoder for WrappedRequestEncoder {
    type Item = Arc<Mutex<WrappedRequest>>;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        let bytes = item
            .lock()
            .marshal()
            .map_err(|e| tonic::Status::internal(format!("encode WriteRequest: {e}")))?;
        dst.put_slice(&bytes);
        Ok(())
    }
}

struct WriteResponseDecoder;

impl Decoder for WriteResponseDecoder {
    type Item = cortexpb::WriteResponse;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let remaining = bytes::Buf::remaining(src);
        if remaining == 0 {
            return Ok(Some(cortexpb::WriteResponse {}));
        }
        let mut buf = vec![0u8; remaining];
        bytes::Buf::copy_to_slice(src, &mut buf);
        let resp = cortexpb::WriteResponse::decode(buf.as_slice())
            .map_err(|e| tonic::Status::internal(format!("decode WriteResponse: {e}")))?;
        Ok(Some(resp))
    }
}

impl Codec for WrappedRequestCodec {
    type Encode = Arc<Mutex<WrappedRequest>>;
    type Decode = cortexpb::WriteResponse;
    type Encoder = WrappedRequestEncoder;
    type Decoder = WriteResponseDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        WrappedRequestEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        WriteResponseDecoder
    }
}

/// Ported from `bufferPoolingIngesterClient` in `pkg/ingester/client/buffering_client.go`.
pub struct BufferingIngesterClient<T> {
    client: IngesterClient<T>,
    raw_grpc: Grpc<T>,
}

impl<T> BufferingIngesterClient<T>
where
    T: Clone
        + tonic::client::GrpcService<tonic::body::BoxBody>
        + Send
        + Sync
        + 'static,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    pub fn new(transport: T) -> Self {
        Self {
            client: IngesterClient::new(transport.clone()),
            raw_grpc: Grpc::new(transport),
        }
    }

    pub fn inner_mut(&mut self) -> &mut IngesterClient<T> {
        &mut self.client
    }

    #[allow(clippy::result_large_err)]
    pub async fn push(
        &mut self,
        request: impl tonic::IntoRequest<cortexpb::WriteRequest>,
        slab_pool: Option<Arc<FastReleasingSlabPool>>,
    ) -> Result<tonic::Response<cortexpb::WriteResponse>, tonic::Status> {
        let Some(pool) = slab_pool else {
            return self.client.push(request).await;
        };

        let tonic_req = request.into_request();
        let (metadata, extensions, inner_req) = tonic_req.into_parts();
        let wrapped = Arc::new(Mutex::new(WrappedRequest::new(Arc::new(inner_req), pool)));

        self.raw_grpc
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("Service was not ready: {}", e.into())))?;

        let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/Push");
        let mut raw_req = tonic::Request::from_parts(metadata, extensions, Arc::clone(&wrapped));
        raw_req
            .extensions_mut()
            .insert(GrpcMethod::new("cortex.Ingester", "Push"));

        let resp = self
            .raw_grpc
            .unary(raw_req, path, WrappedRequestCodec)
            .await;

        if resp.is_ok() {
            wrapped.lock().return_buffers_to_pool();
        }
        resp
    }
}
