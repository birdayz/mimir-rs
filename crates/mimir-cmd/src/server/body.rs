//! HTTP body wrappers used by the server middleware: frame observation (byte counting, gRPC
//! message framing, trailers), completion notification (end of stream or drop), and deadlines
//! (Go `http.Server` `ReadTimeout` / `WriteTimeout` semantics: an expired deadline aborts the
//! connection instead of completing the body).

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Called for every frame yielded by the wrapped body.
pub type FrameCallback = Box<dyn FnMut(&Frame<Bytes>) + Send + Sync>;
/// Called exactly once: `true` when the body reached its end, `false` when it was dropped early or
/// failed.
pub type DoneCallback = Box<dyn FnOnce(bool) + Send + Sync>;

pub struct TrackedBody<B> {
    inner: B,
    on_frame: Option<FrameCallback>,
    on_done: Option<DoneCallback>,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    deadline_msg: &'static str,
}

impl<B> TrackedBody<B> {
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            on_frame: None,
            on_done: None,
            deadline: None,
            deadline_msg: "",
        }
    }

    pub fn on_frame(mut self, cb: FrameCallback) -> Self {
        self.on_frame = Some(cb);
        self
    }

    pub fn on_done(mut self, cb: DoneCallback) -> Self {
        self.on_done = Some(cb);
        self
    }

    pub fn deadline(mut self, at: Option<Instant>, msg: &'static str) -> Self {
        self.deadline = at.map(|at| Box::pin(tokio::time::sleep_until(at.into())));
        self.deadline_msg = msg;
        self
    }

    fn finish(&mut self, completed: bool) {
        if let Some(cb) = self.on_done.take() {
            cb(completed);
        }
    }
}

impl<B> Drop for TrackedBody<B> {
    fn drop(&mut self) {
        self.finish(false);
    }
}

impl<B> Body for TrackedBody<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(cb) = this.on_frame.as_mut() {
                    cb(&frame);
                }
                if this.inner.is_end_stream() {
                    this.finish(true);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.finish(false);
                Poll::Ready(Some(Err(e.into())))
            }
            Poll::Ready(None) => {
                this.finish(true);
                Poll::Ready(None)
            }
            Poll::Pending => {
                if let Some(d) = this.deadline.as_mut() {
                    if d.as_mut().poll(cx).is_ready() {
                        this.finish(false);
                        return Poll::Ready(Some(Err(this.deadline_msg.into())));
                    }
                }
                Poll::Pending
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

use std::future::Future as _;

/// Incrementally parses gRPC length-prefixed messages (`compressed-flag(1) length(4) payload`)
/// and reports each message's wire length (payload + 5-byte header), like grpc-go's
/// `stats.InPayload/OutPayload.WireLength`.
#[derive(Default)]
pub struct GrpcFrameCounter {
    header: [u8; 5],
    header_len: usize,
    remaining: usize,
}

impl GrpcFrameCounter {
    /// Feeds `data`, calling `on_message(wire_len)` for every completed message.
    pub fn feed(&mut self, mut data: &[u8], mut on_message: impl FnMut(usize)) {
        while !data.is_empty() {
            if self.remaining > 0 {
                let n = self.remaining.min(data.len());
                self.remaining -= n;
                data = &data[n..];
                if self.remaining == 0 {
                    let len = u32::from_be_bytes([self.header[1], self.header[2], self.header[3], self.header[4]]);
                    on_message(len as usize + 5);
                }
                continue;
            }
            let n = (5 - self.header_len).min(data.len());
            self.header[self.header_len..self.header_len + n].copy_from_slice(&data[..n]);
            self.header_len += n;
            data = &data[n..];
            if self.header_len == 5 {
                self.header_len = 0;
                let len = u32::from_be_bytes([self.header[1], self.header[2], self.header[3], self.header[4]]) as usize;
                if len == 0 {
                    on_message(5);
                } else {
                    self.remaining = len;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_frame_counter_handles_split_frames() {
        let mut msgs = Vec::new();
        let mut c = GrpcFrameCounter::default();
        let mut wire = vec![0u8, 0, 0, 0, 3, 1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        wire.push(9);
        for chunk in wire.chunks(2) {
            c.feed(chunk, |n| msgs.push(n));
        }
        assert_eq!(msgs, vec![8, 5, 6]);
    }
}
