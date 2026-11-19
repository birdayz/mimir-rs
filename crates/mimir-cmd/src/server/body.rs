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

/// Observer notified for every frame yielded by [`TrackedBody`] and once when the body completes
/// or is dropped/aborted.
pub trait BodyObserver {
    fn on_frame(&mut self, frame: &Frame<Bytes>);
    fn on_done(&mut self, completed: bool);
}

/// Default [`BodyObserver`] backed by optional boxed callbacks.
#[derive(Default)]
pub struct CallbackObserver {
    on_frame: Option<FrameCallback>,
    on_done: Option<DoneCallback>,
}

impl BodyObserver for CallbackObserver {
    #[inline]
    fn on_frame(&mut self, frame: &Frame<Bytes>) {
        if let Some(cb) = self.on_frame.as_mut() {
            cb(frame);
        }
    }

    #[inline]
    fn on_done(&mut self, completed: bool) {
        if let Some(cb) = self.on_done.take() {
            cb(completed);
        }
    }
}

pub type DeadlineErrFn = Box<dyn FnMut() -> BoxError + Send + Sync>;

pub struct TrackedBody<B, O: BodyObserver = CallbackObserver> {
    inner: B,
    observer: O,
    done: bool,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    deadline_at: Option<Instant>,
    deadline_msg: &'static str,
    /// Builds the deadline error instead of `deadline_msg` (e.g. an `h2` reset reason).
    deadline_err: Option<DeadlineErrFn>,
}

impl<B> TrackedBody<B, CallbackObserver> {
    pub fn new(inner: B) -> Self {
        Self::with_observer(inner, CallbackObserver::default())
    }

    pub fn on_frame(mut self, cb: FrameCallback) -> Self {
        self.observer.on_frame = Some(cb);
        self
    }

    pub fn on_done(mut self, cb: DoneCallback) -> Self {
        self.observer.on_done = Some(cb);
        self
    }
}

impl<B, O: BodyObserver> TrackedBody<B, O> {
    pub fn with_observer(inner: B, observer: O) -> Self {
        Self {
            inner,
            observer,
            done: false,
            deadline: None,
            deadline_at: None,
            deadline_msg: "",
            deadline_err: None,
        }
    }

    pub fn deadline(mut self, at: Option<Instant>, msg: &'static str) -> Self {
        self.deadline_at = at;
        self.deadline = at.map(|at| Box::pin(tokio::time::sleep_until(at.into())));
        self.deadline_msg = msg;
        self
    }

    /// Like [`Self::deadline`], failing the body with `err()` once `at` has passed.
    pub fn deadline_with(mut self, at: Option<Instant>, err: fn() -> BoxError) -> Self {
        self.deadline_at = at;
        self.deadline = at.map(|at| Box::pin(tokio::time::sleep_until(at.into())));
        self.deadline_err = Some(Box::new(err));
        self
    }

    /// Like [`Self::deadline_with`], accepting a closure (e.g. to mark an `RpcDeadlineContext` expired).
    pub fn deadline_with_cb<F>(mut self, at: Option<Instant>, err: F) -> Self
    where
        F: FnMut() -> BoxError + Send + Sync + 'static,
    {
        self.deadline_at = at;
        self.deadline = at.map(|at| Box::pin(tokio::time::sleep_until(at.into())));
        self.deadline_err = Some(Box::new(err));
        self
    }

    #[inline]
    fn finish(&mut self, completed: bool) {
        if !self.done {
            self.done = true;
            self.observer.on_done(completed);
        }
    }
}

impl<B, O: BodyObserver> Drop for TrackedBody<B, O> {
    fn drop(&mut self) {
        self.finish(false);
    }
}

impl<B, O> Body for TrackedBody<B, O>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
    O: BodyObserver + Unpin,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = &mut *self;
        if let Some(at) = this.deadline_at {
            let expired = Instant::now() >= at
                || this
                    .deadline
                    .as_mut()
                    .is_some_and(|d| d.as_mut().poll(cx).is_ready());
            if expired {
                this.finish(false);
                let err = match this.deadline_err.as_mut() {
                    Some(f) => f(),
                    None => this.deadline_msg.into(),
                };
                return Poll::Ready(Some(Err(err)));
            }
        }
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                this.observer.on_frame(&frame);
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
            Poll::Pending => Poll::Pending,
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

    #[tokio::test]
    async fn tracked_body_ready_loop_enforces_deadline_and_invokes_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        struct AlwaysReadyBody;
        impl Body for AlwaysReadyBody {
            type Data = Bytes;
            type Error = BoxError;

            fn poll_frame(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"chunk")))))
            }
        }

        let done_completed = Arc::new(AtomicBool::new(true));
        let done_flag = Arc::clone(&done_completed);
        let cb_fired = Arc::new(AtomicBool::new(false));
        let cb_flag = Arc::clone(&cb_fired);

        let deadline = Instant::now() + Duration::from_millis(30);
        let mut body = TrackedBody::new(AlwaysReadyBody)
            .on_done(Box::new(move |completed| {
                done_flag.store(completed, Ordering::SeqCst);
            }))
            .deadline_with_cb(Some(deadline), move || {
                cb_flag.store(true, Ordering::SeqCst);
                "deadline exceeded".into()
            });

        let mut frames = 0usize;
        let mut err_msg = None;
        for _ in 0..100 {
            match std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                Some(Ok(_)) => {
                    frames += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Some(Err(e)) => {
                    err_msg = Some(e.to_string());
                    break;
                }
                None => panic!("AlwaysReadyBody never ends"),
            }
        }
        assert!(frames > 0, "should yield frames before deadline");
        assert_eq!(err_msg.as_deref(), Some("deadline exceeded"));
        assert!(cb_fired.load(Ordering::SeqCst));
        assert!(!done_completed.load(Ordering::SeqCst));
    }
}
