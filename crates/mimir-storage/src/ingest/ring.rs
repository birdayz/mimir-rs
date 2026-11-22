//! Dynamically-sized circular buffer with optional bounded blocking, shrink-on-drain, and die wakeup.
//!
//! Ported from github.com/twmb/franz-go/pkg/kgo/ring.go

use std::sync::{Condvar, Mutex};

pub const MIN_RING_CAP: usize = 8;

#[derive(Debug)]
struct RingState<T> {
    elems: Vec<Option<T>>,
    head: usize,
    len: usize,
    max_len: usize,
    dead: bool,
}

impl<T> Default for RingState<T> {
    fn default() -> Self {
        Self {
            elems: Vec::new(),
            head: 0,
            len: 0,
            max_len: 0,
            dead: false,
        }
    }
}

impl<T> RingState<T> {
    fn resize(&mut self, new_cap: usize) {
        let mut new_elems: Vec<Option<T>> = Vec::with_capacity(new_cap);
        new_elems.resize_with(new_cap, || None);
        if self.len > 0 && !self.elems.is_empty() {
            let cap = self.elems.len();
            for (i, slot) in new_elems.iter_mut().enumerate().take(self.len) {
                let idx = (self.head + i) % cap;
                *slot = self.elems[idx].take();
            }
        }
        self.elems = new_elems;
        self.head = 0;
    }
}

#[derive(Debug)]
pub struct PromiseRing<T> {
    state: Mutex<RingState<T>>,
    cond: Condvar,
}

impl<T> Default for PromiseRing<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> PromiseRing<T> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(RingState::default()),
            cond: Condvar::new(),
        }
    }

    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            state: Mutex::new(RingState {
                max_len,
                ..Default::default()
            }),
            cond: Condvar::new(),
        }
    }

    pub fn init_max_len(&self, max_len: usize) {
        let mut s = self.state.lock().unwrap();
        s.max_len = max_len;
    }

    pub fn die(&self) {
        let mut s = self.state.lock().unwrap();
        s.dead = true;
        self.cond.notify_all();
    }

    pub fn is_empty(&self) -> bool {
        self.state.lock().unwrap().len == 0
    }

    pub fn len(&self) -> usize {
        self.state.lock().unwrap().len
    }

    pub fn capacity(&self) -> usize {
        self.state.lock().unwrap().elems.capacity()
    }

    pub fn push(&self, elem: T) -> (bool, bool) {
        self.do_push(elem, true)
    }

    pub fn push_force(&self, elem: T) -> (bool, bool) {
        self.do_push(elem, false)
    }

    fn do_push(&self, elem: T, wait: bool) -> (bool, bool) {
        let mut s = self.state.lock().unwrap();
        if wait {
            while s.max_len > 0 && s.len >= s.max_len && !s.dead {
                s = self.cond.wait(s).unwrap();
            }
        }
        if s.dead {
            return (false, true);
        }
        if s.len == s.elems.len() {
            let new_cap = (s.elems.len() * 2).max(MIN_RING_CAP);
            s.resize(new_cap);
        }
        let cap = s.elems.len();
        let write_pos = (s.head + s.len) % cap;
        s.elems[write_pos] = Some(elem);
        s.len += 1;
        (s.len == 1, false)
    }

    pub fn drop_peek(&self) -> (Option<T>, bool, bool)
    where
        T: Clone,
    {
        let mut s = self.state.lock().unwrap();
        if s.len == 0 {
            return (None, false, s.dead);
        }
        let cap = s.elems.len();
        let head = s.head;
        s.elems[head] = None;
        s.head = (s.head + 1) % cap;
        s.len -= 1;

        self.cond.notify_one();

        if s.len <= MIN_RING_CAP / 2 && s.elems.len() > MIN_RING_CAP {
            s.resize(MIN_RING_CAP);
        }

        if s.len > 0 {
            let next = s.elems[s.head].clone();
            return (next, true, s.dead);
        }
        (None, false, s.dead)
    }
}
