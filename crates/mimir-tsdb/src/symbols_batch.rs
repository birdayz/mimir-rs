//! Batched external-sort symbol writer and k-way merge iterator.
//!
//! Ported from github.com/grafana/mimir/tsdb/symbols_batch.go.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SymbolsBatchError {
    #[error("no symbols")]
    NoSymbols,
    #[error("symbol flushers closed")]
    Closed,
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid utf-8 in symbol file: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

struct FlusherJob {
    output_file: PathBuf,
    symbols: HashSet<String>,
}

/// Writes sorted deduplicated symbol batches to disk files on background worker threads.
pub struct SymbolFlushers {
    tx: Mutex<Option<SyncSender<FlusherJob>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    err: Arc<Mutex<Option<String>>>,
}

impl SymbolFlushers {
    /// Spawns `concurrency` background flusher threads.
    pub fn new(concurrency: usize) -> Arc<Self> {
        let concurrency = concurrency.max(1);
        let (tx, rx) = sync_channel::<FlusherJob>(0);
        let rx = Arc::new(Mutex::new(rx));
        let err = Arc::new(Mutex::new(None::<String>));

        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let rx_c = Arc::clone(&rx);
            let err_c = Arc::clone(&err);
            workers.push(std::thread::spawn(move || {
                Self::worker_loop(rx_c, err_c);
            }));
        }

        Arc::new(Self {
            tx: Mutex::new(Some(tx)),
            workers: Mutex::new(workers),
            err,
        })
    }

    fn worker_loop(rx: Arc<Mutex<Receiver<FlusherJob>>>, err_slot: Arc<Mutex<Option<String>>>) {
        let mut scratch: Vec<String> = Vec::new();
        let mut failed = false;
        loop {
            let job = {
                let guard = rx.lock().unwrap();
                guard.recv()
            };
            let Ok(job) = job else {
                break;
            };
            if failed {
                continue;
            }
            scratch.clear();
            scratch.extend(job.symbols);
            scratch.sort();
            if let Err(e) = write_symbols_to_file(&job.output_file, &scratch) {
                let mut err_guard = err_slot.lock().unwrap();
                if err_guard.is_none() {
                    *err_guard = Some(e.to_string());
                }
                failed = true;
            }
            scratch.clear();
        }
    }

    pub fn flush_symbols(
        &self,
        output_file: PathBuf,
        symbols: HashSet<String>,
    ) -> Result<(), SymbolsBatchError> {
        if symbols.is_empty() {
            return Err(SymbolsBatchError::NoSymbols);
        }
        if let Some(ref e) = *self.err.lock().unwrap() {
            return Err(SymbolsBatchError::Io(io::Error::other(e.clone())));
        }
        let sender = {
            let guard = self.tx.lock().unwrap();
            guard.as_ref().cloned().ok_or(SymbolsBatchError::Closed)?
        };
        sender
            .send(FlusherJob {
                output_file,
                symbols,
            })
            .map_err(|_| {
                if let Some(ref e) = *self.err.lock().unwrap() {
                    SymbolsBatchError::Io(io::Error::other(e.clone()))
                } else {
                    SymbolsBatchError::Closed
                }
            })
    }

    /// Stops and waits until all flusher threads have finished. Idempotent.
    pub fn close(&self) -> Result<(), SymbolsBatchError> {
        {
            let mut guard = self.tx.lock().unwrap();
            guard.take();
        }
        let handles = {
            let mut w = self.workers.lock().unwrap();
            std::mem::take(&mut *w)
        };
        for h in handles {
            let _ = h.join();
        }
        if let Some(ref e) = *self.err.lock().unwrap() {
            return Err(SymbolsBatchError::Io(io::Error::other(e.clone())));
        }
        Ok(())
    }
}

impl Drop for SymbolFlushers {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn write_symbols_to_file(filename: &Path, symbols: &[String]) -> Result<(), SymbolsBatchError> {
    let f = File::create(filename)?;
    let mut sn = snap::write::FrameEncoder::new(BufWriter::new(f));
    for s in symbols {
        let len = u32::try_from(s.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "symbol too large"))?;
        sn.write_all(&len.to_le_bytes())?;
        sn.write_all(s.as_bytes())?;
    }
    let mut buf = sn
        .into_inner()
        .map_err(|e| io::Error::other(format!("snappy finalize: {e}")))?;
    buf.flush()?;
    Ok(())
}

/// Buffers symbols in memory up to `limit`, flushing sorted deduplicated batches to disk.
pub struct SymbolsBatcher {
    dir: PathBuf,
    limit: usize,
    symbol_files: Vec<PathBuf>,
    buffer: HashSet<String>,
    flushers: Arc<SymbolFlushers>,
}

impl SymbolsBatcher {
    pub fn new(limit: usize, dir: impl Into<PathBuf>, flushers: Arc<SymbolFlushers>) -> Self {
        Self {
            dir: dir.into(),
            limit: limit.max(1),
            symbol_files: Vec::new(),
            buffer: HashSet::with_capacity(limit.max(1)),
            flushers,
        }
    }

    pub fn add_symbol(&mut self, symbol: &str) -> Result<(), SymbolsBatchError> {
        if !self.buffer.contains(symbol) {
            self.buffer.insert(symbol.to_string());
        }
        if self.buffer.len() >= self.limit {
            self.flush_symbols(false)?;
        }
        Ok(())
    }

    pub fn flush_symbols(&mut self, force: bool) -> Result<(), SymbolsBatchError> {
        if !force && self.buffer.len() < self.limit {
            return Ok(());
        }
        if self.buffer.is_empty() {
            return Ok(());
        }
        let file_path = self.dir.join(format!("symbols_{}", self.symbol_files.len()));
        self.symbol_files.push(file_path.clone());
        let buf = std::mem::replace(&mut self.buffer, HashSet::with_capacity(self.limit));
        self.flushers.flush_symbols(file_path, buf)
    }

    pub fn symbol_files(&self) -> &[PathBuf] {
        &self.symbol_files
    }
}

struct SymbolsFile {
    reader: snap::read::FrameDecoder<BufReader<File>>,
    next: Option<String>,
}

impl SymbolsFile {
    fn open(path: &Path) -> Result<Self, SymbolsBatchError> {
        let f = fs::File::open(path)?;
        let reader = snap::read::FrameDecoder::new(BufReader::new(f));
        let mut sf = Self { reader, next: None };
        sf.next = sf.read_next()?;
        Ok(sf)
    }

    fn read_next(&mut self) -> Result<Option<String>, SymbolsBatchError> {
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut bytes = vec![0u8; len];
        self.reader.read_exact(&mut bytes)?;
        Ok(Some(String::from_utf8(bytes)?))
    }

    fn peek(&self) -> Option<&str> {
        self.next.as_deref()
    }

    fn advance(&mut self) -> Result<Option<String>, SymbolsBatchError> {
        let cur = self.next.take();
        if cur.is_some() {
            self.next = self.read_next()?;
        }
        Ok(cur)
    }
}

struct HeapItem {
    file_idx: usize,
    symbol: String,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.symbol == other.symbol && self.file_idx == other.file_idx
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.symbol
            .cmp(&other.symbol)
            .then_with(|| self.file_idx.cmp(&other.file_idx))
    }
}

/// K-way merge deduplicating iterator across flushed symbol batch files.
pub struct SymbolsIterator {
    files: Vec<SymbolsFile>,
    heap: BinaryHeap<Reverse<HeapItem>>,
    last_returned: Option<String>,
}

impl SymbolsIterator {
    pub fn open(filenames: &[PathBuf]) -> Result<Self, SymbolsBatchError> {
        let mut files = Vec::with_capacity(filenames.len());
        let mut heap = BinaryHeap::with_capacity(filenames.len());
        for (idx, p) in filenames.iter().enumerate() {
            let sf = SymbolsFile::open(p)?;
            if let Some(sym) = sf.peek() {
                heap.push(Reverse(HeapItem {
                    file_idx: idx,
                    symbol: sym.to_string(),
                }));
            }
            files.push(sf);
        }
        Ok(Self {
            files,
            heap,
            last_returned: None,
        })
    }

    pub fn next_symbol(&mut self) -> Result<Option<String>, SymbolsBatchError> {
        while let Some(Reverse(top)) = self.heap.pop() {
            let sf = &mut self.files[top.file_idx];
            let result = sf.advance()?;
            if let Some(next_sym) = sf.peek() {
                self.heap.push(Reverse(HeapItem {
                    file_idx: top.file_idx,
                    symbol: next_sym.to_string(),
                }));
            }
            let Some(sym) = result else {
                continue;
            };
            if self.last_returned.as_deref() == Some(sym.as_str()) {
                continue;
            }
            self.last_returned = Some(sym.clone());
            return Ok(Some(sym));
        }
        Ok(None)
    }

    pub fn close(self) -> Result<(), SymbolsBatchError> {
        drop(self.files);
        Ok(())
    }
}
