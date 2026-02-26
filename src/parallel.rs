use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::Result;
use core_affinity;
use rayon::prelude::*;

use crate::{SearchResult, topology};

pub struct Parallel {
    pub block_size: usize,
    pub parallelism: usize,
    /// Number of independent reader threads.  Each reader owns a contiguous
    /// file segment and feeds its own rayon worker pool via a double-buffer
    /// channel, so IO and CPU overlap across segments.  With `num_readers=1`
    /// you get a single reader with double-buffering.
    pub num_readers: usize,
    pub batch_multiplier: usize,
    pub use_simd: bool,
    /// When true, pin each reader thread and its rayon worker pool to a
    /// specific die so that data read by a reader lands in the L3 cache
    /// shared by its workers.
    pub pin_threads: bool,
}

/// Interval in blocks at which to print progress updates during the search
/// operation.
const PROGRESS_INTERVAL_BLOCK: usize = 1024;

/// Shared search state across all readers and workers.
struct SearchState {
    found: AtomicBool,
    found_offset: AtomicUsize,
    blocks_processed: AtomicUsize,
    start_time: Instant,
}

struct BatchInfo {
    start_block: usize,
    data: Vec<u8>,
    num_blocks: usize,
    /// Non-zero only for the very last batch when file size is not a multiple
    /// of block_size.
    last_block_size: usize,
}

impl Parallel {
    pub fn search(&self, input_path: impl AsRef<Path>, needle: u8) -> Result<SearchResult> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .append(false)
            .open(input_path.as_ref())?;

        let file_size = usize::try_from(file.metadata()?.len())?;
        let total_blocks = file_size.div_ceil(self.block_size);

        let num_readers = self.num_readers.max(1);
        // Distribute workers evenly; first `extra` readers get one extra worker.
        let base_workers = (self.parallelism / num_readers).max(1);
        let extra_workers = self.parallelism % num_readers;

        eprintln!(
            "File size: {:.2} MiB ({} blocks of {} bytes)",
            file_size as f64 / (1024.0 * 1024.0),
            total_blocks,
            self.block_size
        );
        eprintln!(
            "Readers: {num_readers}, total workers: {}, batch_multiplier: {}",
            self.parallelism, self.batch_multiplier
        );

        // Build per-reader pinning assignments (reader_core, worker_cores).
        // Readers are assigned to dies round-robin; within each die the first
        // core goes to the reader thread, the rest to the rayon workers.
        let pin_assignments: Vec<Option<(usize, Vec<usize>)>> = if self.pin_threads {
            let groups = topology::cores_by_die();
            eprintln!("Thread pinning enabled ({} die(s) detected):", groups.len());
            let dies: Vec<Vec<usize>> = groups.into_values().collect();
            let n_dies = dies.len();
            (0..num_readers)
                .map(|i| {
                    let workers = if i < extra_workers {
                        base_workers + 1
                    } else {
                        base_workers
                    };
                    let die = &dies[i % n_dies];
                    let reader_core = die[0];
                    // Workers start at die[1], wrapping if the die has fewer
                    // physical cores than workers needed.
                    let worker_cores: Vec<usize> =
                        (0..workers).map(|j| die[(j + 1) % die.len()]).collect();
                    eprintln!(
                        "  reader {i} → core {reader_core} (die {}), workers → {worker_cores:?}",
                        i % n_dies
                    );
                    Some((reader_core, worker_cores))
                })
                .collect()
        } else {
            vec![None; num_readers]
        };

        let state = Arc::new(SearchState {
            found: AtomicBool::new(false),
            found_offset: AtomicUsize::new(0),
            blocks_processed: AtomicUsize::new(0),
            start_time: Instant::now(),
        });

        let file = Arc::new(file);
        // Divide the file into num_readers contiguous segments.
        let blocks_per_reader = total_blocks.div_ceil(num_readers);

        let handles: Vec<std::thread::JoinHandle<Result<()>>> = (0..num_readers)
            .zip(pin_assignments)
            .map(|(i, pin_info)| {
                let segment_start = i * blocks_per_reader;
                let segment_end = ((i + 1) * blocks_per_reader).min(total_blocks);
                let workers = if i < extra_workers {
                    base_workers + 1
                } else {
                    base_workers
                };
                let batch_size = workers * self.batch_multiplier;
                let block_size = self.block_size;
                let use_simd = self.use_simd;
                let state = Arc::clone(&state);
                let file = Arc::clone(&file);

                eprintln!(
                    "  segment {i}: blocks [{segment_start}, {segment_end}), {workers} worker(s), \
                     batch={batch_size} blocks ({:.2} MiB)",
                    (batch_size * block_size) as f64 / (1024.0 * 1024.0)
                );

                std::thread::spawn(move || -> Result<()> {
                    if segment_start >= total_blocks {
                        return Ok(());
                    }

                    let (reader_core_opt, worker_cores_opt) = match pin_info {
                        Some((rc, wc)) => (Some(rc), Some(wc)),
                        None => (None, None),
                    };

                    // Build a dedicated rayon pool for this segment's workers.
                    let pool = {
                        let mut builder = rayon::ThreadPoolBuilder::new().num_threads(workers);
                        if let Some(cores) = worker_cores_opt {
                            builder = builder.start_handler(move |thread_idx| {
                                if let Some(&core_id) = cores.get(thread_idx) {
                                    core_affinity::set_for_current(core_affinity::CoreId {
                                        id: core_id,
                                    });
                                }
                            });
                        }
                        builder.build()?
                    };

                    // Bounded channel with capacity 1: the reader stays one
                    // batch ahead of the workers (double-buffering).
                    // None signals end-of-segment.
                    let (tx, rx) = std::sync::mpsc::sync_channel::<Option<BatchInfo>>(1);

                    // IO thread: reads batches for this segment and sends them.
                    let io_state = Arc::clone(&state);
                    let io_file = Arc::clone(&file);
                    let io_thread = std::thread::spawn(move || -> Result<()> {
                        if let Some(core) = reader_core_opt {
                            core_affinity::set_for_current(core_affinity::CoreId { id: core });
                        }
                        let mut current_block = segment_start;
                        while current_block < segment_end && !io_state.found.load(Ordering::Relaxed)
                        {
                            let blocks_in_batch = batch_size.min(segment_end - current_block);
                            let batch_byte_size = blocks_in_batch * block_size;
                            let last_block_size = if current_block + blocks_in_batch == total_blocks
                            {
                                let remaining = file_size - (current_block * block_size);
                                remaining % block_size
                            } else {
                                0
                            };
                            let mut data = vec![0u8; batch_byte_size];
                            read_aligned_batch(&io_file, current_block * block_size, &mut data)?;
                            if tx
                                .send(Some(BatchInfo {
                                    start_block: current_block,
                                    data,
                                    num_blocks: blocks_in_batch,
                                    last_block_size,
                                }))
                                .is_err()
                            {
                                // Compute loop exited early (needle found).
                                break;
                            }
                            current_block += blocks_in_batch;
                        }
                        let _ = tx.send(None);
                        Ok(())
                    });

                    // Compute loop: receive pre-read batches and search using
                    // this segment's rayon pool.
                    while let Ok(Some(batch)) = rx.recv() {
                        if state.found.load(Ordering::Relaxed) {
                            break;
                        }
                        pool.install(|| {
                            search_batch(
                                &batch.data,
                                batch.start_block,
                                batch.num_blocks,
                                batch.last_block_size,
                                block_size,
                                use_simd,
                                needle,
                                &state,
                            )
                        })?;
                    }

                    io_thread
                        .join()
                        .map_err(|_| anyhow::anyhow!("IO thread panicked"))??;
                    Ok(())
                })
            })
            .collect();

        for handle in handles {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("segment thread panicked"))??;
        }

        if state.found.load(Ordering::Relaxed) {
            let offset = state.found_offset.load(Ordering::Relaxed);
            Ok(SearchResult {
                bytes_searched: offset + 1,
                found_at: Some(offset),
            })
        } else {
            Ok(SearchResult {
                bytes_searched: file_size,
                found_at: None,
            })
        }
    }
}

fn search_batch(
    batch_data: &[u8],
    batch_start_block: usize,
    num_blocks: usize,
    last_block_size: usize,
    block_size: usize,
    use_simd: bool,
    needle: u8,
    state: &SearchState,
) -> Result<()> {
    let block_indices: Vec<usize> = (0..num_blocks).collect();

    block_indices.into_par_iter().try_for_each(|block_idx| {
        if state.found.load(Ordering::Relaxed) {
            return Ok::<_, anyhow::Error>(());
        }

        let block_offset = block_idx * block_size;
        let this_block_size = if block_idx == num_blocks - 1 && last_block_size > 0 {
            last_block_size
        } else {
            block_size
        };

        let block_data = &batch_data[block_offset..block_offset + this_block_size];
        let global_block_idx = batch_start_block + block_idx;
        let global_offset = global_block_idx * block_size;

        let found_idx = if use_simd {
            memchr::memchr(needle, block_data)
        } else {
            block_data.iter().position(|&b| b == needle)
        };

        if let Some(idx) = found_idx {
            if !state.found.swap(true, Ordering::SeqCst) {
                state
                    .found_offset
                    .store(global_offset + idx, Ordering::SeqCst);
            }
            return Ok(());
        }

        let total_processed = state.blocks_processed.fetch_add(1, Ordering::Relaxed) + 1;
        if total_processed.is_multiple_of(PROGRESS_INTERVAL_BLOCK) {
            let bytes_processed = total_processed * block_size;
            let elapsed = state.start_time.elapsed().as_secs_f64();
            let mb_processed = bytes_processed as f64 / (1024.0 * 1024.0);
            let throughput = mb_processed / elapsed;
            eprintln!(
                "Processed: {mb_processed:.2} MiB ({total_processed} blocks, {throughput:.2} \
                 MiB/s)"
            );
        }

        Ok(())
    })?;

    Ok(())
}

fn read_aligned_batch(file: &std::fs::File, offset: usize, buffer: &mut [u8]) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    let mut bytes_read = 0;

    while bytes_read < buffer.len() {
        let result = unsafe {
            libc::pread(
                fd,
                buffer[bytes_read..].as_mut_ptr().cast::<libc::c_void>(),
                buffer.len() - bytes_read,
                i64::try_from(offset + bytes_read)?,
            )
        };

        if result < 0 {
            return Err(std::io::Error::last_os_error().into());
        } else if result == 0 {
            break;
        }

        bytes_read += result.cast_unsigned();
    }

    Ok(())
}
