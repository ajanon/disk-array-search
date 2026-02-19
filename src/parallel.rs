use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::Result;
use rayon::prelude::*;

use crate::SearchResult;

pub struct Parallel {
    pub block_size: usize,
    pub parallelism: usize,
    pub batch_multiplier: usize,
}

/// Interval in blocks at which to print progress updates during the search
/// operation.
const PROGRESS_INTERVAL_BLOCK: usize = 1024;

/// Shared search state for parallel workers
struct SearchState {
    found: Arc<AtomicBool>,
    found_offset: Arc<AtomicUsize>,
    blocks_processed: Arc<AtomicUsize>,
    start_time: Instant,
}

impl Parallel {
    pub fn search(&self, input_path: impl AsRef<Path>, needle: u8) -> Result<SearchResult> {
        // Open file with O_DIRECT
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .append(false)
            // .custom_flags(O_DIRECT)
            .open(input_path.as_ref())?;

        // Get file size
        let file_size = usize::try_from(file.metadata()?.len())?;
        let total_blocks = file_size.div_ceil(self.block_size);

        // Calculate batch size: how many blocks to read at once
        // We want enough blocks to keep all threads busy
        let batch_size = self.parallelism * self.batch_multiplier;

        println!(
            "File size: {:.2} MiB ({} blocks of {} bytes)",
            file_size as f64 / (1024.0 * 1024.0),
            total_blocks,
            self.block_size
        );
        println!(
            "Processing in batches of {} blocks ({:.2} MiB per batch)",
            batch_size,
            (batch_size * self.block_size) as f64 / (1024.0 * 1024.0)
        );

        // Set rayon thread pool size
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.parallelism)
            .build()
            .unwrap();

        // Shared search state
        let state = SearchState {
            found: Arc::new(AtomicBool::new(false)),
            found_offset: Arc::new(AtomicUsize::new(0)),
            blocks_processed: Arc::new(AtomicUsize::new(0)),
            start_time: Instant::now(),
        };

        // Process file in batches
        let mut current_block = 0;

        while current_block < total_blocks && !state.found.load(Ordering::Relaxed) {
            let blocks_in_batch = std::cmp::min(batch_size, total_blocks - current_block);
            let batch_byte_size = blocks_in_batch * self.block_size;
            let last_block_size = if current_block + blocks_in_batch == total_blocks {
                // Last batch might have a partial block
                let remaining = file_size - (current_block * self.block_size);
                remaining % self.block_size
            } else {
                0
            };

            // Read batch into memory
            let mut batch_data = vec![0u8; batch_byte_size];
            read_aligned_batch(&file, current_block * self.block_size, &mut batch_data)?;

            // Process batch in parallel
            pool.install(|| {
                self.search_batch(
                    &batch_data,
                    current_block,
                    blocks_in_batch,
                    last_block_size,
                    needle,
                    &state,
                )
            })?;

            current_block += blocks_in_batch;
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

    fn search_batch(
        &self,
        batch_data: &[u8],
        batch_start_block: usize,
        num_blocks: usize,
        last_block_size: usize,
        needle: u8,
        state: &SearchState,
    ) -> Result<()> {
        // Create vector of block indices to process
        let block_indices: Vec<usize> = (0..num_blocks).collect();

        block_indices.into_par_iter().try_for_each(|block_idx| {
            // Check if another thread already found the needle
            if state.found.load(Ordering::Relaxed) {
                return Ok::<_, anyhow::Error>(());
            }

            let block_offset = block_idx * self.block_size;
            let block_size = if block_idx == num_blocks - 1 && last_block_size > 0 {
                last_block_size
            } else {
                self.block_size
            };

            let block_data = &batch_data[block_offset..block_offset + block_size];
            let global_block_idx = batch_start_block + block_idx;
            let global_offset = global_block_idx * self.block_size;

            // Search for needle in this block
            for (idx, &byte) in block_data.iter().enumerate() {
                if byte == needle {
                    // Try to set found flag atomically
                    if !state.found.swap(true, Ordering::SeqCst) {
                        // We're the first to find it
                        state
                            .found_offset
                            .store(global_offset + idx, Ordering::SeqCst);
                    }
                    return Ok(());
                }
            }

            // Update progress
            let total_processed = state.blocks_processed.fetch_add(1, Ordering::Relaxed) + 1;
            if total_processed.is_multiple_of(PROGRESS_INTERVAL_BLOCK) {
                let bytes_processed = total_processed * self.block_size;
                let elapsed = state.start_time.elapsed().as_secs_f64();
                let mb_processed = bytes_processed as f64 / (1024.0 * 1024.0);
                let throughput = mb_processed / elapsed;
                println!("Processed: {mb_processed:.2} MiB ({total_processed} blocks, {throughput:.2} MiB/s)");
            }

            Ok(())
        })?;

        Ok(())
    }
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
