use std::io::Read;
use std::path::Path;
use std::time::Instant;

use anyhow::Result;

use crate::SearchResult;

pub struct Sequential {
    pub block_size: usize,
}

/// Interval in blocks at which to print progress updates during the search
/// operation.
const PROGRESS_INTERVAL_BLOCK: usize = 1024;

impl Sequential {
    pub fn search(&self, input_path: impl AsRef<Path>, needle: u8) -> Result<SearchResult> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .append(false)
            .open(input_path.as_ref())?;

        let mut buffer = vec![0; self.block_size];

        let mut index = 0;
        let progress_interval_bytes = self.block_size * PROGRESS_INTERVAL_BLOCK;
        let start_time = Instant::now();

        loop {
            match file.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    // Search for needle using SIMD-optimized memchr
                    if let Some(index_in_block) = memchr::memchr(needle, &buffer[..n]) {
                        return Ok(SearchResult {
                            bytes_searched: index + index_in_block + 1,
                            found_at: Some(index + index_in_block),
                        });
                    }
                    index += n;

                    // Print progress every 1024 blocks
                    if index % progress_interval_bytes == 0 {
                        let elapsed = start_time.elapsed().as_secs_f64();
                        let mb_processed = index as f64 / (1024.0 * 1024.0);
                        let throughput = mb_processed / elapsed;
                        let block_count = index / self.block_size;
                        println!(
                            "Processed: {mb_processed:.2} MiB ({block_count} blocks, \
                             {throughput:.2} MiB/s)"
                        );
                    }
                },
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => (),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(SearchResult {
            bytes_searched: index,
            found_at: None,
        })
    }
}
