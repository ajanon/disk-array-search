use std::io::Read;

use anyhow::Result;

use crate::{SearchResult, Searcher};

pub struct Sequential {
    pub block_size: usize,
}

/// Interval in blocks at which to print progress updates during the search
/// operation.
const PROGRESS_INTERVAL_BLOCK: usize = 1024;

impl Searcher for Sequential {
    fn search(&self, mut input: impl Read, needle: u8) -> Result<SearchResult> {
        let mut buffer = vec![0; self.block_size];

        let mut index = 0;
        let progress_interval_bytes = self.block_size * PROGRESS_INTERVAL_BLOCK;

        loop {
            match input.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    for (index_in_block, byte) in buffer[..n].iter().enumerate() {
                        if byte == &needle {
                            return Ok(SearchResult {
                                bytes_searched: index + index_in_block + 1,
                                found_at: Some(index + index_in_block),
                            });
                        }
                    }
                    index += n;

                    // Print progress every 128 blocks
                    if index % progress_interval_bytes == 0 {
                        let mb_processed = index as f64 / (1024.0 * 1024.0);
                        let block_count = index / self.block_size;
                        println!("Processed: {mb_processed:.2} MiB ({block_count} blocks)",);
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
