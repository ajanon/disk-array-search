use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::SearchResult;

pub struct Async {
    pub block_size: usize,
    pub read_parallelism: u16,
    pub search_parallelism: u16,
}

/// Interval in blocks at which to print progress updates during the search
/// operation.
const PROGRESS_INTERVAL_BLOCK: usize = 1024;

/// An extent describing a region of the file to read.
#[derive(Debug, Clone)]
struct ReadExtent {
    offset: usize,
    length: usize,
}

/// A block of data read from the file, with its starting offset.
#[derive(Debug)]
struct Block {
    offset: usize,
    data: Vec<u8>,
}

/// Progress update messages for the printer task.
#[derive(Debug)]
enum ProgressUpdate {
    /// Bytes dispatched to the reading queue
    Dispatched(usize),
    /// Bytes read from disk
    Read(usize),
    /// Bytes processed by searcher workers
    Processed(usize),
}

impl Async {
    /// Async search for the needle in a file.
    pub async fn search(&self, input_path: impl AsRef<Path>, needle: u8) -> Result<SearchResult> {
        let input_path = input_path.as_ref();
        let file_metadata = tokio::fs::metadata(input_path).await?;
        let file_size = usize::try_from(file_metadata.len())?;

        // Channels for the pipeline
        let (extent_tx, extent_rx) =
            mpsc::channel::<ReadExtent>(self.read_parallelism as usize * 4);
        let (block_tx, block_rx) = mpsc::channel::<Block>(self.search_parallelism as usize * 2);
        let (result_tx, mut result_rx) = mpsc::channel::<SearchResult>(1);
        let (progress_tx, progress_rx) = mpsc::channel::<ProgressUpdate>(100);

        // Cancellation token to stop all workers when needle is found
        let cancel_token = CancellationToken::new();

        // Spawn printer task
        let printer_handle = {
            let cancel = cancel_token.clone();
            tokio::spawn(async move { printer_task(progress_rx, cancel).await })
        };

        // Spawn dispatcher task
        let dispatcher_handle = {
            let cancel = cancel_token.clone();
            let block_size = self.block_size;
            let progress_tx = progress_tx.clone();

            tokio::spawn(async move {
                dispatcher_task(file_size, block_size, extent_tx, progress_tx, cancel).await
            })
        };

        // Spawn reader workers
        let mut reader_handles = Vec::new();
        let extent_rx = Arc::new(tokio::sync::Mutex::new(extent_rx));

        for _ in 0..self.read_parallelism {
            let extent_rx = Arc::clone(&extent_rx);
            let block_tx = block_tx.clone();
            let progress_tx = progress_tx.clone();
            let cancel = cancel_token.clone();
            let input_path = input_path.to_path_buf();

            let handle = tokio::spawn(async move {
                reader_worker(input_path, extent_rx, block_tx, progress_tx, cancel).await
            });
            reader_handles.push(handle);
        }

        // Drop block_tx so channel closes when all readers are done
        drop(block_tx);

        // Spawn searcher workers
        let mut searcher_handles = Vec::new();
        let block_rx = Arc::new(tokio::sync::Mutex::new(block_rx));

        for worker_id in 0..self.search_parallelism {
            let block_rx = Arc::clone(&block_rx);
            let result_tx = result_tx.clone();
            let progress_tx = progress_tx.clone();
            let cancel = cancel_token.clone();

            let handle = tokio::spawn(async move {
                searcher_worker(worker_id, block_rx, result_tx, progress_tx, needle, cancel).await
            });
            searcher_handles.push(handle);
        }

        // Drop the original senders so channels can close when all workers are done
        drop(result_tx);
        drop(progress_tx);

        // Wait for first result (if needle found) or for all workers to complete
        let search_result = tokio::select! {
            result = result_rx.recv() => {
                // Needle found! Cancel all other workers
                cancel_token.cancel();
                result.unwrap_or(SearchResult {
                    bytes_searched: file_size,
                    found_at: None,
                })
            }
            () = async {
                // Wait for all tasks to complete
                let _ = dispatcher_handle.await;
                for handle in reader_handles {
                    let _ = handle.await;
                }
                for handle in searcher_handles {
                    let _ = handle.await;
                }
                let _ = printer_handle.await;
            } => {
                // All workers finished, no needle found
                SearchResult {
                    bytes_searched: file_size,
                    found_at: None,
                }
            }
        };

        Ok(search_result)
    }
}

/// Printer task: receives progress updates and displays aggregate statistics.
async fn printer_task(
    mut progress_rx: mpsc::Receiver<ProgressUpdate>,
    cancel: CancellationToken,
) -> Result<()> {
    let start_time = std::time::Instant::now();
    let mut total_dispatched = 0usize;
    let mut total_read = 0usize;
    let mut total_processed = 0usize;
    let mut dispatched_blocks = 0usize;
    let mut read_blocks = 0usize;
    let mut processed_blocks = 0usize;

    while let Some(update) = progress_rx.recv().await {
        if cancel.is_cancelled() {
            break;
        }

        match update {
            ProgressUpdate::Dispatched(bytes) => {
                total_dispatched += bytes;
                dispatched_blocks += 1;
            },
            ProgressUpdate::Read(bytes) => {
                total_read += bytes;
                read_blocks += 1;
            },
            ProgressUpdate::Processed(bytes) => {
                total_processed += bytes;
                processed_blocks += 1;
            },
        }

        // Print progress every PROGRESS_INTERVAL_BLOCK blocks
        if (dispatched_blocks + read_blocks + processed_blocks)
            .is_multiple_of(PROGRESS_INTERVAL_BLOCK)
        {
            let elapsed = start_time.elapsed().as_secs_f64();
            let mb_dispatched = total_dispatched as f64 / (1024.0 * 1024.0);
            let mb_read = total_read as f64 / (1024.0 * 1024.0);
            let mb_processed = total_processed as f64 / (1024.0 * 1024.0);
            let dispatch_throughput = mb_dispatched / elapsed;
            let read_throughput = mb_read / elapsed;
            let process_throughput = mb_processed / elapsed;

            println!(
                "Progress: Dispatched {:.2} MiB ({} blocks, {:.2} MiB/s) | Read {:.2} MiB ({} \
                 blocks, {:.2} MiB/s) | Processed {:.2} MiB ({} blocks, {:.2} MiB/s)",
                mb_dispatched,
                dispatched_blocks,
                dispatch_throughput,
                mb_read,
                read_blocks,
                read_throughput,
                mb_processed,
                processed_blocks,
                process_throughput
            );
        }
    }

    Ok(())
}

/// Dispatcher task: generates read extents and sends them to the reading queue.
async fn dispatcher_task(
    file_size: usize,
    block_size: usize,
    extent_tx: mpsc::Sender<ReadExtent>,
    progress_tx: mpsc::Sender<ProgressUpdate>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut offset = 0;

    while offset < file_size {
        if cancel.is_cancelled() {
            break;
        }

        let length = std::cmp::min(block_size, file_size - offset);
        let extent = ReadExtent { offset, length };

        if extent_tx.send(extent).await.is_err() {
            // Channel closed, stop dispatching
            break;
        }

        offset += length;

        // Send progress update
        let _ = progress_tx.send(ProgressUpdate::Dispatched(length)).await;
    }

    Ok(())
}

/// Reader worker: pulls extents from queue and reads those file regions.
async fn reader_worker(
    input_path: std::path::PathBuf,
    extent_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<ReadExtent>>>,
    block_tx: mpsc::Sender<Block>,
    progress_tx: mpsc::Sender<ProgressUpdate>,
    cancel: CancellationToken,
) -> Result<()> {
    // Each worker opens its own file handle for parallel I/O
    let mut file = File::open(input_path).await?;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        // Get next extent from queue
        let extent = {
            let mut rx = extent_rx.lock().await;
            rx.recv().await
        };

        match extent {
            Some(extent) => {
                // Seek to the offset and read the data
                file.seek(std::io::SeekFrom::Start(extent.offset as u64))
                    .await?;

                let mut buffer = vec![0u8; extent.length];
                let _bytes_read = file.read_exact(&mut buffer).await?;

                let block_size = buffer.len();
                let block = Block {
                    offset: extent.offset,
                    data: buffer,
                };

                // Send progress update after reading
                let _ = progress_tx.send(ProgressUpdate::Read(block_size)).await;

                if block_tx.send(block).await.is_err() {
                    // Channel closed, stop reading
                    break;
                }
            },
            None => {
                // Channel closed, no more extents
                break;
            },
        }
    }

    Ok(())
}

/// Searcher worker: receives blocks from channel and searches for needle.
async fn searcher_worker(
    worker_id: u16,
    block_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Block>>>,
    result_tx: mpsc::Sender<SearchResult>,
    progress_tx: mpsc::Sender<ProgressUpdate>,
    needle: u8,
    cancel: CancellationToken,
) -> Result<()> {
    loop {
        if cancel.is_cancelled() {
            break;
        }

        // Get next block from channel
        let block = {
            let mut rx = block_rx.lock().await;
            rx.recv().await
        };

        match block {
            Some(block) => {
                let block_size = block.data.len();

                // Search for needle in this block using SIMD-optimized memchr
                if let Some(index_in_block) = memchr::memchr(needle, &block.data) {
                    let found_at = block.offset + index_in_block;
                    let result = SearchResult {
                        bytes_searched: found_at + 1,
                        found_at: Some(found_at),
                    };
                    println!("Worker {worker_id} found needle at offset {found_at}");
                    let _ = result_tx.send(result).await;
                    return Ok(());
                }

                // Send progress update after processing the block
                let _ = progress_tx
                    .send(ProgressUpdate::Processed(block_size))
                    .await;
            },
            None => {
                // Channel closed, no more blocks
                break;
            },
        }
    }

    Ok(())
}
