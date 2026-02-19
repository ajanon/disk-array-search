use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use clap::{Parser, Subcommand};

mod r#async;
mod parallel;
mod sequential;

const NEEDLE: u8 = 0x42;

#[derive(Parser)]
#[command(name = "disk-array-search")]
#[command(about = "Search in sequential or parallel mode", long_about = None)]
struct Cli {
    /// Input file path
    #[arg(long)]
    input_file: PathBuf,

    /// Block size (e.g., 4k, 8k, 16k, 32k, 64k, 128k, 256k, 512k, 1m)
    #[arg(long)]
    block_size: ByteSize,

    /// Disable SIMD optimization for search
    #[arg(long)]
    no_simd: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run search sequentially
    Sequential,

    /// Run search in parallel using rayon
    Parallel {
        /// Number of parallel workers
        #[arg(long)]
        parallelism: usize,
        /// Batch size multiplier (blocks per thread in each batch)
        #[arg(long, default_value = "16")]
        batch_multiplier: usize,
    },

    /// Run search asynchronously using tokio
    Async {
        /// Number of parallel readers
        #[arg(long)]
        read_parallelism: u16,
        /// Number of parallel searchers
        #[arg(long)]
        search_parallelism: u16,
    },
}

/// Result of a search operation, including the number of bytes searched and the
/// index where the needle was found (if any).
struct SearchResult {
    /// The number of bytes searched during the operation.
    bytes_searched: usize,
    /// The index in the file where the needle was found, or None if not found.
    found_at: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    println!("Input file: {}", cli.input_file.display());
    println!("Block size: {} bytes", cli.block_size.as_u64());

    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let res = match cli.command {
        Commands::Sequential => {
            println!("Running sequential search");
            println!("SIMD: {}", !cli.no_simd);
            let searcher = sequential::Sequential {
                block_size: usize::try_from(cli.block_size.as_u64())?,
                use_simd: !cli.no_simd,
            };
            searcher.search(&cli.input_file, NEEDLE)?
        },
        Commands::Parallel {
            parallelism,
            batch_multiplier,
        } => {
            println!("Running parallel (rayon) search");
            println!("Parallelism: {parallelism}");
            println!("Batch multiplier: {batch_multiplier}");
            println!("SIMD: {}", !cli.no_simd);
            let searcher = parallel::Parallel {
                block_size: usize::try_from(cli.block_size.as_u64())?,
                parallelism,
                batch_multiplier,
                use_simd: !cli.no_simd,
            };
            searcher.search(&cli.input_file, NEEDLE)?
        },
        Commands::Async {
            read_parallelism,
            search_parallelism,
        } => {
            println!("Running async (tokio) search");
            println!("Read parallelism: {read_parallelism}");
            println!("Search parallelism: {search_parallelism}");
            println!("SIMD: {}", !cli.no_simd);
            let searcher = r#async::Async {
                block_size: usize::try_from(cli.block_size.as_u64())?,
                read_parallelism,
                search_parallelism,
                use_simd: !cli.no_simd,
            };
            searcher.search(&cli.input_file, NEEDLE).await?
        },
    };
    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    let duration = end
        .checked_sub(start)
        .ok_or(anyhow!("Duration calculation error"))?;
    println!("Bytes searched: {}", res.bytes_searched);
    println!("Time taken: {} seconds", duration.as_secs_f64());
    println!(
        "Throughput: {} MiB/s",
        (res.bytes_searched as f64) / (duration.as_secs_f64() * 1024.0 * 1024.0)
    );
    match res.found_at {
        Some(index) => println!("Needle found at index {index}"),
        None => println!("Needle not found"),
    }
    Ok(())
}
