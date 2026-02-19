use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use clap::{Parser, Subcommand};

mod parallel;
mod sequential;

const NEEDLE: u8 = 0x42;
const O_DIRECT: i32 = 0x4000;

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

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run search sequentially
    Sequential,

    /// Run search in parallel
    Parallel {
        /// Number of parallel workers
        #[arg(long)]
        parallelism: u8,
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

/// Trait for searchers that can perform a search operation on a file.
trait Searcher {
    /// Searches for the needle in the specified input file.
    fn search(&self, input: impl Read, needle: u8) -> Result<SearchResult>;
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    println!("Input file: {}", cli.input_file.display());
    println!("Block size: {} bytes", cli.block_size.as_u64());

    let searcher = match cli.command {
        Commands::Sequential => {
            println!("Running sequential search");
            sequential::Sequential {
                block_size: usize::try_from(cli.block_size.as_u64())?,
            }
        },
        Commands::Parallel { parallelism } => {
            println!("Running parallel search");
            println!("Parallelism: {parallelism}");
            todo!();
        },
    };

    std::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .custom_flags(O_DIRECT)
        .open(&cli.input_file)?;
    let file = std::fs::File::open(&cli.input_file)?;
    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let res = searcher.search(file, NEEDLE)?;
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
