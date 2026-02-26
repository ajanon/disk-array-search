use std::num::ParseIntError;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use clap::{Parser, Subcommand, ValueEnum};

mod r#async;
mod parallel;
mod sequential;

fn parse_byte(s: &str) -> Result<u8, ParseIntError> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u8::from_str_radix(hex, 16)
    } else {
        s.parse::<u8>()
    }
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    /// Human-readable output on stdout
    Plain,
    /// CSV line on stdout; columns vary by subcommand (see --output-format
    /// help)
    Csv,
}

#[derive(Parser)]
#[command(name = "disk-array-search")]
#[command(
    about = "Search in sequential or parallel mode",
    long_about = "Search for a byte value in a file using sequential, parallel, or async \
                  I/O.\n\nExit codes:\n  0  Needle found\n  1  Error\n  2  Needle not found (no \
                  output is produced)"
)]
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

    /// Byte value to search for (decimal or hex with 0x prefix, e.g. 66 or
    /// 0x42)
    #[arg(long, default_value = "0x42", value_parser = parse_byte)]
    needle: u8,

    /// Output format (use -h for csv column details)
    #[arg(
        long,
        value_enum,
        default_value = "plain",
        long_help = "Output format.\n\nFor csv, columns depend on the subcommand:\n  sequential: \
                     command,needle,block_size_bytes,simd,bytes_searched,duration_secs\n  \
                     parallel:   \
                     command,needle,block_size_bytes,simd,parallelism,batch_multiplier,\
                     bytes_searched,duration_secs\n  async:      \
                     command,needle,block_size_bytes,simd,read_parallelism,search_parallelism,\
                     bytes_searched,duration_secs"
    )]
    output_format: OutputFormat,

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
async fn main() {
    match run().await {
        Ok(()) => {},
        Err(e) => {
            eprintln!("Error: {e:#}");
            std::process::exit(1);
        },
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    let block_size_bytes = cli.block_size.as_u64();
    let simd = !cli.no_simd;

    eprintln!("Input file: {}", cli.input_file.display());
    eprintln!("Block size: {block_size_bytes} bytes");
    eprintln!("Needle: 0x{:02x}", cli.needle);

    let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let res = match &cli.command {
        Commands::Sequential => {
            eprintln!("Running sequential search");
            eprintln!("SIMD: {simd}");
            let searcher = sequential::Sequential {
                block_size: usize::try_from(block_size_bytes)?,
                use_simd: simd,
            };
            searcher.search(&cli.input_file, cli.needle)?
        },
        Commands::Parallel {
            parallelism,
            batch_multiplier,
        } => {
            let (parallelism, batch_multiplier) = (*parallelism, *batch_multiplier);
            eprintln!("Running parallel (rayon) search");
            eprintln!("Parallelism: {parallelism}");
            eprintln!("Batch multiplier: {batch_multiplier}");
            eprintln!("SIMD: {simd}");
            let searcher = parallel::Parallel {
                block_size: usize::try_from(block_size_bytes)?,
                parallelism,
                batch_multiplier,
                use_simd: simd,
            };
            searcher.search(&cli.input_file, cli.needle)?
        },
        Commands::Async {
            read_parallelism,
            search_parallelism,
        } => {
            let (read_parallelism, search_parallelism) = (*read_parallelism, *search_parallelism);
            eprintln!("Running async (tokio) search");
            eprintln!("Read parallelism: {read_parallelism}");
            eprintln!("Search parallelism: {search_parallelism}");
            eprintln!("SIMD: {simd}");
            let searcher = r#async::Async {
                block_size: usize::try_from(block_size_bytes)?,
                read_parallelism,
                search_parallelism,
                use_simd: simd,
            };
            searcher.search(&cli.input_file, cli.needle).await?
        },
    };
    let end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    if res.found_at.is_none() {
        std::process::exit(2);
    }

    let duration = end
        .checked_sub(start)
        .ok_or(anyhow!("Duration calculation error"))?;
    let duration_secs = duration.as_secs_f64();

    match cli.output_format {
        OutputFormat::Plain => {
            println!("Bytes searched: {}", res.bytes_searched);
            println!("Time taken: {duration_secs} seconds");
            println!(
                "Throughput: {} MiB/s",
                (res.bytes_searched as f64) / (duration_secs * 1024.0 * 1024.0)
            );
            println!("Needle found at index {}", res.found_at.unwrap());
        },
        OutputFormat::Csv => {
            let common = format!(
                "0x{:02x},{},{},{}",
                cli.needle, block_size_bytes, simd, res.bytes_searched
            );
            match &cli.command {
                Commands::Sequential => {
                    println!("sequential,{common},{duration_secs}");
                },
                Commands::Parallel {
                    parallelism,
                    batch_multiplier,
                } => {
                    println!("parallel,{common},{parallelism},{batch_multiplier},{duration_secs}");
                },
                Commands::Async {
                    read_parallelism,
                    search_parallelism,
                } => {
                    println!(
                        "async,{common},{read_parallelism},{search_parallelism},{duration_secs}"
                    );
                },
            }
        },
    }

    Ok(())
}
