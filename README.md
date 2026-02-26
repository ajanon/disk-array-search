# disk-array-search

Benchmarks sequential, parallel (rayon), and async (tokio) byte-search strategies over a large file, and compares them against raw I/O throughput measured by fio.

The goal is to identify which search strategy saturates disk bandwidth most effectively at various block sizes and parallelism levels.

## Setup

1. Set the file size to **twice your RAM** in the following scripts (keep all three in sync):
   - `prepare-input-file.sh` → `FILESIZE_GB`
   - `fio.sh` → `FILESIZE_GB`
   - `bench.sh` → `FILESIZE_GB`

2. Build the binary:
   ```bash
   cargo build --release
   ```

## Running

1. **fio baseline** — measures raw sequential read throughput across block sizes and I/O depths:
   ```bash
   ./fio.sh
   ```
   Results are written to `results/fio.csv`. Data is plotted in
   `results/fio_throughput_graph.png`.

2. **Search benchmarks** — runs sequential, parallel, and async searchers across block sizes and parallelism configurations:
   ```bash
   ./bench.sh
   ```
   Results are written to `results/sequential.csv`, `results/parallel.csv`,
   `results/parallel_l3.csv`, and `results/async.csv`.

   Before each run, the VM caches are dropped.

## Results

Results are analyzed with the [analysis.ipynb](analysis.ipynb) Jupyter notebook.

**Setup (one-time):**
```bash
sudo apt install python3-notebook python3-pandas python3-matplotlib
```

**Open the notebook:**
```bash
jupyter notebook analysis.ipynb
```

Then run all cells (`Run → Run All Cells`). Each section loads and plots the CSV data from the `results/` directory:

- **fio baseline** — throughput by block size and I/O depth
- **Sequential** — SIMD vs no-SIMD throughput
- **Parallel** — throughput by block size, parallelism, number of readers, batch multiplier, and thread pinning
- **Parallel L3-aware** — large block sizes sized to the L3 cache, varying reader count, pinned vs unpinned
- **Async** — throughput by block size, read/search parallelism
- **Comparison** — best throughput per mode overlaid on the fio baseline

## Architecture

Please note that the architecture below has been balanced specifically for my
computer.

### Sequential

The simplest mode. A single thread reads the file block by block using standard
`Read::read`, filling a fixed-size buffer on each iteration. Each buffer is
scanned for the needle using either a scalar byte-by-byte walk or `memchr`
(which uses SIMD under the hood when available). There is no concurrency: I/O
and search alternate on the same thread.

### Async

Built on Tokio with a three-stage pipeline connected by bounded MPSC channels:

1. **Dispatcher**: a single task walks the file address space and emits
   `ReadExtent` records (offset + length) into a channel. It stops early if
   the cancellation token fires.

2. **Reader workers** (`read_parallelism` tasks): each worker pulls extents
   from a shared `Mutex<Receiver>`, opens its own file handle, reads the
   extent with `AsyncReadExt`, and forwards the resulting `Block` (offset +
   data) into a second channel.

3. **Searcher workers** (`search_parallelism` tasks): each worker pulls
   blocks from a second shared `Mutex<Receiver>` and scans for the needle.
   When found, it sends the result and fires the cancellation token, which
   propagates to all other workers.

The two parallelism knobs are independent: reader and searcher counts can be
tuned separately to balance I/O latency against CPU utilisation.

### Parallel

Built on Rayon with a multi-reader, double-buffered pipeline. The file is
split into `num_readers` contiguous segments; each segment is processed
independently by a dedicated **reader thread** and a private **Rayon worker
pool**, all racing to find the needle.

#### Per-segment pipeline (double-buffering)

Each reader segment runs this two-thread pipeline:

```
IO thread ──[sync_channel(1)]──► compute loop ──► pool.install(search_batch)
```

The IO thread reads the next batch while the Rayon workers are still
processing the current one. The channel capacity of 1 means the IO thread
stays exactly one batch ahead — it blocks as soon as the compute loop falls
behind, keeping memory usage bounded to two batch buffers per segment.

With `num_readers=1` (the default) there is a single segment and a single
such pipeline; this alone recovers the IO/CPU overlap that a naive
read-then-process loop would miss.

#### Segment and worker allocation

The `parallelism` workers are distributed across segments as evenly as
possible; if the division is not exact, the first segments each get one extra
worker. Each segment's batch contains `workers_in_segment × batch_multiplier`
blocks.

The optional `--l3-cache-size` flag computes the multiplier automatically to
size the total batch to ~75% of the total L3 cache, so that the data each
worker touches per batch fits in its die's cache.

#### Thread pinning and topology

When `--pin-threads` is set, readers and their worker pools are pinned to
physical dies (read from `/sys/devices/system/cpu/*/topology/die_id`):

- Segments are assigned to dies round-robin (segment 0 → die 0, segment 1 →
  die 1, segment 2 → die 0, …).
- Within each die, the **reader thread** is pinned to the first core; the
  **Rayon workers** are pinned to the remaining cores, wrapping if the die
  has fewer cores than workers.

This means the data a reader brings in from disk lands in the L3 cache shared
by its workers, and workers never incur cross-die snoop traffic. With
`num_readers ≥ num_dies`, each die gets its own reader and worker pool, fully
localising both the I/O and the compute to that die's cache domain.

#### Early termination

A single `AtomicBool` is shared across all segments. Every worker checks it
before scanning each block; when any worker finds the needle it sets the flag,
and every other worker (across all segments) stops at its next block
boundary. The IO thread for each segment also checks the flag before issuing
each `pread`, so readers stop prefetching as soon as the result is known.

