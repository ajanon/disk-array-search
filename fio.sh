#!/bin/bash -eu

# Should be twice RAM size
declare -r FILESIZE_GB="128"
declare -r RUNTIME="60"

declare -r FIO_DIR=".fio"
declare -r INPUT_FILENAME="${FIO_DIR}/fio-input-file"
declare -r OUTPUT_DIR="${FIO_DIR}/results"
declare -r OUTPUT_CSV="${FIO_DIR}/results.csv"
declare -r OUTPUT_GRAPH="${FIO_DIR}/throughput_graph.png"

declare -ra BASE_ARGS=(
    --name=fio-seq-read
    --filename="${INPUT_FILENAME}"
    --rw=read
    --direct=1
    --numjobs=1
    --ioengine=libaio
    --group_reporting
    --time_based
    --runtime="${RUNTIME}"
    --readonly
    --allow_file_create=0
    --output-format=json
)

declare -ra BLOCK_SIZES=(
    4k
    8k
    16k
    32k
    64k
    128k
    256k
    512k
    1m
)

declare -ra IODEPTHS=(
    1
    2
    4
    8
    16
    32
    64
    128
    256
    512
    1024
)

filesize_bytes() {
    printf "%d" "$((FILESIZE_GB * 1024 * 1024 * 1024))"
}

prepare_input_file() {
    mkdir -p "${FIO_DIR}"
    if [[ ! -f "${INPUT_FILENAME}" ]] || [[ $(filesize_bytes) -ne $(stat -c%s "${INPUT_FILENAME}") ]]; then
        rm -f "${INPUT_FILENAME}"
        printf "Creating input file of size %sG at %s\n" "${FILESIZE_GB}" "${INPUT_FILENAME}" >&2
        dd if=/dev/urandom of="${INPUT_FILENAME}" bs=1MiB count="$(filesize_bytes)B" status=progress
        sync "${INPUT_FILENAME}"
    else
        printf "Input file %s already exists, skipping creation\n" "${INPUT_FILENAME}" >&2
    fi
}

fio() {
    command fio \
        --filesize="$(filesize_bytes)" \
        "${BASE_ARGS[@]}" \
        "$@" \
        >/dev/null
}

parse_results() {
    printf "iodepth,block_size,bandwidth_mib_s\n" > "${OUTPUT_CSV}"

    printf "\n=== FIO Results ===\n\n"
    printf "%-10s %-12s %-20s\n" "IODepth" "Block Size" "Bandwidth (MiB/s)"
    printf "%-10s %-12s %-20s\n" "-------" "----------" "----------------"

    for iodepth in "${IODEPTHS[@]}"; do
        for block_size in "${BLOCK_SIZES[@]}"; do
            output_file="${OUTPUT_DIR}/result_iodepth${iodepth}_bs${block_size}.json"
            if [[ -f "${output_file}" ]]; then
                # Get bandwidth in KiB/s and convert to MiB/s
                bw_kib=$(jq -r '.jobs[0].read.bw' "${output_file}")
                bw_mib=$(awk "BEGIN {printf \"%.2f\", ${bw_kib}/1024}")
                printf "%-10s %-12s %-20s\n" "${iodepth}" "${block_size}" "${bw_mib}"
                printf "%s,%s,%s\n" "${iodepth}" "${block_size}" "${bw_mib}" >> "${OUTPUT_CSV}"
            fi
        done
    done

    printf "\nResults saved in:\n" >&2
    printf "  CSV: %s\n" "${OUTPUT_CSV}" >&2
    printf "  JSON: %s\n" "${OUTPUT_DIR}" >&2
}

generate_graph() {
    (
        declare -r plot_script="$(mktemp --suffix=.py)"
        cleanup() {
            rm -f "${plot_script}"
        }
        trap cleanup EXIT INT TERM
        cat > "${plot_script}" << 'EOF'
#!/usr/bin/python3

import csv
import sys
from collections import defaultdict
import os
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use("Agg")
csv_file = os.getenv("OUTPUT_CSV")
output_graph = os.getenv("OUTPUT_GRAPH")

data = defaultdict(lambda: defaultdict(float))
block_sizes = []
iodepths = []

with open(csv_file, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        iodepth = int(row["iodepth"])
        block_size = row["block_size"]
        bandwidth = float(row["bandwidth_mib_s"])

        if iodepth not in iodepths:
            iodepths.append(iodepth)
        if block_size not in block_sizes:
            block_sizes.append(block_size)

        data[iodepth][block_size] = bandwidth

iodepths.sort()

x = np.arange(len(iodepths))
width = 0.8 / len(block_sizes)
multiplier = 0

fig, ax = plt.subplots(figsize=(14, 9))

colors = ["#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2",
          "#D55E00", "#CC79A7", "#999999", "#000000"]

patterns = ["/", "\\", "|", "-", "+", "x", "o", "O", "."]

# Plot bars for each block size
for i, block_size in enumerate(block_sizes):
    offset = width * multiplier
    values = [data[iodepth][block_size] for iodepth in iodepths]
    
    bars = ax.bar(x + offset, values, width,
                  label=block_size,
                  color=colors[i % len(colors)],
                  hatch=patterns[i % len(patterns)],
                  edgecolor="black",
                  linewidth=0.5)
    multiplier += 1

# Customize plot
ax.set_xlabel("IODepth (parallel requests)", fontsize=13, fontweight="bold")
ax.set_ylabel("Throughput (MiB/s)", fontsize=13, fontweight="bold")
ax.set_title("FIO Read Throughput by IODepth and Block Size", fontsize=16, fontweight="bold")
ax.set_xticks(x + width * (len(block_sizes) - 1) / 2)
ax.set_xticklabels(iodepths)
ax.legend(title="Block Size", loc="upper left", bbox_to_anchor=(1, 1),
          frameon=True, fancybox=True, shadow=True)
ax.grid(axis="y", alpha=0.3, linestyle="--")

# Adjust layout to prevent legend cutoff
plt.tight_layout()

# Save figure
plt.savefig(output_graph, dpi=150, bbox_inches="tight")
print(f"  Graph: {output_graph}", file=sys.stderr)
EOF
        export OUTPUT_CSV OUTPUT_GRAPH
        python3 "${plot_script}" 2>&1
    )
}

benchmark() {
    mkdir -p "${OUTPUT_DIR}"
    printf "\nRunning fio benchmarks...\n\n" >&2
    for iodepth in "${IODEPTHS[@]}"; do
        for block_size in "${BLOCK_SIZES[@]}"; do
            output_file="${OUTPUT_DIR}/result_iodepth${iodepth}_bs${block_size}.json"
            printf "Running: iodepth=%s bs=%s\n" "${iodepth}" "${block_size}" >&2
            fio --iodepth="${iodepth}" \
                --bs="${block_size}" \
                --output="${output_file}" \
                "$@"
        done
    done
}

main() {
    local command="${1:-all}"
    shift || true

    case "$command" in
        prepare)
            prepare_input_file
            ;;
        benchmark)
            benchmark "$@"
            ;;
        parse)
            parse_results
            ;;
        plot)
            generate_graph
            ;;
        all)
            prepare_input_file
            benchmark "$@"
            parse_results
            generate_graph
            ;;
        *)
            printf "Usage: %s [prepare|benchmark|parse|plot|all]\n" "$0" >&2
            printf "\n" >&2
            printf "Commands:\n" >&2
            printf "  prepare   - Create input file only\n" >&2
            printf "  benchmark - Run fio benchmarks only\n" >&2
            printf "  parse     - Parse existing results to CSV\n" >&2
            printf "  plot      - Generate graph from CSV\n" >&2
            printf "  all       - Run all steps (default)\n" >&2
            exit 1
            ;;
    esac
}

main "$@"
