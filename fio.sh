#!/bin/bash -eu

declare -r RUNTIME="60"

# Should be twice RAM size and equal to what's used in prepare-input-file.sh
declare -r FILESIZE_GB="128"

declare -r FIO_DIR=".fio"
declare -r INPUT_FILENAME="read-input-file-rand"
declare -r OUTPUT_DIR="${FIO_DIR}/results"
declare -r OUTPUT_CSV="results/fio.csv"

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
            ./prepare_input_file.sh
            ;;
        benchmark)
            benchmark "$@"
            ;;
        parse)
            parse_results
            ;;
        all)
            ./prepare_input_file.sh
            benchmark "$@"
            parse_results
            ;;
        *)
            printf "Usage: %s [prepare|benchmark|parse|plot|all]\n" "$0" >&2
            printf "\n" >&2
            printf "Commands:\n" >&2
            printf "  prepare   - Create input file only\n" >&2
            printf "  benchmark - Run fio benchmarks only\n" >&2
            printf "  parse     - Parse existing results to CSV\n" >&2
            printf "  all       - Run all steps (default)\n" >&2
            exit 1
            ;;
    esac
}

main "$@"
