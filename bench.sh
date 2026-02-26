#!/bin/bash -eu

# Byte value to search for (hex with 0x prefix).
declare -r NEEDLE="0x42"

# Should be twice RAM size and equal to what's used in prepare-input-file.sh
declare -r FILESIZE_GB="128"

declare -r INPUT_FILE="read-input-file"
declare -r BINARY="./target/release/disk-array-search"

declare -r OUT_SEQUENTIAL="results/sequential.csv"
declare -r OUT_PARALLEL="results/parallel.csv"
declare -r OUT_ASYNC="results/async.csv"

declare -ra BLOCK_SIZES=(
    4k
    64k
    1m
)

# "" = simd enabled, "--no-simd" = simd disabled
declare -ra SIMD_MODES=("" "--no-simd")

declare -ra PARALLELISM_VALUES=(4 16 32)
declare -ra BATCH_MULTIPLIER_VALUES=(4 16 32)

declare -ra READ_PARALLELISM_VALUES=(4 16)
declare -ra SEARCH_PARALLELISM_VALUES=(4 16)

filesize_bytes() {
    printf "%d" "$((FILESIZE_GB * 1024 * 1024 * 1024))"
}

needle_byte() {
    # Convert NEEDLE (e.g. "0x42") to a printf-compatible escape (e.g. \x42).
    # Bash arithmetic natively handles hex literals like 0x42.
    declare decimal="$(( NEEDLE ))"
    printf "\\x$(printf '%02x' "${decimal}")"
}

# Runs sudo in the background to keep the authorization going.
#
# Arguments:
# 1: how long to sleep, default to 60s
sudo_keepalive() {
    declare -ri sudo_keepalive_sleep="${1:-60}"
    sudo -v
    while true; do
        sudo -v
        sleep "${sudo_keepalive_sleep}"
        kill -0 $$ || exit
    done 2>/dev/null &
}

drop_caches() { echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null; }

# Drops VM caches, then runs the binary with the given arguments.
# Stderr from the binary is silenced; stdout (the CSV line) is passed through.
run_bench() {
    drop_caches
    "${BINARY}" "$@" 2>/dev/null
}

prepare() {
    printf "==> Preparing input file...\n" >&2
    ./prepare-input-file.sh zero

    declare file_size
    file_size="$(stat -c '%s' "${INPUT_FILE}")"
    declare last_byte_offset="$(( file_size - 1 ))"

    printf "==> Writing needle (0x%02x) at offset %d...\n" \
        "$(( NEEDLE ))" "${last_byte_offset}" >&2
    printf "$(needle_byte)" \
        | dd of="${INPUT_FILE}" bs=1 seek="${last_byte_offset}" conv=notrunc 2>/dev/null

    printf "==> Input file ready.\n" >&2
}

benchmark_sequential() {
    # CSV header: command,needle,block_size_bytes,simd,bytes_searched,duration_secs
    printf "command,needle,block_size_bytes,simd,bytes_searched,duration_secs\n" \
        > "${OUT_SEQUENTIAL}"

    for block_size in "${BLOCK_SIZES[@]}"; do
        for simd_flag in "${SIMD_MODES[@]}"; do
            declare simd_label="true"
            if [[ "${simd_flag}" == "--no-simd" ]]; then simd_label="false"; fi
            printf "  sequential  bs=%-6s  simd=%s\n" \
                "${block_size}" "${simd_label}" >&2
            declare -a args=(
                --input-file="${INPUT_FILE}"
                --block-size="${block_size}"
                --needle="${NEEDLE}"
                --output-format=csv
            )
            [[ -n "${simd_flag}" ]] && args+=("${simd_flag}")
            run_bench "${args[@]}" sequential >> "${OUT_SEQUENTIAL}"
        done
    done
}

benchmark_parallel() {
    # CSV header: command,needle,block_size_bytes,simd,parallelism,batch_multiplier,bytes_searched,duration_secs
    printf "command,needle,block_size_bytes,simd,parallelism,batch_multiplier,bytes_searched,duration_secs\n" \
        > "${OUT_PARALLEL}"

    for block_size in "${BLOCK_SIZES[@]}"; do
        for parallelism in "${PARALLELISM_VALUES[@]}"; do
            for batch_mult in "${BATCH_MULTIPLIER_VALUES[@]}"; do
                for simd_flag in "${SIMD_MODES[@]}"; do
                    declare simd_label="true"
                    if [[ "${simd_flag}" == "--no-simd" ]]; then simd_label="false"; fi
                    printf "  parallel  bs=%-6s  p=%-4s  bm=%-4s  simd=%s\n" \
                        "${block_size}" "${parallelism}" "${batch_mult}" "${simd_label}" >&2
                    declare -a args=(
                        --input-file="${INPUT_FILE}"
                        --block-size="${block_size}"
                        --needle="${NEEDLE}"
                        --output-format=csv
                    )
                    [[ -n "${simd_flag}" ]] && args+=("${simd_flag}")
                    run_bench "${args[@]}" parallel \
                        --parallelism="${parallelism}" \
                        --batch-multiplier="${batch_mult}" \
                        >> "${OUT_PARALLEL}"
                done
            done
        done
    done
}

benchmark_async() {
    # CSV header: command,needle,block_size_bytes,simd,read_parallelism,search_parallelism,bytes_searched,duration_secs
    printf "command,needle,block_size_bytes,simd,read_parallelism,search_parallelism,bytes_searched,duration_secs\n" \
        > "${OUT_ASYNC}"

    for block_size in "${BLOCK_SIZES[@]}"; do
        for read_p in "${READ_PARALLELISM_VALUES[@]}"; do
            for search_p in "${SEARCH_PARALLELISM_VALUES[@]}"; do
                for simd_flag in "${SIMD_MODES[@]}"; do
                    declare simd_label="true"
                    if [[ "${simd_flag}" == "--no-simd" ]]; then simd_label="false"; fi
                    printf "  async  bs=%-6s  read_p=%-4s  search_p=%-4s  simd=%s\n" \
                        "${block_size}" "${read_p}" "${search_p}" "${simd_label}" >&2
                    declare -a args=(
                        --input-file="${INPUT_FILE}"
                        --block-size="${block_size}"
                        --needle="${NEEDLE}"
                        --output-format=csv
                    )
                    [[ -n "${simd_flag}" ]] && args+=("${simd_flag}")
                    run_bench "${args[@]}" async \
                        --read-parallelism="${read_p}" \
                        --search-parallelism="${search_p}" \
                        >> "${OUT_ASYNC}"
                done
            done
        done
    done
}

benchmark() {
    declare -a targets=("$@")
    if [[ ${#targets[@]} -eq 0 ]]; then
        targets=(sequential parallel async)
    fi

    mkdir -p "results"

    # Keep sudo alive in the background so that we can drop caches between runs
    # without re-prompting for password.
    sudo_keepalive
    for target in "${targets[@]}"; do
        case "${target}" in
            sequential)
                printf "\n==> Running sequential benchmarks...\n" >&2
                benchmark_sequential
                printf "  -> %s\n" "${OUT_SEQUENTIAL}" >&2
                ;;
            parallel)
                printf "\n==> Running parallel benchmarks...\n" >&2
                benchmark_parallel
                printf "  -> %s\n" "${OUT_PARALLEL}" >&2
                ;;
            async)
                printf "\n==> Running async benchmarks...\n" >&2
                benchmark_async
                printf "  -> %s\n" "${OUT_ASYNC}" >&2
                ;;
            *)
                printf "Unknown benchmark: %s (valid: sequential, parallel, async)\n" \
                    "${target}" >&2
                exit 1
                ;;
        esac
    done
}

generate_graphs() {
    printf "TODO" >&2
}

main() {
    declare command="${1:-all}"
    shift || true

    case "${command}" in
        prepare)
            prepare
            ;;
        benchmark)
            benchmark "$@"
            ;;
        plot)
            generate_graphs
            ;;
        all)
            prepare
            benchmark "$@"
            ;;
        *)
            printf "Usage: %s [prepare|benchmark|plot|all] [sequential|parallel|async ...]\n" "$0" >&2
            printf "\n" >&2
            printf "Commands:\n" >&2
            printf "  prepare    - Prepare input file with needle at last byte\n" >&2
            printf "  benchmark  - Run benchmarks (default: all; or pass subset: sequential parallel async)\n" >&2
            printf "  plot       - Generate graphs from CSV results\n" >&2
            printf "  all        - Run prepare + benchmark (default)\n" >&2
            exit 1
            ;;
    esac
}

main "$@"
