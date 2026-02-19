#!/bin/bash -eu

declare -r INPUT_FILENAME="read-input-file"

# Should be twice RAM size
declare -r FILESIZE_GB="128"

filesize_bytes() {
    printf "%d" "$((FILESIZE_GB * 1024 * 1024 * 1024))"
}

prepare_input_file() {
    declare -r dd_input="$1"
    if [[ ! -f "${INPUT_FILENAME}" ]] || [[ $(filesize_bytes) -ne $(stat -c%s "${INPUT_FILENAME}") ]]; then
        rm -f "${INPUT_FILENAME}"
        printf "Creating input file of size %sG at %s\n" "${FILESIZE_GB}" "${INPUT_FILENAME}" >&2
        dd if="${dd_input}" of="${INPUT_FILENAME}" bs=1MiB count="$(filesize_bytes)B" status=progress
        sync "${INPUT_FILENAME}"
    else
        printf "Input file %s already exists, skipping creation\n" "${INPUT_FILENAME}" >&2
    fi
}

main() {
    declare -r input="${1:-urandom}"
    shift || true

    case "${input}" in
        urandom)
            prepare_input_file "/dev/urandom"
            ;;
        zero)
            prepare_input_file "/dev/zero"
            ;;
        *)
            printf "ERROR: Unknown input source %s\n" "${input}" >&2
            exit 1
            ;;
    esac
}

main "$@"