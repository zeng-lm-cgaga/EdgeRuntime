#!/usr/bin/env bash
# Build and run the six supported MPMC benchmark matrix cases.

set -euo pipefail

usage() {
	cat <<'USAGE'
usage: scripts/run_mpmc_bench_matrix.sh [--out-dir DIR] [--preset NAME]

The default output directory is a new temporary directory. CSV and the matrix
log are written there; this script never stages or modifies Git files.
USAGE
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
build_preset="${EDGE_RUNTIME_BENCH_PRESET:-dev-debug}"
output_dir=""

while [[ $# -gt 0 ]]; do
	case "$1" in
		--out-dir)
			if [[ $# -lt 2 ]]; then
				echo "--out-dir requires a directory" >&2
				exit 2
			fi
			output_dir="$2"
			shift 2
			;;
		--preset)
			if [[ $# -lt 2 ]]; then
				echo "--preset requires a CMake preset" >&2
				exit 2
			fi
			build_preset="$2"
			shift 2
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "unknown argument: $1" >&2
			usage >&2
			exit 2
			;;
	esac
done

cd -- "$repo_root"

if [[ -z "$output_dir" ]]; then
	temp_parent="${TMPDIR:-/tmp}"
	output_dir="$(mktemp -d "${temp_parent%/}/edgeruntime-mpmc-bench.XXXXXX")"
elif [[ "$output_dir" != /* ]]; then
	output_dir="$repo_root/$output_dir"
fi
mkdir -p -- "$output_dir"

cmake --preset "$build_preset"
cmake --build --preset "$build_preset" --target edge_mpmc_bench

benchmark="$repo_root/build/$build_preset/tools/edge_mpmc_bench"
if [[ ! -x "$benchmark" ]]; then
	echo "benchmark executable was not produced: $benchmark" >&2
	exit 1
fi

matrix_log="$(mktemp "$output_dir/mpmc_matrix.XXXXXX.log")"

count_csv() {
	local csv_files=()
	shopt -s nullglob
	csv_files=("$output_dir"/*.csv)
	printf '%s\n' "${#csv_files[@]}"
}

field_value() {
	local key="$1"
	local line="$2"
	local token
	for token in $line; do
		if [[ "$token" == "${key}="* ]]; then
			printf '%s\n' "${token#*=}"
			return 0
		fi
	done
	return 1
}

run_case() {
	local payload_size="$1"
	local producers="$2"
	local consumers="$3"
	local messages="$4"
	local before_csv
	local after_csv
	local run_output
	local result_line
	local label
	local correctness
	local expected
	local published
	local delivered
	local reported_payload_size
	local reported_producers
	local reported_consumers
	local missed_samples

	before_csv="$(count_csv)"
	echo "[mpmc-bench] ${payload_size}B ${producers}P${consumers}C"
	run_output="$(
		"$benchmark" \
			--payload-size "$payload_size" \
			--producers "$producers" \
			--consumers "$consumers" \
			--messages "$messages" \
			--capacity 8 \
			--timeout-ms 10000 \
			--out-dir "$output_dir" 2>&1 | tee -a "$matrix_log"
	)"
	result_line="$(printf '%s\n' "$run_output" | awk '$1 == "RESULT" { line = $0 } END { print line }')"
	if [[ -z "$result_line" ]]; then
		echo "benchmark did not emit a RESULT line" >&2
		exit 1
	fi

	label="$(field_value label "$result_line")"
	correctness="$(field_value correctness "$result_line")"
	expected="$(field_value expected "$result_line")"
	published="$(field_value published "$result_line")"
	delivered="$(field_value delivered "$result_line")"
	reported_payload_size="$(field_value payload_bytes "$result_line")"
	reported_producers="$(field_value producers "$result_line")"
	reported_consumers="$(field_value consumers "$result_line")"
	missed_samples="$(field_value spsc_missed_samples "$result_line")"
	if [[ "$label" != "VM_ONLY" || "$correctness" != "PASS" ||
		"$expected" != "$published" || "$published" != "$delivered" ||
		"$reported_payload_size" != "$payload_size" ||
		"$reported_producers" != "$producers" ||
		"$reported_consumers" != "$consumers" ||
		"$missed_samples" != "NOT_APPLICABLE" ]]; then
		echo "correctness gate failed: $result_line" >&2
		exit 1
	fi
	printf '%s\n' "$result_line"

	after_csv="$(count_csv)"
	if (( after_csv != before_csv + 1 )); then
		echo "expected one new CSV (before=$before_csv after=$after_csv)" >&2
		exit 1
	fi
}

for payload_size in 64 1024 4096; do
	for mode in 1 2; do
		if (( mode == 1 )); then
			run_case "$payload_size" 1 1 24
		else
			run_case "$payload_size" 2 2 16
		fi
	done
done

echo "[mpmc-bench] matrix passed"
echo "[mpmc-bench] output directory: $output_dir"
echo "[mpmc-bench] log: $matrix_log"
