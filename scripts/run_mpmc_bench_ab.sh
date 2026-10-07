#!/usr/bin/env bash
# Run the v1 MPMC queue under busy/blocking and retry-policy A/B caller modes.

set -euo pipefail

usage() {
	cat <<'USAGE'
usage: scripts/run_mpmc_bench_ab.sh [--out-dir DIR] [--preset NAME]
                                     [--runs N] [--timeout-ms N]
                                     [--wait-policy busy|blocking|both]

Runs the selected wait policy for 64/1024/4096-byte payloads with 1P1C and 2P2C.
busy runs both yield and spin; blocking uses yield as its fixed retry label. 'both' runs
all three comparable modes.
The default output directory is temporary. The script never stages or modifies Git files.
USAGE
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
build_preset="${EDGE_RUNTIME_BENCH_PRESET:-dev-debug}"
output_dir=""
runs=1
timeout_ms=10000
wait_policy_selector=busy

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
		--runs)
			if [[ $# -lt 2 ]]; then
				echo "--runs requires a positive integer" >&2
				exit 2
			fi
			runs="$2"
			shift 2
			;;
		--timeout-ms)
			if [[ $# -lt 2 ]]; then
				echo "--timeout-ms requires a positive integer" >&2
				exit 2
			fi
			timeout_ms="$2"
			shift 2
			;;
		--wait-policy)
			if [[ $# -lt 2 ]]; then
				echo "--wait-policy requires busy, blocking, or both" >&2
				exit 2
			fi
			wait_policy_selector="$2"
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

if [[ ! "$runs" =~ ^[1-9][0-9]*$ || ! "$timeout_ms" =~ ^[1-9][0-9]*$ ]]; then
	echo "--runs and --timeout-ms must be positive integers" >&2
	exit 2
fi
if [[ "$wait_policy_selector" != "busy" && "$wait_policy_selector" != "blocking" &&
	"$wait_policy_selector" != "both" ]]; then
	echo "--wait-policy must be busy, blocking, or both" >&2
	exit 2
fi

cd -- "$repo_root"

if [[ -z "$output_dir" ]]; then
	temp_parent="${TMPDIR:-/tmp}"
	output_dir="$(mktemp -d "${temp_parent%/}/edgeruntime-mpmc-ab.XXXXXX")"
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

matrix_log="$(mktemp "$output_dir/mpmc_ab.XXXXXX.log")"

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

check_result() {
	local result_line="$1"
	local policy="$2"
	local wait_mode="$3"
	local payload_size="$4"
	local producers="$5"
	local consumers="$6"
	local label correctness retry wait expected published delivered recovery errors missed
	label="$(field_value label "$result_line")"
	correctness="$(field_value correctness "$result_line")"
	retry="$(field_value retry_policy "$result_line")"
	wait="$(field_value wait_policy "$result_line")"
	expected="$(field_value expected "$result_line")"
	published="$(field_value published "$result_line")"
	delivered="$(field_value delivered "$result_line")"
	recovery="$(field_value recovery "$result_line")"
	errors="$(field_value errors "$result_line")"
	missed="$(field_value spsc_missed_samples "$result_line")"
	if [[ "$label" != "VM_ONLY" || "$correctness" != "PASS" ||
		"$retry" != "$policy" || "$wait" != "$wait_mode" ||
		"$expected" != "$published" ||
		"$published" != "$delivered" || "$recovery" != "0" || "$errors" != "0" ||
		"$missed" != "NOT_APPLICABLE" ||
		"$(field_value payload_bytes "$result_line")" != "$payload_size" ||
		"$(field_value producers "$result_line")" != "$producers" ||
		"$(field_value consumers "$result_line")" != "$consumers" ]]; then
		echo "correctness gate failed: $result_line" >&2
		exit 1
	fi
	printf '%s\n' "$result_line"
}

run_case() {
	local policy="$1"
	local wait_mode="$2"
	local payload_size="$3"
	local producers="$4"
	local consumers="$5"
	local messages="$6"
	local before_csv run_output result_line
	local -a result_lines

	before_csv="$(count_csv)"
	echo "[mpmc-ab] wait_policy=$wait_mode retry_policy=$policy ${payload_size}B "\
"${producers}P${consumers}C runs=$runs"
	run_output="$($benchmark \
		--retry-policy "$policy" \
		--wait-policy "$wait_mode" \
		--payload-size "$payload_size" \
		--producers "$producers" \
		--consumers "$consumers" \
		--messages "$messages" \
		--capacity 8 \
		--runs "$runs" \
		--timeout-ms "$timeout_ms" \
		--out-dir "$output_dir" 2>&1 | tee -a "$matrix_log")"
	mapfile -t result_lines < <(printf '%s\n' "$run_output" | awk '$1 == "RESULT" { print }')
	if (( ${#result_lines[@]} != runs )); then
		echo "expected $runs RESULT lines, got ${#result_lines[@]}" >&2
		exit 1
	fi
	for result_line in "${result_lines[@]}"; do
		check_result "$result_line" "$policy" "$wait_mode" "$payload_size" "$producers" "$consumers"
	done
	local after_csv=$((before_csv + runs))
	if [[ "$(count_csv)" != "$after_csv" ]]; then
		echo "expected $runs new CSV files for wait_policy=$wait_mode retry_policy=$policy "\
		"(before=$before_csv)" >&2
		exit 1
	fi
}

run_wait_mode() {
	local wait_mode="$1"
	for policy in yield spin; do
		if [[ "$wait_mode" == "blocking" && "$policy" == "spin" ]]; then continue; fi
		for payload_size in 64 1024 4096; do
			for mode in 1 2; do
				if (( mode == 1 )); then
					run_case "$policy" "$wait_mode" "$payload_size" 1 1 24
				else
					run_case "$policy" "$wait_mode" "$payload_size" 2 2 16
				fi
			done
		done
	done
}

if [[ "$wait_policy_selector" == "both" ]]; then
	run_wait_mode busy
	run_wait_mode blocking
else
	run_wait_mode "$wait_policy_selector"
fi

echo "[mpmc-ab] matrix passed"
echo "[mpmc-ab] output directory: $output_dir"
echo "[mpmc-ab] log: $matrix_log"
