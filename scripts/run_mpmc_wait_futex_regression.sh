#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
cd "$repo_root"

preset="${1:-dev-debug}"
output_dir="${2:-}"
if [[ -z "$output_dir" ]]; then
	output_dir="$(mktemp -d /tmp/edgeruntime-mpmc-wait.XXXXXX)"
else
	mkdir -p "$output_dir"
fi

cmake --build --preset "$preset" \
	--target edge_runtime mpmc_wait_test edge_mpmc_wait_child

test_binary="$repo_root/build/$preset/test/integration/mpmc_wait_test"
child_binary="$repo_root/build/$preset/test/integration/edge_mpmc_wait_child"
trace_prefix="$output_dir/futex.%p"

if [[ ! -x "$test_binary" || ! -x "$child_binary" ]]; then
	echo "missing wait test binaries under $repo_root/build/$preset" >&2
	exit 2
fi
if ! command -v strace >/dev/null 2>&1; then
	echo "strace is required for the external futex evidence check" >&2
	exit 2
fi

set +e
GTEST_FILTER='MpmcWait.EmptyQueueWakesCrossProcessConsumer' \
	timeout --foreground 30s \
	strace -ff -ttt -T -e trace=futex -o "$trace_prefix" \
		"$test_binary" "$child_binary" 2>&1 | tee "$output_dir/gtest.log"
command_status=${PIPESTATUS[0]}
set -e
if [[ "$command_status" -ne 0 ]]; then
	echo "wait test under strace failed with exit code $command_status" >&2
	exit "$command_status"
fi

trace_files=("$output_dir"/futex.*)
if [[ ! -e "${trace_files[0]}" ]]; then
	echo "strace produced no futex transcript" >&2
	exit 1
fi
if ! rg -q 'FUTEX_WAIT,.*\) = 0' "${trace_files[@]}"; then
	echo "no successful process-shared FUTEX_WAIT was observed" >&2
	exit 1
fi
if ! rg -q 'FUTEX_WAKE,.*\) = [1-9][0-9]*' "${trace_files[@]}"; then
	echo "no successful process-shared FUTEX_WAKE was observed" >&2
	exit 1
fi
if rg -q 'FUTEX_TRACE_DROPPED' "${trace_files[@]}"; then
	echo "futex trace overflowed and dropped events" >&2
	exit 1
fi
echo "FUTEX_EVIDENCE=PASS output_dir=$output_dir"
