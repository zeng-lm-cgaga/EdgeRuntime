#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
compat_helper="$script_dir/self_ipcs_compat.py"
cd "$repo_root"

preset="${1:-dev-debug}"
output_dir="${2:-}"
if [[ -z "$output_dir" ]]; then
	output_dir="$(mktemp -d /tmp/selfipcs-mpmc-wait.XXXXXX)"
else
	mkdir -p "$output_dir"
fi

build_dir="$(python3 "$compat_helper" build-dir "$repo_root" "$preset")"
library_target="$(python3 "$compat_helper" target "$build_dir" self_ipcs)"
child_target="$(python3 "$compat_helper" target "$build_dir" self_ipcs_mpmc_wait_child)"
cmake --build --preset "$preset" \
	--target "$library_target" mpmc_wait_test "$child_target"

test_binary="$(python3 "$compat_helper" binary "$build_dir/test/integration" mpmc_wait_test)"
child_binary="$(python3 "$compat_helper" binary "$build_dir/test/integration" self_ipcs_mpmc_wait_child)"
trace_prefix="$output_dir/futex.%p"

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
