#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
runs=${1:-10}
preset=${2:-dev-debug}

if [[ ! "$runs" =~ ^[1-9][0-9]*$ ]]; then
	printf 'runs must be a positive integer\n' >&2
	exit 2
fi

cd "$repo_root"
cmake --build --preset "$preset" \
	--target shared_buffer_pool_integration_test edge_shared_buffer_pool_child

for ((run = 1; run <= runs; ++run)); do
	printf 'buffer generation regression run %d/%d (%s)\n' "$run" "$runs" "$preset"
	ctest --preset "$preset" -R '^shared_buffer_pool_er17$' --output-on-failure
done

printf 'buffer generation regression passed: runs=%d preset=%s repo=%s\n' \
	"$runs" "$preset" "$repo_root"
