#!/usr/bin/env bash
# 按固定顺序执行调试、Sanitizer、发布安装和下游示例验证。

set -euo pipefail

OUT_DIR="evidence/er8"
while [[ $# -gt 0 ]]; do
	case "$1" in
		--out-dir)
			OUT_DIR="$2"
			shift 2
			;;
		*)
			echo "unknown argument: $1" >&2
			exit 2
			;;
	esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "$OUT_DIR" != /* ]]; then
	OUT_DIR="$ROOT/$OUT_DIR"
fi

mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/ci.log"
: >"$LOG"

log() {
	echo "[ci] $*" | tee -a "$LOG"
}

run_stage() {
	local label="$1"
	shift
	log "== stage: $label =="
	if ! "$@" >>"$LOG" 2>&1; then
		log "== FAILED: $label =="
		return 1
	fi
	log "== ok: $label =="
}

export ASAN_OPTIONS="detect_leaks=0:halt_on_error=1"
export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"

run_stage "dev-debug configure" cmake --preset dev-debug
run_stage "dev-debug build" cmake --build --preset dev-debug
run_stage "dev-debug ctest" ctest --preset dev-debug --output-on-failure

run_stage "asan-ubsan configure" cmake --preset asan-ubsan
run_stage "asan-ubsan build" cmake --build --preset asan-ubsan
run_stage "asan-ubsan ctest" ctest --preset asan-ubsan --output-on-failure

run_stage "release configure" cmake --preset release
run_stage "release build" cmake --build --preset release

INSTALL_PREFIX="$OUT_DIR/install"
rm -rf "$INSTALL_PREFIX"
run_stage "install" cmake --install build/dev-debug --prefix "$INSTALL_PREFIX"
INSTALL_PREFIX="$(cd "$INSTALL_PREFIX" && pwd)"
DEMO_BUILD="$OUT_DIR/consume_demo_build"
rm -rf "$DEMO_BUILD"
run_stage "consume_demo configure" \
	cmake -S examples/consume_demo -B "$DEMO_BUILD" \
	-DCMAKE_PREFIX_PATH="$INSTALL_PREFIX" -DCMAKE_BUILD_TYPE=Debug
run_stage "consume_demo build" cmake --build "$DEMO_BUILD"
run_stage "consume_demo run" "$DEMO_BUILD/consume_demo"

log "== ci.sh: ALL STAGES PASSED =="
log "install prefix: $INSTALL_PREFIX"
log "installed files:"
(cd "$INSTALL_PREFIX" && find . -type f | sort) | tee -a "$LOG"

echo "[ci] transcript: $LOG"
