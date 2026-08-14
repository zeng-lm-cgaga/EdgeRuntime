#!/usr/bin/env bash
# ci.sh — local release-audit gate (ER8 / design §26 completion definition).
#
# Re-runs, in one script on a clean tree, everything the release-audit milestone
# must prove before a release can be claimed:
#
#   1. dev-debug build + full ctest          (§26: clean build, all tests green)
#   2. asan-ubsan build + full ctest         (§26: ASan/UBSan no known failures)
#   3. release build (-Werror, failpoints off)  (§26: release compiles failpoints out)
#   4. install + standalone consume_demo     (§26: public API and target installable)
#
# The GitHub Actions workflow (.github/workflows/ci.yml) is the same gate on a
# clean runner; this script is the equivalent that can run on any dev machine
# with the contract toolchain (Ubuntu 22.04, GCC 11, CMake 3.22, Linux >= 5.15).
#
# Usage:
#   scripts/ci.sh [--out-dir DIR]        default evidence/er8
#
# Exit code is non-zero if any stage fails. A transcript is written to
# --out-dir/ci.log (and each stage's ctest log to a separate file).

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

# Fail fast and loudly: undefined behavior in any stage must not be masked.
export ASAN_OPTIONS="detect_leaks=0:halt_on_error=1"
export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"

# --- 1. dev-debug ---------------------------------------------------------
run_stage "dev-debug configure" cmake --preset dev-debug
run_stage "dev-debug build" cmake --build --preset dev-debug
run_stage "dev-debug ctest" ctest --preset dev-debug --output-on-failure

# --- 2. asan-ubsan --------------------------------------------------------
run_stage "asan-ubsan configure" cmake --preset asan-ubsan
run_stage "asan-ubsan build" cmake --build --preset asan-ubsan
run_stage "asan-ubsan ctest" ctest --preset asan-ubsan --output-on-failure

# --- 3. release -----------------------------------------------------------
run_stage "release configure" cmake --preset release
run_stage "release build" cmake --build --preset release

# --- 4. install + consume_demo -------------------------------------------
INSTALL_PREFIX="$OUT_DIR/install"
rm -rf "$INSTALL_PREFIX"
run_stage "install" cmake --install build/dev-debug --prefix "$INSTALL_PREFIX"
# CMAKE_PREFIX_PATH must be absolute: CMake resolves a relative prefix against
# the consume_demo build dir, not the repo root.
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
