#!/usr/bin/env bash
# 按固定顺序执行调试、Sanitizer、发布安装和独立下游消费验证。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"
exec python3 "$ROOT/scripts/release_audit.py" "$@"
