#!/usr/bin/env python3
"""Resolve SelfIPCs artifacts while keeping historical EdgeRuntime evidence readable."""

from __future__ import annotations

import argparse
import os
from pathlib import Path


def canonical_name(name: str) -> str:
    if name == "edge_runtime_platform_probe":
        return "self_ipcs_platform_probe"
    if name == "edge_runtime":
        return "self_ipcs"
    if name.startswith("edge_"):
        return "self_ipcs_" + name.removeprefix("edge_")
    return name


def artifact_names(name: str) -> tuple[str, ...]:
    canonical = canonical_name(name)
    if canonical == "self_ipcs_platform_probe":
        return canonical, "edge_runtime_platform_probe"
    if canonical == "self_ipcs":
        return canonical, "edge_runtime"
    if canonical.startswith("self_ipcs_"):
        return canonical, "edge_" + canonical.removeprefix("self_ipcs_")
    return (canonical,)


def resolve_artifact(directory: Path, names: tuple[str, ...], *, executable: bool = False) -> Path:
    for name in names:
        path = directory / name
        if path.is_file() and (not executable or os.access(path, os.X_OK)):
            return path
    raise FileNotFoundError(f"missing {'executable' if executable else 'artifact'} in {directory}: "
                            + ", ".join(names))


def resolve_binary(directory: Path, name: str) -> Path:
    return resolve_artifact(directory, artifact_names(name), executable=True)


def resolve_archive(build_dir: Path) -> Path:
    return resolve_artifact(build_dir / "src", ("libself_ipcs.a", "libedge_runtime.a"))


def default_build_dir(repo: Path, preset: str) -> Path:
    suffix = {"dev-debug": "debug", "asan-ubsan": "asan-ubsan", "release": "release"}.get(
        preset, preset)
    canonical = repo / "build" / f"selfipcs-{suffix}"
    legacy = repo / "build" / preset
    return canonical if canonical.is_dir() or not legacy.is_dir() else legacy


def build_target(build_dir: Path, name: str) -> str:
    names = artifact_names(name)
    targets_file = build_dir / "CMakeFiles" / "TargetDirectories.txt"
    if targets_file.is_file():
        targets = {Path(line).name for line in targets_file.read_text().splitlines()}
        for candidate in names:
            if f"{candidate}.dir" in targets:
                return candidate
        raise ValueError(f"missing build target in {build_dir}: {', '.join(names)}")
    return names[0]


def parse_cache(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line or line.startswith(("//", "#")):
            continue
        key, value = line.split("=", 1)
        key = key.split(":", 1)[0]
        if key in values and values[key] != value:
            raise ValueError(f"conflicting duplicate cache entry: {key}")
        values[key] = value
    return values


def cache_bool(key: str, value: str) -> bool:
    normalized = value.upper()
    if normalized in ("ON", "YES", "TRUE", "Y", "1"):
        return True
    if (normalized in ("OFF", "NO", "FALSE", "N", "0", "", "IGNORE", "NOTFOUND")
            or normalized.endswith("-NOTFOUND")):
        return False
    raise ValueError(f"invalid boolean cache entry: {key}={value!r}")


def validate_release_cache(path: Path, *, require_testing_off: bool = False,
                           require_optimization: bool = True) -> dict[str, str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    settings = ["CMAKE_BUILD_TYPE:STRING=Release"]
    if require_optimization:
        settings.append("CMAKE_CXX_FLAGS_RELEASE:STRING=-O3 -DNDEBUG")
    for setting in settings:
        if setting not in lines:
            raise ValueError(f"Release configuration required: {setting}")
    values = parse_cache(path)
    if values.get("CMAKE_BUILD_TYPE") != "Release":
        raise ValueError("unexpected Release build configuration")
    if require_testing_off and values.get("BUILD_TESTING") != "OFF":
        raise ValueError("Release configuration required: BUILD_TESTING=OFF")
    for suffix in ("WARNINGS_AS_ERRORS", "ENABLE_SANITIZERS", "ENABLE_FAILPOINTS"):
        keys = [f"{prefix}_{suffix}" for prefix in ("SELFIPCS", "EDGERUNTIME")]
        flags = {key: cache_bool(key, values[key]) for key in keys if key in values}
        if len(set(flags.values())) > 1:
            raise ValueError(f"conflicting cache flags: {keys[0]} and {keys[1]}")
        if suffix != "WARNINGS_AS_ERRORS" and (not flags or any(flags.values())):
            raise ValueError(f"Release configuration required: {keys[0]}=OFF (or {keys[1]}=OFF)")
    return values


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("build-dir", "target", "binary"):
        subparser = commands.add_parser(command)
        subparser.add_argument("directory", type=Path)
        subparser.add_argument("name")
    args = parser.parse_args()
    try:
        if args.command == "build-dir":
            result = default_build_dir(args.directory, args.name)
        elif args.command == "target":
            result = build_target(args.directory, args.name)
        else:
            result = resolve_binary(args.directory, args.name)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
