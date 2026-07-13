"""Deterministic, JUnit-backed pytest sharding for the PR 11 sandbox validation."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import xml.etree.ElementTree as element_tree
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "docs" / "validation" / "pr11"
MANIFEST_PATH = OUTPUT_DIR / "collected-nodeids.json"
PLAN_PATH = OUTPUT_DIR / "shard-plan.json"
REPORT_PATH = OUTPUT_DIR / "shard-report.json"
SLOW_FILES = {
    "tests/test_kalshi_ws_collector.py",
    "tests/test_strategy_observer.py",
}
SLOW_SHARD_SIZE = 8


class _NodeCollector:
    def __init__(self) -> None:
        self.nodeids: list[str] = []

    @pytest.hookimpl
    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        self.nodeids = sorted(item.nodeid for item in items)


def collect_nodeids() -> list[str]:
    root = str(ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
    collector = _NodeCollector()
    result = pytest.main(["--collect-only", "-q"], plugins=[collector])
    if result != pytest.ExitCode.OK:
        raise RuntimeError(f"pytest collection failed with exit code {result}.")
    if not collector.nodeids:
        raise RuntimeError("pytest collection returned no node IDs.")
    return collector.nodeids


def build_plan(nodeids: list[str]) -> list[dict[str, Any]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for nodeid in nodeids:
        grouped[_node_file(nodeid)].append(nodeid)

    shards: list[dict[str, Any]] = []
    for file_name in sorted(grouped):
        nodes = sorted(grouped[file_name])
        chunks = (
            [
                nodes[index : index + SLOW_SHARD_SIZE]
                for index in range(0, len(nodes), SLOW_SHARD_SIZE)
            ]
            if file_name in SLOW_FILES
            else [nodes]
        )
        for index, chunk in enumerate(chunks, start=1):
            shard_id = f"{file_name.removeprefix('tests/').removesuffix('.py')}-{index:02d}"
            shards.append(
                {
                    "id": shard_id,
                    "file": file_name,
                    "nodeids": chunk,
                    "command": [
                        sys.executable,
                        "-m",
                        "pytest",
                        "-q",
                        "--junitxml",
                        str(OUTPUT_DIR / "junit" / f"{shard_id}.xml"),
                        *chunk,
                    ],
                }
            )
    return shards


def write_plan() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    nodeids = collect_nodeids()
    shards = build_plan(nodeids)
    _write_json(MANIFEST_PATH, {"nodeids": nodeids, "count": len(nodeids)})
    _write_json(
        PLAN_PATH,
        {
            "slow_files": sorted(SLOW_FILES),
            "slow_shard_size": SLOW_SHARD_SIZE,
            "shards": shards,
            "assigned_node_count": sum(len(shard["nodeids"]) for shard in shards),
        },
    )
    print(f"Collected {len(nodeids)} node IDs into {len(shards)} deterministic shards.")


def run_shard(shard_id: str) -> None:
    plan = _load_json(PLAN_PATH)
    shard = next((item for item in plan["shards"] if item["id"] == shard_id), None)
    if shard is None:
        raise SystemExit(f"Unknown shard: {shard_id}")
    OUTPUT_DIR.joinpath("logs").mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    completed = subprocess.run(
        shard["command"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    duration_seconds = round(time.monotonic() - started, 3)
    log_path = OUTPUT_DIR / "logs" / f"{shard_id}.log"
    log_path.write_text(completed.stdout + completed.stderr, encoding="utf-8")
    junit_path = Path(shard["command"][5])
    junit = _junit_counts(junit_path)
    result = {
        "id": shard_id,
        "command": shard["command"],
        "nodeids": shard["nodeids"],
        "assigned_count": len(shard["nodeids"]),
        "exit_code": completed.returncode,
        "duration_seconds": duration_seconds,
        "junit": junit,
        "test_timings": _junit_timings(junit_path, file_name=shard["file"]),
        "log": str(log_path.relative_to(ROOT)),
    }
    _write_json(OUTPUT_DIR / "results" / f"{shard_id}.json", result)
    print(
        json.dumps(
            {
                "id": shard_id,
                "assigned_count": result["assigned_count"],
                "exit_code": result["exit_code"],
                "duration_seconds": result["duration_seconds"],
                "junit": result["junit"],
            },
            sort_keys=True,
        )
    )
    raise SystemExit(completed.returncode)


def verify() -> None:
    manifest = _load_json(MANIFEST_PATH)
    plan = _load_json(PLAN_PATH)
    expected = list(manifest["nodeids"])
    assigned = [nodeid for shard in plan["shards"] for nodeid in shard["nodeids"]]
    result_paths = sorted((OUTPUT_DIR / "results").glob("*.json"))
    results = [_load_json(path) for path in result_paths]
    for result in results:
        if not result.get("test_timings"):
            result["test_timings"] = _junit_timings(
                Path(result["command"][5]),
                file_name=_node_file(result["nodeids"][0]),
            )
    executed = [
        nodeid
        for result in results
        if result["exit_code"] == 0
        for nodeid in result["nodeids"]
    ]
    junit_total = sum(result["junit"]["tests"] for result in results)
    test_timings = [
        timing for result in results for timing in result.get("test_timings", [])
    ]
    file_durations: dict[str, float] = defaultdict(float)
    file_counts: dict[str, int] = defaultdict(int)
    for timing in test_timings:
        file_durations[timing["file"]] += timing["duration_seconds"]
        file_counts[timing["file"]] += 1
    report = {
        "collected_node_count": len(expected),
        "assigned_node_count": len(assigned),
        "executed_node_count": len(executed),
        "unique_assigned_node_count": len(set(assigned)),
        "unique_executed_node_count": len(set(executed)),
        "omitted_nodes": sorted(set(expected) - set(assigned)),
        "unexecuted_nodes": sorted(set(expected) - set(executed)),
        "duplicate_assigned_nodes": _duplicates(assigned),
        "duplicate_executed_nodes": _duplicates(executed),
        "aggregate": {
            "passed": sum(
                result["junit"]["tests"]
                - result["junit"]["failures"]
                - result["junit"]["errors"]
                - result["junit"]["skipped"]
                for result in results
            ),
            "failed": sum(result["junit"]["failures"] for result in results),
            "errors": sum(result["junit"]["errors"] for result in results),
            "skipped": sum(result["junit"]["skipped"] for result in results),
            "junit_tests": junit_total,
        },
        "shards": results,
        "slowest_shards": sorted(
            results,
            key=lambda item: item["duration_seconds"],
            reverse=True,
        )[:10],
        "slowest_tests": sorted(
            test_timings,
            key=lambda item: item["duration_seconds"],
            reverse=True,
        )[:10],
        "slowest_files": [
            {
                "file": file_name,
                "duration_seconds": round(duration_seconds, 6),
                "test_count": file_counts[file_name],
            }
            for file_name, duration_seconds in sorted(
                file_durations.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:10]
        ],
    }
    _write_json(REPORT_PATH, report)
    is_complete = (
        set(expected) == set(assigned) == set(executed)
        and len(expected) == len(assigned) == len(executed) == junit_total
        and not report["duplicate_assigned_nodes"]
        and not report["duplicate_executed_nodes"]
        and report["aggregate"]["failed"] == 0
        and report["aggregate"]["errors"] == 0
    )
    print(
        json.dumps(
            {
                "report": str(REPORT_PATH.relative_to(ROOT)),
                "collected": report["collected_node_count"],
                "assigned": report["assigned_node_count"],
                "executed": report["executed_node_count"],
                "passed": report["aggregate"]["passed"],
                "failed": report["aggregate"]["failed"],
                "errors": report["aggregate"]["errors"],
                "skipped": report["aggregate"]["skipped"],
                "omitted": len(report["omitted_nodes"]),
                "unexecuted": len(report["unexecuted_nodes"]),
                "duplicate_assigned": len(report["duplicate_assigned_nodes"]),
                "duplicate_executed": len(report["duplicate_executed_nodes"]),
            },
            sort_keys=True,
        )
    )
    raise SystemExit(0 if is_complete else 1)


def _junit_counts(path: Path) -> dict[str, int]:
    root = element_tree.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    return {
        key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }


def _junit_timings(path: Path, *, file_name: str) -> list[dict[str, Any]]:
    root = element_tree.parse(path).getroot()
    timings = []
    for case in root.iter("testcase"):
        timings.append(
            {
                "file": file_name,
                "node": f"{case.attrib.get('classname', '')}::{case.attrib.get('name', '')}",
                "duration_seconds": float(case.attrib.get("time", "0")),
            }
        )
    return timings


def _node_file(nodeid: str) -> str:
    return nodeid.split("::", maxsplit=1)[0]


def _duplicates(values: list[str]) -> list[str]:
    return sorted({value for value in values if values.count(value) > 1})


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("plan")
    run_parser = subparsers.add_parser("run-shard")
    run_parser.add_argument("shard_id")
    subparsers.add_parser("verify")
    args = parser.parse_args()
    if args.command == "plan":
        write_plan()
    elif args.command == "run-shard":
        run_shard(args.shard_id)
    else:
        verify()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
