from __future__ import annotations

import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_requirements_txt_matches_runtime_pyproject_dependencies() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    runtime_dependencies = pyproject["project"]["dependencies"]
    dev_dependencies = pyproject["project"]["optional-dependencies"]["dev"]
    requirements = _requirement_lines(PROJECT_ROOT / "requirements.txt")

    assert {_normalize_requirement(item) for item in requirements} == {
        _normalize_requirement(item) for item in runtime_dependencies
    }
    assert _package_names(requirements).isdisjoint(_package_names(dev_dependencies))


def _requirement_lines(path: Path) -> list[str]:
    lines = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#"):
            lines.append(line)
    return lines


def _normalize_requirement(requirement: str) -> str:
    return "".join(requirement.split()).lower()


def _package_names(requirements: list[str]) -> set[str]:
    return {_package_name(requirement) for requirement in requirements}


def _package_name(requirement: str) -> str:
    name_part = requirement.split(";", 1)[0].strip()
    for separator in ("<", ">", "=", "!", "~", " "):
        name_part = name_part.split(separator, 1)[0]
    return name_part.split("[", 1)[0].replace("_", "-").lower()

