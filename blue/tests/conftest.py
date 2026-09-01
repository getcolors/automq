from pathlib import Path

from blue.cli import load_yaml

ROOT = Path(__file__).resolve().parents[2]

# The compute stage's output, as three applied nodes.
PARAMS = [
    {"index": 0, "ip": "203.0.113.10", "vpc-ip": "10.40.0.3", "user": "root"},
    {"index": 1, "ip": "203.0.113.11", "vpc-ip": "10.40.0.4", "user": "root"},
    {"index": 2, "ip": "203.0.113.12", "vpc-ip": "10.40.0.5", "user": "root"},
]


def _load(name: str, overrides: dict | None = None) -> dict:
    text = (ROOT / "test" / "fixtures" / name).read_text().replace("WORKDIR", ".colors")
    return {**load_yaml(text), **(overrides or {})}


def fixture(overrides: dict | None = None) -> dict:
    return _load("colors.yml", overrides)


def optout(overrides: dict | None = None) -> dict:
    return _load("optout.yml", overrides)


def applied(overrides: dict | None = None) -> dict:
    """A fixture carrying the compute stage's applied output."""
    return fixture({"profile": "automq-vultr",
                    "automq/params": {"nodes": PARAMS},
                    **(overrides or {})})
