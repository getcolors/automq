from pathlib import Path

from blue.cli import load_yaml

ROOT = Path(__file__).resolve().parents[2]

# The compute stage's recorded `params`, as ONCE reads it: snake_case node
# keys, every field present.
PARAMS = {
    "provider": "vultr",
    "ssh_key_id": "7692e92a",
    "nodes": [
        {"role": None, "index": 0, "ip": "203.0.113.10", "vpc_ip": "10.40.0.3",
         "user": "root", "sudoer": "root", "name": "automq-vultr-0"},
        {"role": None, "index": 1, "ip": "203.0.113.11", "vpc_ip": "10.40.0.4",
         "user": "root", "sudoer": "root", "name": "automq-vultr-1"},
        {"role": None, "index": 2, "ip": "203.0.113.12", "vpc_ip": "10.40.0.5",
         "user": "root", "sudoer": "root", "name": "automq-vultr-2"},
    ],
}


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
                    "once/cluster": PARAMS,
                    **(overrides or {})})
