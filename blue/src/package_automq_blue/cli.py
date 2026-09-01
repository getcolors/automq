"""CLI entry: the same verbs as the green launcher, with the logic kept here
where the test suite reaches it — the copied payload holds none of its own."""

from __future__ import annotations

import asyncio
import sys

from blue.cli import find_up, run_cli

from .workflow import automq_workflow

USAGE = ("Usage: blue <build|create|delete|validate> "
         "[-f|--file colors.yml] [--dry-run]\n"
         "\n"
         "  build     render the work directory only — contact nothing\n"
         "  create    provision the cluster, converge it, and prove it works\n"
         "  delete    stop the cluster and destroy DNS and infrastructure\n"
         "  validate  check desired state, tools, and Vultr access\n"
         "\n"
         "Object storage is never destroyed by `delete`: the buckets hold the\n"
         "cluster's data, and emptying them is a separate, explicit action.")

LIFECYCLE = ("build", "create", "delete", "validate")


def _find() -> str:
    return find_up("colors.yml") or "colors.yml"


def default_args(args: list[str]) -> list[str]:
    if any(a in ("-f", "--file") or str(a).startswith("--file=") for a in args):
        return args
    return [*args, "-f", _find()]


async def run(*args):
    """REPL-friendly entry point that returns the final outcome map."""
    args = default_args(list(args))
    command = args[0] if args else None
    if command in ("help", "--help", "-h"):
        return {"blue/exit": 0, "blue/err": USAGE}
    if command in LIFECYCLE:
        return await run_cli(automq_workflow, args)
    return {"blue/exit": 2, "blue/err": USAGE}


def exec(args: list[str] | None = None) -> None:
    result = asyncio.run(run(*(sys.argv[1:] if args is None else args)))
    if result.get("blue/err"):
        stream = sys.stdout if (result.get("blue/exit") or 0) == 0 else sys.stderr
        print(result["blue/err"], file=stream)
        if result.get("blue/trace"):
            print(result["blue/trace"], file=stream)
    raise SystemExit(result.get("blue/exit") or 0)
