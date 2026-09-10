"""Deployment-owned S3 application buckets and scoped credentials."""
import re
import json
from pathlib import Path
from blue import tofu
from blue.cli import stage_dir
from blue.runtime import runtime
from blue.scaffold import scaffold, PRESERVE_JINJA_DELIMITERS

tool = "automq-storage"


def managed(opts):
    return opts.get("automq-storage-managed") is True


def directory(opts):
    return stage_dir(opts, tool, default_profile="automq")


def aws_env(opts):
    mapping = {"aws-access-key-id": "AWS_ACCESS_KEY_ID", "aws-secret-access-key": "AWS_SECRET_ACCESS_KEY", "aws-session-token": "AWS_SESSION_TOKEN"}
    return {variable: str(opts[key]) for key, variable in mapping.items() if opts.get(key)}


def specs(opts):
    return [{"template": {"name": "tools/storage/main.tf", "content": (Path(__file__).parent / "resources/tools/storage/main.tf").read_text()}, "target": directory(opts) + "/main.tf", "data": opts, "opts": PRESERVE_JINJA_DELIMITERS}]


async def ownership_preflight(opts):
    config = {"cwd": directory(opts), "env": aws_env(opts)}
    result = await runtime.exec(["tofu", "init", "-input=false", "-no-color"], **config)
    if result.exit:
        raise RuntimeError("managed storage state operation failed")
    result = await runtime.exec(["tofu", "state", "list"], **config)
    if result.exit and "No state file was found!" not in (result.err or ""):
        raise RuntimeError("managed storage state operation failed")
    resources = []
    if not result.exit and result.out.strip():
        shown = await runtime.exec(["tofu", "show", "-json"], **config)
        if shown.exit:
            raise RuntimeError("managed storage state operation failed")
        resources = json.loads(shown.out).get("values", {}).get("root_module", {}).get("resources", [])
    for role, key in [("data", "automq-data-r2-bucket"), ("ops", "automq-ops-r2-bucket")]:
        if any(resource.get("address") == f'aws_s3_bucket.application["{role}"]' and resource.get("values", {}).get("bucket") == opts[key] for resource in resources):
            continue
        probe = await runtime.exec(["aws", "s3api", "head-bucket", "--bucket", opts[key], "--region", opts["automq-r2-region"]], **config)
        if not (probe.exit > 0 and re.search(r"\(404\)|Not Found|NoSuchBucket", probe.err or "")):
            raise RuntimeError("managed storage refuses to adopt an existing or inaccessible bucket")


async def storage_step(opts):
    if not managed(opts):
        return {**opts, "blue/exit": 0}
    try:
        documents = specs(opts)
        if opts.get("blue/event") == "create":
            scaffold(opts, documents)
            await ownership_preflight(opts)
        return await tofu.tofu_with_spec(opts, documents, dir=directory(opts), env=aws_env(opts), output_key="automq/storage-credentials")
    except Exception:
        return {**opts, "blue/exit": 1, "blue/err": "managed S3 storage failed; inspect bucket ownership, state access, and AWS permissions"}


def credential_env(opts):
    credentials = opts.get("automq/storage-credentials", {})
    access = credentials.get("access_key_id")
    secret = credentials.get("secret_access_key")
    if not access or not secret or not access.strip() or not secret.strip():
        raise RuntimeError("managed storage credentials unavailable")
    return {"COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID": access, "COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY": secret, "ANSIBLE_HOST_KEY_CHECKING": "False"}
