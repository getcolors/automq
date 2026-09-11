"""Deployment-owned application buckets and scoped credentials."""
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
    if opts.get("provider-backend") == "oci":
        mapping = {"oci-access-key-id": "AWS_ACCESS_KEY_ID", "oci-secret-access-key": "AWS_SECRET_ACCESS_KEY"}
    return {variable: str(opts[key]) for key, variable in mapping.items() if opts.get(key)}


def specs(opts):
    result = [{"template": {"name": "tools/storage/main.tf", "content": (Path(__file__).parent / "resources/tools/storage/main.tf").read_text()}, "target": directory(opts) + "/main.tf", "data": {**opts, "automq-storage-gcs": opts.get("automq-storage-provider") == "gcs", "automq-storage-oci": opts.get("automq-storage-provider") == "oci", "oci-auth": opts.get("oci-auth", "APIKey"), "oci-home-region": opts.get("oci-home-region", opts.get("automq-r2-region"))}, "opts": PRESERVE_JINJA_DELIMITERS}]
    if opts.get("automq-storage-provider") == "oci":
        result.append({"template": {"name": "tools/storage/oci-storage.py", "content": (Path(__file__).parent / "resources/tools/storage/oci-storage.py").read_text()}, "target": directory(opts) + "/oci-storage.py", "data": opts, "opts": PRESERVE_JINJA_DELIMITERS})
    return result


async def oci_operation(opts, action):
    values = {key: opts.get(key) for key in ["profile", "oci-auth", "oci-config-file-profile", "oci-namespace", "oci-compartment-id", "automq-r2-region", "automq-data-r2-bucket", "automq-ops-r2-bucket", "compute-prevent-destroy"]}
    result = await runtime.exec(["python3", "oci-storage.py", action, json.dumps(values)], cwd=directory(opts), env=aws_env(opts))
    if result.exit:
        raise RuntimeError("OCI storage operation failed")


async def ownership_preflight(opts):
    if opts.get("automq-storage-provider") == "oci":
        return await oci_operation(opts, "preflight")
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
        if any(resource.get("address") == f'{"google_storage_bucket" if opts.get("automq-storage-provider") == "gcs" else "aws_s3_bucket"}.application["{role}"]' and (resource.get("values", {}).get("bucket") or resource.get("values", {}).get("name")) == opts[key] for resource in resources):
            continue
        probe = await runtime.exec((["gcloud", "storage", "buckets", "describe", "gs://" + opts[key], "--project", opts["google-project"], "--format=json"] if opts.get("automq-storage-provider") == "gcs" else ["aws", "s3api", "head-bucket", "--bucket", opts[key], "--region", opts["automq-r2-region"]]), **config)
        if not (probe.exit > 0 and re.search(r"\(404\)|Not Found|NoSuchBucket|HTTPError 404|not found: 404", probe.err or "")):
            raise RuntimeError("managed storage refuses to adopt an existing or inaccessible bucket")


async def storage_step(opts):
    if not managed(opts):
        return {**opts, "blue/exit": 0}
    try:
        documents = specs(opts)
        if opts.get("blue/event") == "delete" and opts.get("automq-storage-provider") == "oci":
            scaffold({**opts, "blue/event": "create"}, documents)
            await oci_operation(opts, "cleanup")
        if opts.get("blue/event") == "create":
            scaffold(opts, documents)
            await ownership_preflight(opts)
        return await tofu.tofu_with_spec(opts, documents, dir=directory(opts), env=aws_env(opts), output_key="automq/storage-credentials")
    except Exception:
        return {**opts, "blue/exit": 1, "blue/err": "managed storage failed; inspect bucket ownership, state access, and provider permissions"}


def credential_env(opts):
    credentials = opts.get("automq/storage-credentials", {})
    access = credentials.get("access_key_id")
    secret = credentials.get("secret_access_key")
    if not access or not secret or not access.strip() or not secret.strip() or (opts.get("automq-storage-provider") == "oci" and (not credentials.get("oci_signing_key_b64") or not credentials.get("oci_signing_key_id"))):
        raise RuntimeError("managed storage credentials unavailable")
    return {"COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID": access, "COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY": secret, "COLORS_PAR_AUTOMQ_OCI_SIGNING_KEY_B64": credentials.get("oci_signing_key_b64", ""), "COLORS_PAR_AUTOMQ_OCI_SIGNING_KEY_ID": credentials.get("oci_signing_key_id", ""), "ANSIBLE_HOST_KEY_CHECKING": "False"}
