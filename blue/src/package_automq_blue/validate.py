"""Desired-state, credential, tool, and Vultr validation.

Green renders its keys as Clojure keywords, so every message here carries the
same leading colon — the three colours must report identical errors for one
colors.yml.
"""

from __future__ import annotations

import re

from blue.cli import par_name
from blue.runtime import runtime
from package_once_blue.validate import providers as once_providers
from colors_compute import validate as compute_validate, credential_requirements
from colors_compute.contract import registry
from colors_compute.planning import plan_deployment
from colors_compute.ssh import _mode

from . import cluster

profile_par = par_name("profile")

# The registry and the spec live in `cluster`, which every node derivation
# needs and which this module already depends on for the principals; they are
# named here too so the lifecycle reads them from the validator, as the other
# delegating packages do.
compute_providers = registry()["compute"]
default_compute_provider = cluster.default_compute_provider


# Every key desired state must carry whichever provider is selected. The
#
# `vultr-ssh-keys` is deliberately absent: per the SSH Keypair Standard its
# *absence* selects keygen mode, and requiring it would make a conforming
# deployment invalid. `vultr-name` is absent for the same shape of reason — the
# Compute Name Standard makes the profile the default and the key only an
# override (§2, §5).
required = [
    "profile", "workdir", "provider-compute", "provider-dns", "provider-backend",
    "compute-prevent-destroy",
    "automq-image", "automq-node-count", "automq-cluster-id",
    "automq-host", "automq-broker-name-prefix",
    "automq-letsencrypt-email", "automq-lego-version",
    "automq-kafka-port", "automq-internal-port", "automq-controller-port",
    "automq-sasl-user", "automq-sasl-mechanism", "automq-heap-opts",
    "automq-data-r2-bucket", "automq-ops-r2-bucket",
    "automq-r2-endpoint", "automq-r2-region",
    "automq-wal-batch-interval-ms", "automq-wal-max-bytes-in-batch",

]

host_re = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+")
email_re = re.compile(r"[^@\s]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+")
image_re = re.compile(r"[^\s:@]+(?:/[^\s:@]+)*(?::[^\s:@]+)?(?:@sha256:[0-9a-f]{64})?")
digest_re = re.compile(r"@sha256:[0-9a-f]{64}$")
bucket_re = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
endpoint_re = re.compile(r"https://[a-z0-9.-]+(?::\d+)?/?")
prefix_re = re.compile(r"[a-z][a-z0-9-]{0,15}")
# kafka-storage.sh random-uuid: a UUID in unpadded URL-safe base64.
cluster_id_re = re.compile(r"[A-Za-z0-9_-]{22}")
principal_re = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")


def _s(value) -> str:
    """Clojure's `str`: nil renders empty, booleans lowercase."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def missing(value) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def keygen(opts: dict) -> bool:
    """Use normalized library results for application rendering."""
    return _mode(opts)["mode"] == "managed"


def env_errors(env: dict) -> list[str]:
    if _s(env.get(profile_par)):
        return [f"{profile_par} is set; profile must come from colors.yml only"]
    return []


def _port(value) -> bool:
    return _int(value) and 1 <= value <= 65535


def state_errors(opts: dict) -> list[str]:
    """Use normalized library results for application rendering."""
    errors: list[str] = []
    errors += [f":{k} is required"
               for k in [*required, *({"r2": ["r2-bucket", "r2-endpoint"], "s3": ["s3-bucket", "s3-region"], "gcs": ["gcs-bucket", "gcs-region"]}.get(opts.get("provider-backend"), []))]
               if missing(opts.get(k))]
    if opts.get("provider-dns") not in ("cloudflare", "none"):
        errors.append(":provider-dns must be cloudflare or none")
    if opts.get("automq-tls-mode", "acme") not in ("acme", "private-ca"):
        errors.append(":automq-tls-mode must be acme or private-ca")
    if opts.get("provider-dns") == "none" and opts.get("automq-tls-mode") != "private-ca":
        errors.append(":provider-dns none requires :automq-tls-mode private-ca")
    if opts.get("automq-tls-mode") == "private-ca" and opts.get("provider-dns") != "none":
        errors.append(":automq-tls-mode private-ca requires :provider-dns none")
    if "automq-apt-security-mirror" in opts and not re.fullmatch(r"https?://[a-z0-9.-]+(?::[0-9]+)?/[A-Za-z0-9._~/-]+", str(opts["automq-apt-security-mirror"])):
        errors.append(":automq-apt-security-mirror must be an HTTP or HTTPS repository URL")
    if opts.get("provider-backend") not in ("s3", "r2", "gcs"):
        errors.append(":provider-backend must be s3, r2 or gcs")
    # A boolean, not `True`. The guard is lifted for exactly one run by
    # COLORS_PAR_COMPUTE_PREVENT_DESTROY=false, which arrives through the same
    # overlay as every other parameter — so demanding `true` here would reject
    # the override before the delete-time guard could honour it, and the
    # documented way to destroy this deployment would not work at all. What must
    # stay true is the value COMMITTED to colors.yml, and that is a review rule
    # rather than something validation can see.
    if not isinstance(opts.get("compute-prevent-destroy"), bool):
        errors.append(":compute-prevent-destroy must be true or false")

    # --- cluster shape
    # An even count is not merely unusual, it is worse than the odd count below
    # it: four voters tolerate one failure, exactly as three do, while adding a
    # node that can fail. One node is allowed because it is a legitimate
    # development shape, but it is not a quorum.
    count = opts.get("automq-node-count")
    if not missing(count):
        if not _int(count):
            errors.append(":automq-node-count must be an integer")
        elif not 1 <= count <= 9:
            errors.append(":automq-node-count must be from 1 to 9")
        elif count % 2 == 0 and count > 1:
            errors.append(":automq-node-count must be odd: an even quorum "
                          "tolerates no more failures than the odd size below it")
    if not (missing(opts.get("automq-cluster-id"))
            or cluster_id_re.fullmatch(_s(opts.get("automq-cluster-id")))):
        errors.append(":automq-cluster-id must be a 22-character base64 UUID as "
                      "produced by `kafka-storage.sh random-uuid`")
    if not (missing(opts.get("automq-host"))
            or host_re.fullmatch(_s(opts.get("automq-host")))):
        errors.append(":automq-host must be a fully qualified hostname")
    if not (missing(opts.get("automq-broker-name-prefix"))
            or prefix_re.fullmatch(_s(opts.get("automq-broker-name-prefix")))):
        errors.append(":automq-broker-name-prefix must be a short lowercase label")
    if not (missing(opts.get("automq-letsencrypt-email"))
            or email_re.fullmatch(_s(opts.get("automq-letsencrypt-email")))):
        errors.append(":automq-letsencrypt-email must be an email address")

    # --- image
    if not (missing(opts.get("automq-image"))
            or image_re.fullmatch(_s(opts.get("automq-image")))):
        errors.append(":automq-image must be a container image reference")
    # This package owns its unit and configuration templates rather than running
    # an upstream installer, so nothing tells it when a floating tag moves
    # underneath it. A digest is what turns a silent retag into a failure at
    # pull time instead of a behaviour change at run time.
    if not (missing(opts.get("automq-image"))
            or digest_re.search(_s(opts.get("automq-image")))):
        errors.append(":automq-image must be pinned by digest (…@sha256:…)")

    # --- listeners
    port_keys = ["automq-kafka-port", "automq-internal-port", "automq-controller-port"]
    errors += [f":{k} must be an integer from 1 to 65535"
               for k in port_keys
               if not missing(opts.get(k)) and not _port(opts.get(k))]
    ports = [opts[k] for k in port_keys if opts.get(k) is not None]
    if len(ports) == 3 and len(set(ports)) != 3:
        errors.append(":automq-kafka-port, :automq-internal-port and "
                      ":automq-controller-port must differ")
    if not (missing(opts.get("automq-sasl-mechanism"))
            or opts.get("automq-sasl-mechanism") == "SCRAM-SHA-512"):
        errors.append(":automq-sasl-mechanism must be SCRAM-SHA-512")
    # Four principals share one namespace in the metadata log, and two that
    # collide would silently merge authorities — the client principal is ACL
    # scoped and the others are superusers, so a collision is a privilege
    # escalation rather than a naming annoyance.
    principals = [("automq-sasl-user", cluster.client_user(opts)),
                  ("automq-admin-user", cluster.admin_user(opts)),
                  ("automq-broker-user", cluster.broker_user(opts)),
                  ("automq-controller-user", cluster.controller_user(opts))]
    errors += [f":{k} must be a safe 1-64 character principal name"
               for k, v in principals if not principal_re.fullmatch(v)]
    users = [v for _, v in principals]
    if len(set(users)) != len(users):
        errors.append("the client, admin, broker and controller principals "
                      "must all differ")

    # --- object storage
    if "automq-storage-managed" in opts and not isinstance(opts["automq-storage-managed"], bool):
        errors.append(":automq-storage-managed must be true or false")
    if opts.get("automq-storage-managed") and opts.get("automq-storage-provider") not in ("s3", "gcs"):
        errors.append("managed storage requires :automq-storage-provider s3 or gcs")
    if opts.get("automq-storage-managed") and opts.get("automq-storage-provider") == "gcs" and (missing(opts.get("google-project")) or opts.get("automq-r2-endpoint") != "https://storage.googleapis.com"):
        errors.append("managed GCS storage requires :google-project and :automq-r2-endpoint https://storage.googleapis.com")
    if opts.get("automq-storage-managed") and opts.get("automq-storage-provider") == "s3" and opts.get("automq-r2-region") == "auto":
        errors.append("managed S3 storage requires an AWS region in :automq-r2-region")
    bucket_keys = ["automq-data-r2-bucket", "automq-ops-r2-bucket"]
    errors += [f":{k} must be a valid bucket name" for k in bucket_keys
               if not missing(opts.get(k)) and not bucket_re.fullmatch(_s(opts.get(k)))]
    # AutoMQ addresses the two roles by distinct bucket ids and writes different
    # key layouts under each; it also supports no path prefix at all, so one
    # bucket cannot host both roles side by side.
    if (not missing(opts.get("automq-data-r2-bucket"))
            and opts.get("automq-data-r2-bucket") == opts.get("automq-ops-r2-bucket")):
        errors.append(":automq-data-r2-bucket and :automq-ops-r2-bucket must be "
                      "different buckets")
    # The state bucket is the operator's, holds every deployment's tfstate, and
    # AutoMQ writes hash-prefixed keys at the bucket root. Sharing them is not a
    # style question.
    errors += [f":{k} must not be the OpenTofu state bucket: AutoMQ writes keys "
               "at the bucket root" for k in bucket_keys
               if not missing(opts.get(k)) and _s(opts.get(k)) == _s(opts.get(str(opts.get("provider-backend")) + "-bucket"))]
    if not (missing(opts.get("automq-r2-endpoint"))
            or endpoint_re.fullmatch(_s(opts.get("automq-r2-endpoint")))):
        errors.append(":automq-r2-endpoint must be an https endpoint URL")
    interval = opts.get("automq-wal-batch-interval-ms")
    if not (missing(interval) or (_int(interval) and 1 <= interval <= 60000)):
        errors.append(":automq-wal-batch-interval-ms must be an integer from 1 to 60000")
    batch = opts.get("automq-wal-max-bytes-in-batch")
    if not (missing(batch) or (_int(batch) and batch > 0)):
        errors.append(":automq-wal-max-bytes-in-batch must be a positive integer")

    # canonical VPC CIDR, and the node count as a positive integer.
    errors += compute_validate(opts)
    if not errors:
        try:
            plan_deployment(opts, cluster.topology(opts), cluster.requirements(opts))
        except ValueError as error:
            errors.append(str(error))
    return errors


def backend_secrets(opts):
    return registry()['backend'].get(opts.get('provider-backend'), {}).get('secrets', [])


# What talking to Cloudflare needs, on any real event. The compute provider's
# credential comes from the registry.
dns_secrets = ["cloudflare-api-token"]

# What converging the cluster needs, and therefore only a create. Every SASL
# password, the keystore password, and the SCRAM salts are generated on the
# hosts and are never supplied by the operator.
application_secrets = ["automq-r2-access-key-id", "automq-r2-secret-access-key"]


def secret_errors(opts: dict, event: str) -> list[str]:
    """Credentials a real event needs: the selected compute provider's,
    Cloudflare's, the backend's, and on a create the storage keys. A delete
    tears down infrastructure and never converges anything, so it asks for the
    provider credentials only; demanding the storage keys to destroy machines
    would be a lock on the exit."""
    keys = [*[name.removeprefix("COLORS_PAR_").lower().replace("_", "-") for name in (credential_requirements(opts) if event == "validate" else [])],
            *(dns_secrets if opts.get("provider-dns") != "none" else []),
            *(application_secrets if event == "create" and not opts.get("automq-storage-managed") else []),
            *backend_secrets(opts)]
    return [f"required credential is not set: {par_name(k)}"
            for k in dict.fromkeys(keys) if missing(opts.get(k))]


def tofu_env(opts, slot):
    return {'cloudflare-api-token': 'CLOUDFLARE_API_TOKEN'} if slot == 'provider-dns' else once_providers['provider-backend'].get(opts.get('provider-backend'), {}).get('tofu-env', {}) if slot == 'provider-backend' else {}


# ------------------------------------------------------------ runtime checks

required_tools = ["tofu", "aws", "ansible-playbook", "ssh", "ssh-keygen", "curl", "openssl"]

async def _command_present(runner, command: str) -> bool:
    result = await runner(["sh", "-c", 'command -v "$1" >/dev/null 2>&1', "sh", command])
    return result.exit == 0


async def runtime_errors(opts, runner=None):
    runner = runner or runtime.exec
    tools = [*required_tools, *(["gcloud"] if opts.get("automq-storage-provider") == "gcs" else [])]
    present = {tool: await _command_present(runner, tool) for tool in tools}
    return [f"required tool is not on PATH: {tool}" for tool in tools if not present[tool]]
