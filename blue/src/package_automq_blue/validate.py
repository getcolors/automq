"""Desired-state, credential, tool, and Vultr validation.

Green renders its keys as Clojure keywords, so every message here carries the
same leading colon — the three colours must report identical errors for one
colors.yml.
"""

from __future__ import annotations

import re

from blue.cli import par_name
from blue.runtime import runtime
from package_once_blue import compute as once_compute
from package_once_blue import compute_cluster as once_cluster
from package_once_blue import ssh as once_ssh
from package_once_blue.validate import providers as once_providers

from . import cluster

profile_par = par_name("profile")

# The registry and the spec live in `cluster`, which every node derivation
# needs and which this module already depends on for the principals; they are
# named here too so the lifecycle reads them from the validator, as the other
# delegating packages do.
compute_providers = cluster.compute_providers
default_compute_provider = cluster.default_compute_provider
spec = cluster.spec

# Every key desired state must carry whichever provider is selected. The
# provider-scoped keys come from `compute_providers`.
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
    "r2-bucket", "r2-endpoint",
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
    """Whether this deployment owns its machine keypair. Delegates to ONCE, the
    standard's reference implementation, so one rule decides it everywhere."""
    return once_ssh.keygen(opts)


def env_errors(env: dict) -> list[str]:
    if _s(env.get(profile_par)):
        return [f"{profile_par} is set; profile must come from colors.yml only"]
    return []


def _port(value) -> bool:
    return _int(value) and 1 <= value <= 65535


def state_errors(opts: dict) -> list[str]:
    """Every problem with desired state at once: the missing keys (this
    package's and the selected provider's), the package's own checks, then the
    Compute Cluster Standard's — selection, the source lists, the provider
    rules, the created network's CIDR and the topology — which are ONCE's over
    `spec`."""
    errors: list[str] = []
    errors += [f":{k} is required"
               for k in [*required, *once_compute.required_keys(spec, opts)]
               if missing(opts.get(k))]
    if opts.get("provider-dns") != "cloudflare":
        errors.append(":provider-dns must be cloudflare")
    if opts.get("provider-backend") not in ("local", "s3", "r2"):
        errors.append(":provider-backend must be local, s3, or r2")
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
               if not missing(opts.get(k)) and _s(opts.get(k)) == _s(opts.get("r2-bucket"))]
    if not (missing(opts.get("automq-r2-endpoint"))
            or endpoint_re.fullmatch(_s(opts.get("automq-r2-endpoint")))):
        errors.append(":automq-r2-endpoint must be an https endpoint URL")
    interval = opts.get("automq-wal-batch-interval-ms")
    if not (missing(interval) or (_int(interval) and 1 <= interval <= 60000)):
        errors.append(":automq-wal-batch-interval-ms must be an integer from 1 to 60000")
    batch = opts.get("automq-wal-max-bytes-in-batch")
    if not (missing(batch) or (_int(batch) and batch > 0)):
        errors.append(":automq-wal-max-bytes-in-batch must be a positive integer")

    # --- compute: the Compute Cluster Standard's checks are ONCE's over the
    # spec — selection, the source lists, the Vultr os id and name rules, the
    # canonical VPC CIDR, and the node count as a positive integer.
    errors += once_cluster.state_errors(spec, opts)
    return errors


def backend_secrets(opts: dict) -> list[str]:
    entry = once_providers["provider-backend"].get(str(opts.get("provider-backend")), {})
    return entry.get("secrets", [])


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
    keys = [*once_compute.secrets(spec, opts),
            *dns_secrets,
            *(application_secrets if event == "create" else []),
            *backend_secrets(opts)]
    return [f"required credential is not set: {par_name(k)}"
            for k in dict.fromkeys(keys) if missing(opts.get(k))]


def tofu_env(opts: dict, slot: str) -> dict[str, str]:
    if slot == "provider-compute":
        return once_compute.tofu_env(spec, opts)
    if slot == "provider-dns":
        return {"cloudflare-api-token": "CLOUDFLARE_API_TOKEN"}
    if slot == "provider-backend":
        entry = once_providers["provider-backend"].get(str(opts.get("provider-backend")), {})
        return entry.get("tofu-env", {})
    return {}


# ------------------------------------------------------------ runtime checks

required_tools = ["tofu", "ansible-playbook", "ssh", "curl", "openssl"]

account_url = "https://api.vultr.com/v2/account"


async def _command_present(runner, command: str) -> bool:
    result = await runner(["sh", "-c", 'command -v "$1" >/dev/null 2>&1', "sh", command])
    return result.exit == 0


def api_error(result) -> str | None:
    """Turn one probe of the Vultr account endpoint into an error, or None.

    The distinction is the point. A single message covering every non-2xx status
    reports a Vultr outage as a bad credential and sends the operator off to
    rotate a key that was never the problem. Only 401 and 403 say anything about
    the key. A request that never reached the API at all shows up as curl's
    literal `000`, which is not an HTTP status: that is the operator's network,
    and naming it saves the same wasted rotation."""
    exit_code = result.exit if hasattr(result, "exit") else result["exit"]
    out = result.out if hasattr(result, "out") else result.get("out")
    match = re.search(r"\d{3}$", _s(out).strip())
    status = int(match.group(0)) if match else None
    if status is None or status == 0:
        return (f"could not reach the Vultr API at {account_url} "
                f"(curl exit {exit_code}): this is a local network, DNS, or TLS "
                "failure, not a credential problem. Check connectivity and retry.")
    if 200 <= status <= 299:
        return None
    if status in (401, 403):
        return (f"Vultr rejected COLORS_PAR_VULTR_API_KEY (HTTP {status}): the key "
                "is missing, revoked, or its allowed-subnet list does not include "
                "this machine. Check the key in the Vultr console and update "
                ".envrc.private.")
    if status == 429:
        return ("Vultr rate-limited the credential check (HTTP 429). The key is "
                "valid; wait for the limit to reset and retry.")
    if 500 <= status <= 599:
        return (f"the Vultr API returned HTTP {status} for {account_url}. That is a "
                "failure on Vultr's side, not your credential — do not rotate "
                "COLORS_PAR_VULTR_API_KEY. Check https://status.vultr.com and retry.")
    return f"unexpected HTTP {status} from {account_url} during the credential check."


async def runtime_errors(opts: dict, runner=None) -> list[str]:
    """Check local tools and authenticate the configured Vultr key. The runner
    argument keeps command decisions testable without network access."""
    runner = runner or runtime.exec
    present = {tool: await _command_present(runner, tool) for tool in required_tools}
    errors = [f"required tool is not on PATH: {tool}"
              for tool in required_tools if not present[tool]]
    key = opts.get("vultr-api-key")
    # No `-f`: the status code is the diagnosis, so it has to survive into
    # stdout instead of collapsing into curl's exit code.
    result = None
    if not missing(key) and present["curl"]:
        result = await runner(["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
                               "--connect-timeout", "10", "--max-time", "20",
                               "-H", f"Authorization: Bearer {key}", account_url])
    probe = api_error(result) if result is not None else None
    return [*errors, probe] if probe else errors
