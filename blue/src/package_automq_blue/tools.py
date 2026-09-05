"""Compute, DNS, local SSH config, cluster convergence, and acceptance stages."""

from __future__ import annotations

import json
from pathlib import Path

from blue import tofu
from blue.ansible import ansible_with_spec
from blue.cli import stage_dir
from blue.runtime import runtime
from blue.scaffold import PRESERVE_JINJA_DELIMITERS, content_spec, scaffold
from package_once_blue import compute as once_compute
from package_once_blue import compute_cluster as once_cluster
from package_once_blue.utils import registrable_domain

from . import cluster, ssh_config, validate

infrastructure_tool = "automq-infrastructure"
dns_tool = "automq-dns"
ansible_tool = "automq-ansible"
ansible_local_tool = "automq-ansible-local"
acceptance_tool = "automq-acceptance"
ROOT = Path(__file__).parent / "resources"
template_opts = PRESERVE_JINJA_DELIMITERS


def tool_dir(opts: dict, tool: str) -> str:
    return stage_dir(opts, tool, default_profile="automq")


def template(path: str, file: str) -> dict:
    name = f"tools/{path}/{file}"
    return {"name": name, "content": (ROOT / name).read_text()}


def spec(source: dict, target: str, data: dict) -> dict:
    return {"template": source, "target": target, "data": data, "opts": template_opts}


def raw_spec(target: str, content: str) -> dict:
    return content_spec(target, content)


# A source list as desired state or an overlay string carries it. ONCE's, so
# the validator and the templates can never disagree about what an entry is.
cidrs = once_compute.cidrs


def credential_env(opts: dict, *slots: str) -> dict[str, str] | None:
    merged: dict[str, str] = {}
    for slot in [*slots, "provider-backend"]:
        merged.update(validate.tofu_env(opts, slot))
    result = {}
    for key, env_var in merged.items():
        value = "" if opts.get(key) is None else str(opts.get(key))
        if value:
            result[env_var] = value
    return result or None


def backend_credential_env(opts: dict) -> dict[str, str] | None:
    return credential_env(opts)


async def state_output(opts: dict):
    """The reader ONCE's `read_state` takes: the recorded `params` map with the
    underscores kept (`ssh_key_id`, `vpc_ip`), or None when the state is
    readable and holds no compute. This package's pre-adoption states already
    recorded `params` in this shape, only without `provider`, which the Compute
    Provider Standard reads as the default provider — so there is no legacy
    translation. An unreadable backend is whatever `blue.tofu` raises — the
    SDK's `StepError` — deliberately uncaught: `read_state` turns it into
    `{"error": message}`, and create and delete treat that differently. Looked
    up on this module at call time, so tests can replace it."""
    outputs = await tofu.outputs(tool_dir(opts, infrastructure_tool),
                                 backend_credential_env(opts))
    return (outputs or {}).get("params")


def nodes(opts: dict) -> list[dict]:
    """The cluster's nodes for every later stage: the recorded cluster under
    `once/cluster` on a real run, ONCE's fallbacks on a build."""
    return cluster.nodes(opts, opts.get("once/cluster"))


# ------------------------------------------------------------------ compute


def infrastructure_data(opts: dict) -> dict:
    return {**opts,
            "ssh-keygen": validate.keygen(opts),
            "node-count": cluster.node_count(opts),
            "compute-name": cluster.compute_name(opts),
            # The firewall rule renders this. A template key that is absent
            # renders as empty rather than failing, so omitting it produced
            # `port = ""` — which survives build, golden, dry-run and validate,
            # and is rejected only by the provider on a real apply.
            "kafka-port": cluster.kafka_port(opts),
            # The quorum and inter-broker ports are opened to the VPC subnet
            # only — see the firewall comment in main.tf for why that rule has
            # to exist at all.
            "controller-port": cluster.controller_port(opts),
            "internal-port": cluster.internal_port(opts),
            "ssh-sources-hcl": tofu.hcl_list(cidrs(opts, "vultr-ssh-sources")),
            "kafka-sources-hcl": tofu.hcl_list(cidrs(opts, "vultr-kafka-sources"))}


def resolved_cluster(opts: dict, result: dict) -> dict:
    """The applied compute stage's `params`, adopted under `once/cluster` for
    the stages that follow — or ONCE's refusal: no `params` output at all, or a
    node set that is partial, undeclared, duplicated or incomplete, exits 1
    rather than rendering a quorum string against the documentation
    addresses."""
    return once_cluster.resolved_cluster(cluster.spec, opts, result, {},
                                         once_cluster.output_params(result))


async def infrastructure_step(opts: dict) -> dict:
    dir = tool_dir(opts, infrastructure_tool)
    specs = [spec(template("infrastructure", "main.tf"), f"{dir}/main.tf",
                  infrastructure_data(opts))]
    result = await tofu.tofu_with_spec(
        opts, specs, dir=dir, env=credential_env(opts, "provider-compute"))
    if (result.get("blue/exit") or 0) > 0:
        return result
    if opts.get("blue/event") in ("build", "delete"):
        return result
    return resolved_cluster(opts, result)


# ---------------------------------------------------------------------- dns


def zone(opts: dict) -> str | None:
    """The Cloudflare zone the cluster's names belong to (their registrable
    domain)."""
    return registrable_domain(opts.get("automq-host"))


def dns_json(opts: dict, nodes_: list[dict]) -> str:
    """Every A record this cluster needs.

    The bootstrap name carries one record per node, so a client that knows only
    that name reaches some broker and is redirected from there. Each broker also
    gets its own name, because that is what it advertises and what its
    certificate must cover.

    `proxied` is false on every record and is not a preference. Cloudflare's
    proxy terminates HTTP; Kafka is a raw TCP protocol on 9092, and a proxied
    record would publish an address that speaks HTTP to a client speaking
    Kafka."""
    return tofu.constructs_json([
        *[tofu.construct("resource", "cloudflare_dns_record", f"bootstrap_{i}",
                         {"zone_id": "${data.cloudflare_zone.zone.id}",
                          "name": opts.get("automq-host"), "content": n["ip"],
                          "type": "A", "proxied": False, "ttl": 60})
          for i, n in enumerate(nodes_)],
        *[tofu.construct("resource", "cloudflare_dns_record", f"broker_{n['index']}",
                         {"zone_id": "${data.cloudflare_zone.zone.id}",
                          "name": n["broker-name"], "content": n["ip"],
                          "type": "A", "proxied": False, "ttl": 60})
          for n in nodes_]])


async def dns_step(opts: dict) -> dict:
    dir = tool_dir(opts, dns_tool)
    nodes_ = nodes(opts)
    data = {**opts, "automq-zone": zone(opts)}
    specs = [spec(template("dns", "main.tf"), f"{dir}/main.tf", data),
             raw_spec(f"{dir}/record.tf.json", dns_json(data, nodes_))]
    return await tofu.tofu_with_spec(
        opts, specs, dir=dir, env=credential_env(opts, "provider-dns"))


# ------------------------------------------------------- ssh config (local)


def ansible_local_data(opts: dict) -> dict:
    """Only what a `build` genuinely knows. Addresses are run-time facts and
    reach the play as extra-vars instead, so the rendered playbook carries no IP
    and is identical on every workstation (SSH Config Standard §6)."""
    return {**opts,
            "ssh-keygen": validate.keygen(opts),
            "ssh-config-identity-file": ssh_config.identity_file(opts),
            "host-alias": ssh_config.host_alias(opts)}


def ansible_local_specs(opts: dict) -> list[dict]:
    dir = tool_dir(opts, ansible_local_tool)
    data = ansible_local_data(opts)
    return [spec(template("ansible-local", name), f"{dir}/{name}", data)
            for name in ["ansible.cfg", "inventory.ini", "main.yml"]]


def ssh_config_hosts(opts: dict, nodes_: list[dict]) -> list[dict]:
    """The `~/.ssh/config` entries, as data the play loops over: the bare
    profile pointing at node 0 (the spec's entry), then one alias per node.
    ONCE's (Compute Cluster Standard §6)."""
    return once_cluster.ssh_config_hosts(cluster.spec, opts, nodes_)


async def ansible_local_step(opts: dict) -> dict:
    """Write or remove the `~/.ssh/config` block. The same playbook serves both
    events; `block_state` is what distinguishes them."""
    dir = tool_dir(opts, ansible_local_tool)
    delete = opts.get("blue/event") == "delete"
    return await ansible_with_spec(
        opts, ansible_local_specs(opts),
        dir=dir, inventory="inventory.ini",
        playbooks={"create": "main.yml", "delete": "main.yml"},
        extra_vars={"host_alias": ssh_config.host_alias(opts),
                    "ssh_hosts": ssh_config_hosts(opts, nodes(opts)),
                    "block_state": "absent" if delete else "present"})


# ------------------------------------------------------------------ ansible


def _pretty(value, indent=0):
    """Cheshire's pretty JSON, byte for byte — Green's artifact contract. Keys
    render in the order they are given, which is why every map below is built
    already sorted."""
    if isinstance(value, list):
        if not value:
            return "[ ]"
        return "[ " + ", ".join(_pretty(item, indent) for item in value) + " ]"
    if isinstance(value, dict):
        if not value:
            return "{ }"
        pad = " " * (indent + 2)
        body = ",\n".join(f"{pad}{json.dumps(str(k))} : {_pretty(v, indent + 2)}"
                          for k, v in value.items())
        return "{\n" + body + "\n" + " " * indent + "}"
    return json.dumps(value)


def inventory(opts: dict, nodes_: list[dict]) -> str:
    """One host per node, each carrying the facts only it has.

    Per-node values live here rather than in the rendered templates because
    there is one template set for the whole cluster: the playbook fills
    `node.id`, the listeners and the advertised names from these variables. The
    cluster-wide values that must be *identical* everywhere — the quorum string
    above all — are rendered once into the play instead, so three nodes cannot
    disagree about them."""
    hosts = {}
    for n in nodes_:
        host = {"ansible_host": n["ip"],
                "ansible_user": n.get("user") or "root",
                "automq_node_id": n["index"],
                "automq_vpc_ip": n["vpc-ip"],
                "automq_broker_name": n["broker-name"],
                "automq_listeners": cluster.listeners(opts, n),
                "automq_advertised_listeners": cluster.advertised_listeners(opts, n),
                # Node 0 is the only ACME client and the only host that receives
                # the zone-editing token.
                "automq_cert_issuer": n["index"] == 0}
        if validate.keygen(opts):
            host["ansible_ssh_private_key_file"] = opts.get("ssh-private-key-path")
        hosts[str(n["name"])] = dict(sorted(host.items()))
    return _pretty({"all": {"children": {"automq": {
        "hosts": dict(sorted(hosts.items()))}}}})


def ansible_data(opts: dict) -> dict:
    """Template values for the convergence stage.

    Deliberately carries no credential. The R2 keys and the Cloudflare token
    reach the hosts as Ansible `lookup('env', ...)` expressions written
    literally into main.yml, where `preserve-jinja-delimiters` passes them
    through untouched — routing them through this map would let the renderer
    HTML-escape the quotes and hand Ansible `&#39;`. The secret therefore exists
    only in the process that needs it: not in `.colors/`, not in a golden, not
    in this map."""
    nodes_ = nodes(opts)
    return {**opts,
            "ssh-keygen": validate.keygen(opts),
            "node-count": cluster.node_count(opts),
            "quorum-voters": cluster.quorum_voters(opts, nodes_),
            "certificate-names": cluster.certificate_names(opts),
            "certificate-names-csv": ",".join(cluster.certificate_names(opts)),
            "bootstrap-internal": ",".join(
                f"{n['vpc-ip']}:{cluster.internal_port(opts)}" for n in nodes_),
            "bootstrap-external": f"{opts.get('automq-host')}:{cluster.kafka_port(opts)}",
            "admin-user": cluster.admin_user(opts),
            "broker-user": cluster.broker_user(opts),
            "controller-user": cluster.controller_user(opts),
            "client-user": cluster.client_user(opts),
            "scram-principals": cluster.scram_principals(opts),
            "super-users": cluster.super_users(opts),
            "client-acls": cluster.client_acls(opts),
            "topic-prefix": cluster.topic_prefix(opts),
            "controller-port": cluster.controller_port(opts),
            "internal-port": cluster.internal_port(opts),
            "kafka-port": cluster.kafka_port(opts)}


ANSIBLE_FILES = [
    "ansible.cfg", "main.yml", "cleanup.yml", "compose.yml", "server.properties",
    "store.py", "secrets.sh", "render-config.sh", "format.sh", "acl.sh", "scram.sh",
    "cert.sh", "cert-deploy.sh", "cert-deploy.service", "cert-deploy.timer",
    "cert-renew.service", "cert-renew.timer",
    "status.sh", "credential.sh", "smoke.sh", "rotate.sh",
]


def ansible_specs(opts: dict) -> list[dict]:
    dir = tool_dir(opts, ansible_tool)
    data = ansible_data(opts)
    return [*[spec(template("ansible", name), f"{dir}/{name}", data)
              for name in ANSIBLE_FILES],
            raw_spec(f"{dir}/inventory.json", inventory(data, nodes(opts)))]


async def ansible_step(opts: dict) -> dict:
    dir = tool_dir(opts, ansible_tool)
    if opts.get("blue/event") == "delete" and opts.get("once/cluster") is None:
        # A readable state without compute: there is nothing to stop, and the
        # cleanup play would only fail against the placeholder addresses. (An
        # unreadable state, or a partial one, never reaches here — the delete
        # failed closed at adoption.)
        return {**opts, "blue/exit": 0}
    return await ansible_with_spec(
        opts, ansible_specs(opts),
        dir=dir, inventory="inventory.json",
        playbooks={"create": "main.yml", "delete": "cleanup.yml"},
        host_key_checking=False)


# --------------------------------------------------------------- acceptance


def acceptance_specs(opts: dict) -> list[dict]:
    dir = tool_dir(opts, acceptance_tool)
    return [spec(template("acceptance", "acceptance.sh"), f"{dir}/acceptance.sh",
                 ansible_data(opts))]


def process_result(opts: dict, label: str, result) -> dict:
    if result.exit == 0:
        return {**opts, "blue/exit": 0}
    return {**opts,
            "blue/exit": max(1, result.exit),
            "blue/err": f"{label} failed: " + (str(result.err or "")
                                               or str(result.out or "")
                                               or "(no output)")}


async def acceptance_step(opts: dict) -> dict:
    """The operator path, proved from the workstation.

    Everything the playbook can prove, the playbook already proved on the hosts
    before the ready marker was written. What is left is what only a client
    outside the deployment can establish: that the public names resolve, that
    the certificate they serve validates, that SASL_SSL admits the client
    principal and refuses a wrong password, that the ACLs deny what they should,
    and that killing a broker which leads a partition does not lose the records
    written to it.

    Forty-five minutes, not twenty. Every wait in that script is bounded, but
    the bounds add up: the partition becoming writable again (300s), the
    survival read retried while the partition is reassigned (120s), the victim
    rejoining with bounded lag (600s), and the controller quorum re-forming
    (600s). Those are worst cases and the usual run is a fraction of them — but
    a ceiling below the sum of the parts turns a slow cluster into a killed
    test, and a killed test cannot run the trap that restarts the broker it
    stopped."""
    rendered = scaffold(opts, acceptance_specs(opts))
    if opts.get("blue/event") != "create":
        return rendered
    result = await runtime.exec(
        ["bash", f"{tool_dir(opts, acceptance_tool)}/acceptance.sh"],
        timeout_ms=2700000)
    return process_result(rendered, "acceptance", result)


def generated_cleanup_step(opts: dict) -> dict:
    return scaffold(scaffold(opts, ansible_specs(opts)), acceptance_specs(opts))
