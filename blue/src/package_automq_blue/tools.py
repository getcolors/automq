"""Compute, DNS, local SSH config, cluster convergence, and acceptance stages."""

from __future__ import annotations

import json
from pathlib import Path

from blue import tofu
from blue.ansible import ansible_with_spec, parse_recap
from blue.cli import stage_dir
from blue.runtime import runtime
from blue.scaffold import PRESERVE_JINJA_DELIMITERS, content_spec, scaffold
from colors_compute.orchestration import orchestrate
from colors_compute.planning import plan_deployment
from package_once_blue.utils import registrable_domain

from . import cluster, ssh_config, validate, storage

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


# the validator and the templates can never disagree about what an entry is.



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


def nodes(opts):
    return cluster.nodes(opts, opts.get('colors-compute/cluster'))


async def infrastructure_step(opts):
    planning = opts.get('blue/event') == 'build' or opts.get('blue/dry-run')
    if planning:
        result = plan_deployment(opts, cluster.topology(opts), cluster.requirements(opts))
        directory = tool_dir(opts, infrastructure_tool)
        for stage, documents in [('shared', result['documents']['shared']), *[(f'nodes/{node}', docs) for node, docs in result['documents']['nodes'].items()]]:
            for filename, document in documents.items():
                target = Path(directory) / stage / filename
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps(document, sort_keys=True, indent=2) + '\n')
    else:
        result = await orchestrate(opts, cluster.topology(opts), cluster.requirements(opts))
    if result['status'] not in ('ready', 'planned', 'destroyed'):
        return {**opts, 'blue/exit': 1, 'blue/err': '\n'.join(result.get('errors', [])) or 'compute lifecycle refused; inspect state ownership and configuration'}
    values = {**opts, 'blue/exit': 0}
    if 'cluster' in result:
        values['colors-compute/cluster'] = result['cluster']
    path = result.get('key', {}).get('private_key_path')
    if path:
        values['ssh-private-key-path'] = path.replace('$HOME/.ssh', '/home/build-placeholder/.ssh') if planning else path
    return values


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
    if opts.get("provider-dns") == "none":
        return {**opts, "blue/exit": 0}
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
            "ssh-config-identity-file": ssh_config.identity_file(opts) if validate.keygen(opts) else opts.get("ssh-private-key-path", ""),
            "host-alias": ssh_config.host_alias(opts)}


def ansible_local_specs(opts: dict) -> list[dict]:
    dir = tool_dir(opts, ansible_local_tool)
    data = ansible_local_data(opts)
    return [spec(template("ansible-local", name), f"{dir}/{name}", data)
            for name in ["ansible.cfg", "inventory.ini", "main.yml"]]


def ssh_config_hosts(opts: dict, nodes_: list[dict]) -> list[dict]:
    """Use normalized library results for application rendering."""
    return [{**nodes_[0], 'name': opts['profile']}, *[{**node, 'name': opts['profile'] + '-' + str(node['index'])} for node in nodes_]]


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
        if opts.get("ssh-private-key-path"):
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
    opts = {key: value for key, value in opts.items() if key != "automq/storage-credentials"}
    names = [node["ip"] for node in nodes_] if opts.get("provider-dns") == "none" else cluster.certificate_names(opts)
    return {**opts,
            "ssh-keygen": validate.keygen(opts) or bool(opts.get("ssh-private-key-path")),
            "node-count": cluster.node_count(opts),
            "quorum-voters": cluster.quorum_voters(opts, nodes_),
            "certificate-names": names,
            "certificate-names-csv": ",".join(names),
            "bootstrap-internal": ",".join(
                f"{n['vpc-ip']}:{cluster.internal_port(opts)}" for n in nodes_),
            "bootstrap-external": f"{nodes_[0]['ip'] if opts.get('provider-dns') == 'none' else opts.get('automq-host')}:{cluster.kafka_port(opts)}",
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
    if opts.get("blue/event") == "delete" and opts.get("colors-compute/cluster") is None:
        # A readable state without compute: there is nothing to stop, and the
        # cleanup play would only fail against the placeholder addresses. (An
        # unreadable state, or a partial one, never reaches here — the delete
        # failed closed at adoption.)
        return {**opts, "blue/exit": 1, "blue/err": "compute inventory unavailable"}
    if storage.managed(opts) and opts.get("blue/event") == "create":
        rendered = scaffold(opts, ansible_specs(opts))
        result = await runtime.exec(["ansible-playbook", "-i", "inventory.json", "main.yml"], cwd=dir, env=storage.credential_env(opts), timeout_ms=7200000)
        if result.exit == 0:
            return {**rendered, "blue/exit": 0, "ansible/recap": parse_recap(result.out)}
        return {**rendered, "blue/exit": 1, "blue/err": "Ansible convergence failed: " + result.out + result.err}
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
    if result.out:
        print(result.out, end="", flush=True)
    return process_result(rendered, "acceptance", result)


def generated_cleanup_step(opts: dict) -> dict:
    return scaffold(scaffold(opts, ansible_specs(opts)), acceptance_specs(opts))
