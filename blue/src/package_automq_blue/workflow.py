"""The AutoMQ lifecycle DAG, the port of io.github.getcolors.automq.workflow."""

from __future__ import annotations

import os

from blue import dry_run, progress, tofu
from blue.cli import par_name, read_pars
from blue.lifecycle import preflight
from blue.workflow import advice_add, failed, workflow
from colors_compute.inspection import read_deployment
from colors_compute import finalize_backend

from . import cluster, ssh, ssh_config, tools, validate, storage

DEFAULTS = {
    "provider-compute": validate.default_compute_provider,
    "provider-dns": "cloudflare",
    "automq-tls-mode": "acme",
    "provider-backend": "r2",
    "compute-prevent-destroy": True,
    "workdir": ".colors",
    "automq-node-count": cluster.DEFAULT_NODE_COUNT,
    "automq-broker-name-prefix": "b",
    "automq-kafka-port": 9092,
    "automq-internal-port": 9094,
    "automq-controller-port": 9093,
    "automq-sasl-user": "automq",
    "automq-admin-user": "automq-admin",
    "automq-broker-user": "automq-broker",
    "automq-controller-user": "automq-controller",
    "automq-sasl-mechanism": "SCRAM-SHA-512",
    "automq-client-topic-prefix": "colors-",
    "automq-topic-partitions": 6,
    "automq-log-retention-hours": 168,
    "automq-r2-region": "auto",
    "automq-wal-batch-interval-ms": 250,
    "automq-wal-max-bytes-in-batch": 8388608,
}

# Events that authenticate against Vultr and require the local toolchain.
CHECKED_EVENTS = ("create", "delete", "validate")


async def start_step(original, env=None):
    environment = dict(os.environ if env is None else env)
    overlaid = read_pars({**DEFAULTS, **original}, environment)
    real = not overlaid.get('blue/dry-run') and overlaid.get('blue/event') in CHECKED_EVENTS
    runtime_errors = await validate.runtime_errors(overlaid) if real else []
    async def after(opts, _env, context):
        if context['real'] and context['event'] == 'delete':
            result = await read_deployment(opts, environment)
            if result['status'] != 'present' and opts.get(str(opts.get('provider-backend')) + '-bucket-mode') == 'managed':
                return {**opts, 'automq/finalize-only': True, 'blue/exit': 0}
            if result['status'] == 'destroyed':
                return {**opts, 'automq/already-destroyed': True, 'blue/exit': 0}
            if result['status'] != 'present':
                return {**opts, 'blue/exit': 1, 'blue/err': 'compute state unavailable; legacy monolithic state requires explicit migration'}
            opts = {**opts, 'colors-compute/cluster': result['cluster']}
            path = result.get('key', {}).get('private_key_path')
            if path:
                opts['ssh-private-key-path'] = path
            return {**opts, 'blue/exit': 0}
        if context['real'] and context['event'] == 'create':
            return ssh_config.preflight(opts)
        return {**ssh.with_machine_key(opts), 'blue/exit': 0}
    return await preflight(original, defaults=DEFAULTS, overlay=read_pars, env=env,
        validators=[lambda _o, e, _c: validate.env_errors(e),
                    lambda o, _e, _c: validate.state_errors(o),
                    lambda o, _e, c: validate.secret_errors(o, c['event']) if c['real'] and c['event'] in CHECKED_EVENTS and not validate.state_errors(o) else [],
                    lambda o, _e, c: ['compute destruction is protected; set COLORS_PAR_COMPUTE_PREVENT_DESTROY=false to delete'] if c['real'] and c['event'] == 'delete' and o.get('compute-prevent-destroy') else [],
                    lambda _o, _e, _c: runtime_errors], after_validate=after)


def wire_fn(step: str, run_opts: dict):
    # `validate` answers "would this run?" and must not render or plan anything
    # to do it. Falling through to the create chain would call `tofu validate`
    # on a compute stage that reads the machine public key — a file only a real
    # create generates — so the check would fail on exactly the fresh checkout
    # it exists to serve.
    if run_opts.get("blue/event") == "validate":
        return {"automq/start": (start_step,)}.get(step)
    if run_opts.get("blue/event") == "delete":
        # The `~/.ssh/config` block goes before the destroy, the keypair after
        # it. A block that outlives its host is stale but harmless; a key that
        # predeceases its host locks the operator out of machines that still
        # exist. Both orders are deliberate — standards/ssh-config.md §4 is
        # explicit that they must not be tidied into agreement.
        return {
            "automq/start": (start_step, "automq/ansible"),
            "automq/ansible": (tools.ansible_step, "automq/ssh-config"),
            "automq/ssh-config": (tools.ansible_local_step, "automq/dns"),
            # DNS goes before the compute destroy: records pointing at addresses
            # that have been released are worse than no records, because a
            # reissued address makes them point at somebody else's machine.
            "automq/dns": (tools.dns_step, "automq/storage" if run_opts.get("automq-storage-managed") else "automq/infrastructure"),
            "automq/storage": (storage.storage_step, "automq/infrastructure"),
            "automq/infrastructure": (tools.infrastructure_step, "automq/backend-finalize") if run_opts.get(str(run_opts.get("provider-backend")) + "-bucket-mode") == "managed" else (tools.infrastructure_step,),
            "automq/backend-finalize": (backend_finalize_step,),
        }.get(step)
    return {
        "automq/start": (start_step, "automq/infrastructure"),
        "automq/infrastructure": (tools.infrastructure_step, "automq/storage" if run_opts.get("automq-storage-managed") else "automq/ssh-config"),
        "automq/storage": (storage.storage_step, "automq/ssh-config"),
        "automq/ssh-config": (tools.ansible_local_step, "automq/dns"),
        # DNS before convergence, because every broker advertises a name that
        # must already resolve — and because the certificate is issued for those
        # names during the play.
        "automq/dns": (tools.dns_step, "automq/ansible"),
        "automq/ansible": (tools.ansible_step, "automq/acceptance"),
        "automq/acceptance": (tools.acceptance_step,),
    }.get(step)


def backend_advice(tool: str):
    conventional = tofu.conventional_backend_advice(
        dir=lambda o, tool=tool: tools.tool_dir(o, tool),
        key=lambda o, tool=tool: f"{o.get('profile') or ''}/{tool}.tfstate")
    gcs = tofu.gcs_backend_advice(
        lambda o: tools.tool_dir(o, tool),
        lambda o: {"bucket": o["gcs-bucket"], "prefix": f"{o.get('profile') or ''}/{tool}.tfstate"})
    return lambda opts: gcs(opts) if opts.get("provider-backend") == "gcs" else conventional(opts)


side_effecting = ["automq/infrastructure", "automq/dns", "automq/ssh-config",
                  "automq/ansible", "automq/acceptance", "automq/ssh-cleanup", "automq/storage", "automq/backend-finalize"]


async def backend_finalize_step(opts):
    try:
        result = await finalize_backend(opts, {**os.environ, **storage.aws_env(opts)})
        if result["status"] in ("skipped", "absent", "destroyed"):
            return {**opts, "blue/exit": 0}
        return {**opts, "blue/exit": 1, "blue/err": "managed backend finalization failed"}
    except Exception:
        return {**opts, "blue/exit": 1, "blue/err": "managed backend finalization failed; inspect ownership and remaining state"}


def next_steps(step, successors, opts):
    if failed(opts):
        return []
    if opts.get("automq/already-destroyed"):
        return []
    if step == "automq/start" and opts.get("automq/finalize-only"):
        return [("automq/backend-finalize", opts)]
    return [(successor, opts) for successor in successors or []]


def create_workflow():
    wf = workflow(start="automq/start", wire_fn=wire_fn, next_fn=next_steps)
    wf = advice_add(wf, "automq/dns", "before", "automq.workflow/backend",
                    backend_advice(tools.dns_tool))
    wf = advice_add(wf, "automq/storage", "before", "automq.workflow/storage-backend", backend_advice(storage.tool))
    return dry_run.advise(progress.advise(wf), side_effecting)


automq_workflow = create_workflow()
