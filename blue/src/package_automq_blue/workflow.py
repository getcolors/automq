"""The AutoMQ lifecycle DAG, the port of io.github.getcolors.automq.workflow."""

from __future__ import annotations

import os

from blue import dry_run, progress, tofu
from blue.cli import par_name, read_pars
from blue.lifecycle import preflight
from blue.workflow import advice_add, failed, workflow
from package_once_blue import compute as once_compute
from package_once_blue import compute_cluster as once_cluster

from . import cluster, ssh, ssh_config, tools, validate

DEFAULTS = {
    "provider-compute": validate.default_compute_provider,
    "provider-dns": "cloudflare",
    "provider-backend": "local",
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
    "vultr-vpc-subnet": "10.40.0.0/24",
}

# Events that authenticate against Vultr and require the local toolchain.
CHECKED_EVENTS = ("create", "delete", "validate")


async def start_step(original: dict, env: dict | None = None) -> dict:
    # The tool and Vultr checks shell out, and preflight's validators are
    # synchronous — so they run here, over the same overlaid state preflight
    # will build, and reach the validator list through a closure. Rebuilding the
    # overlay is deliberate: reporting a missing tool only on the run *after*
    # the operator fixed their colors.yml is exactly the "one thing at a time"
    # behaviour exit code 2 exists to avoid. The compute state is read once here
    # too, on the same overlaid opts — the overlay is what carries the backend
    # credentials — and only for the two events that touch a provider; the
    # validator and the after-validate share the one read. Both reads go
    # through their module attributes so tests can replace them.
    environment = dict(os.environ if env is None else env)
    overlaid = read_pars({**DEFAULTS, **original}, environment)
    event = overlaid.get("blue/event")
    context = {"event": event, "real": not overlaid.get("blue/dry-run")}
    runtime_errors = (await validate.runtime_errors(overlaid)
                      if context["real"] and event in CHECKED_EVENTS else [])
    state = (await once_cluster.read_state(overlaid, tools.state_output)
             if once_compute.lifecycle_event(context) else {})

    # The machine key's create matrix and the Vultr preflight run before any
    # template is rendered: an unowned key on disk or at the provider stops the
    # run while stopping is still free. Delete fills the same template values —
    # a destroy renders before it destroys — and adopts the recorded cluster
    # under `once/cluster`, failing closed on a backend it cannot read and on a
    # state that does not describe every node; but it checks no key, because
    # its key cleanup runs after the compute destroy.
    async def after(opts, _env, ctx):
        real, event = ctx["real"], ctx["event"]
        if real and event == "delete":
            return once_cluster.adopt_state(validate.spec, opts, "delete", state)
        if real and event == "create":
            async def recorded(_opts):
                return state.get("params")
            opts = await ssh.ensure_key(opts, recorded)
            if failed(opts):
                return opts
            opts = ssh.preflight(ssh.with_machine_key(opts))
            if failed(opts):
                return opts
            opts = ssh_config.preflight(opts)
            if failed(opts):
                return opts
            return {**opts, "blue/exit": 0}
        return {**ssh.with_machine_key(opts), "blue/exit": 0}

    return await preflight(
        original, defaults=DEFAULTS, overlay=read_pars, env=env,
        validators=[
            lambda _o, e, _c: validate.env_errors(e),
            lambda o, _e, _c: validate.state_errors(o),
            # Compute Provider Standard §4 before the credentials: a recorded
            # provider that differs from the selected one reports the
            # actionable error, not a missing token for the provider that was
            # just selected.
            lambda o, _e, c: (once_cluster.provider_validator(
                validate.spec, o, state.get("params"),
                lambda: validate.secret_errors(o, c["event"]))
                if once_compute.lifecycle_event(c) else []),
            lambda o, _e, c: ([f"compute destruction is protected; set "
                               f"{par_name('compute-prevent-destroy')}=false to delete"]
                              if c["real"] and c["event"] == "delete"
                              and o.get("compute-prevent-destroy") else []),
            lambda _o, _e, _c: runtime_errors,
        ],
        after_validate=after)


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
            "automq/dns": (tools.dns_step, "automq/infrastructure"),
            "automq/infrastructure": (tools.infrastructure_step, "automq/ssh-cleanup"),
            "automq/ssh-cleanup": (ssh.cleanup_step,),
        }.get(step)
    return {
        "automq/start": (start_step, "automq/infrastructure"),
        "automq/infrastructure": (tools.infrastructure_step, "automq/ssh-config"),
        "automq/ssh-config": (tools.ansible_local_step, "automq/dns"),
        # DNS before convergence, because every broker advertises a name that
        # must already resolve — and because the certificate is issued for those
        # names during the play.
        "automq/dns": (tools.dns_step, "automq/ansible"),
        "automq/ansible": (tools.ansible_step, "automq/acceptance"),
        "automq/acceptance": (tools.acceptance_step,),
    }.get(step)


def backend_advice(tool: str):
    return tofu.conventional_backend_advice(
        dir=lambda o, tool=tool: tools.tool_dir(o, tool),
        key=lambda o, tool=tool: f"{o.get('profile') or ''}/{tool}.tfstate")


side_effecting = ["automq/infrastructure", "automq/dns", "automq/ssh-config",
                  "automq/ansible", "automq/acceptance", "automq/ssh-cleanup"]


def create_workflow():
    wf = workflow(start="automq/start", wire_fn=wire_fn)
    wf = advice_add(wf, "automq/infrastructure", "before", "automq.workflow/backend",
                    backend_advice(tools.infrastructure_tool))
    wf = advice_add(wf, "automq/dns", "before", "automq.workflow/backend",
                    backend_advice(tools.dns_tool))
    return dry_run.advise(progress.advise(wf), side_effecting)


automq_workflow = create_workflow()
