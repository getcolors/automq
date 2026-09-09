"""AutoMQ application facts built from the shared compute contract."""

from __future__ import annotations

from colors_compute import collect, expand
from colors_compute.deployment_request import deployment_requests, source_cidrs
from colors_compute.planning import plan_deployment

DEFAULT_NODE_COUNT = 3
default_compute_provider = 'vultr'


def topology(opts):
    return [{'role': None, 'count': opts.get('automq-node-count', DEFAULT_NODE_COUNT)}]


def _sources(opts, name):
    return source_cidrs(opts, name, 'automq-' + name)


def requirements(opts):
    ingress = [{'id': 'ssh', 'protocol': 'tcp', 'from_port': 22, 'to_port': 22, 'sources': _sources(opts, 'ssh-sources')}]
    kafka = _sources(opts, 'kafka-sources')
    if kafka:
        ingress.append({'id': 'kafka', 'protocol': 'tcp', 'from_port': kafka_port(opts), 'to_port': kafka_port(opts), 'sources': kafka})
    for name, port in [('controller', controller_port(opts)), ('internal', internal_port(opts))]:
        ingress.append({'id': name, 'protocol': 'tcp', 'from_port': port, 'to_port': port, 'sources': ['private']})
    return {'security': {'ingress': ingress, 'egress': 'all', 'private_filter': True},
            'private': True, 'legacy_state_keys': [str(opts.get('profile')) + '/automq-infrastructure.tfstate']}


def _requests(opts):
    return deployment_requests(opts, topology(opts), requirements(opts), {'mode': 'managed', 'public_key': 'ssh-ed25519 PLACEHOLDER managed-by-colors'})


def _s(value) -> str:
    """Clojure's `str`: nil renders empty, booleans lowercase."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


# ------------------------------------------------------------------- names


def node_count(opts: dict) -> int:
    """Use normalized library results for application rendering."""
    return topology(opts)[0]['count']


def indexes(opts: dict) -> list[int]:
    """Use normalized library results for application rendering."""
    return [node["index"] for node in expand(topology(opts))]


def broker_name(opts: dict, i: int) -> str:
    """The public name broker ``i`` advertises, ``b<i>.<automq-host>``.

    Kafka redirects a client from the bootstrap name to whatever a broker
    advertises, so this name must resolve publicly and must appear in that
    broker's certificate. Both the DNS stage and the SAN list below derive from
    this one function."""
    prefix = _s(opts.get("automq-broker-name-prefix")) or "b"
    return f"{prefix}{i}.{_s(opts.get('automq-host'))}"


def broker_names(opts: dict) -> list[str]:
    return [broker_name(opts, i) for i in indexes(opts)]


def certificate_names(opts: dict) -> list[str]:
    """The exact SAN list: the bootstrap name plus every broker name.

    Derived rather than guessed. An earlier design used a wildcard, which
    required deriving the zone from the host and left the apex needing its own
    SAN anyway; enumerating the names this cluster actually serves is both
    shorter and checkable."""
    return [_s(opts.get("automq-host")), *broker_names(opts)]


def compute_name(opts: dict) -> str:
    """Use normalized library results for application rendering."""
    return _requests(opts)['shared']['name']


def machine_name(opts: dict, i: int) -> str:
    """The label of machine ``i``, ``<compute-name>-<i>``: the Cluster
    Standard's fallback name for the None role, which is also what the template
    labels the instance. Numbered because there is more than one; the standard
    names the machine after the profile, and the index disambiguates without
    introducing a second naming scheme."""
    return _requests(opts)['nodes'][i]['name']


def machine_names(opts: dict) -> list[str]:
    return [machine_name(opts, i) for i in indexes(opts)]


# --------------------------------------------------------------------- nodes


def _automq_node(opts: dict, node: dict) -> dict:
    """Use normalized library results for application rendering."""
    result = {k: v for k, v in node.items() if k != "vpc_ip"}
    result["vpc-ip"] = node.get("vpc_ip")
    result["broker-name"] = broker_name(opts, node["index"])
    return result


def fallback_nodes(opts: dict) -> list[dict]:
    """Use normalized library results for application rendering."""
    return [_automq_node(opts, n) for n in plan_deployment(opts, topology(opts), requirements(opts))['cluster']['nodes']]


def nodes(opts: dict, params=None) -> list[dict]:
    """Use normalized library results for application rendering."""
    recorded = params or opts.get('colors-compute/cluster')
    if recorded is None:
        if opts.get('blue/event') == 'build' or opts.get('blue/dry-run'):
            return fallback_nodes(opts)
        raise ValueError('compute cluster unavailable; refusing placeholder inventory')
    declarations = recorded.get('nodes', []) if opts.get('blue/event') == 'delete' else expand(topology(opts))
    requests = [{**node, 'private': True, 'provider': opts['provider-compute']} for node in declarations]
    checked = collect(requests, recorded.get('nodes', []), requests[0]['node_id'])
    return [_automq_node(opts, n) for n in checked['nodes']]


# ----------------------------------------------------------------- listeners


def controller_port(opts: dict) -> int:
    value = opts.get("automq-controller-port")
    return 9093 if value is None else value


def internal_port(opts: dict) -> int:
    value = opts.get("automq-internal-port")
    return 9094 if value is None else value


def kafka_port(opts: dict) -> int:
    value = opts.get("automq-kafka-port")
    return 9092 if value is None else value


def quorum_voters(opts: dict, nodes_: list[dict]) -> str:
    """``controller.quorum.voters``, identical on every node.

    Static rather than dynamic: three fixed nodes are desired state, and a
    static list is what makes the rendered configuration deterministic and the
    goldens meaningful. Built from VPC addresses — the quorum never crosses the
    public interface."""
    return ",".join(f"{n['index']}@{n['vpc-ip']}:{controller_port(opts)}"
                    for n in nodes_)


def listeners(opts: dict, n: dict) -> str:
    """``listeners`` for node ``n``. CONTROLLER and INTERNAL bind the VPC
    address specifically, which is why the container runs with host networking:
    a bridged container cannot bind an address that belongs only to the host.
    EXTERNAL binds every interface because it is the public endpoint."""
    return (f"CONTROLLER://{n['vpc-ip']}:{controller_port(opts)}"
            f",INTERNAL://{n['vpc-ip']}:{internal_port(opts)}"
            f",EXTERNAL://0.0.0.0:{kafka_port(opts)}")


def advertised_listeners(opts: dict, n: dict) -> str:
    """What node ``n`` tells clients to come back to. INTERNAL advertises the
    VPC address; EXTERNAL advertises this broker's own public name, which must
    resolve and must be in its certificate. CONTROLLER is deliberately absent —
    Kafka rejects a controller entry in ``advertised.listeners``."""
    return (f"INTERNAL://{n['vpc-ip']}:{internal_port(opts)}"
            f",EXTERNAL://{n['broker-name']}:{kafka_port(opts)}")


# ---------------------------------------------------------------- principals


def _principal(value, fallback: str) -> str:
    return _s(value) or fallback


def admin_user(opts: dict) -> str:
    return _principal(opts.get("automq-admin-user"), "automq-admin")


def broker_user(opts: dict) -> str:
    return _principal(opts.get("automq-broker-user"), "automq-broker")


def controller_user(opts: dict) -> str:
    return _principal(opts.get("automq-controller-user"), "automq-controller")


def client_user(opts: dict) -> str:
    return _principal(opts.get("automq-sasl-user"), "automq")


def scram_principals(opts: dict) -> list[str]:
    """The principals bootstrapped into the metadata log by the genesis format.

    The controller principal is deliberately absent: it authenticates with
    PLAIN from a static JAAS file, precisely so that forming the controller
    quorum depends on nothing stored in the metadata log the quorum is trying to
    serve."""
    return [admin_user(opts), broker_user(opts), client_user(opts)]


def super_users(opts: dict) -> str:
    """``super.users``. The client principal is never here — it is ACL-scoped,
    and a public endpoint whose only authenticated identity is a superuser is an
    authorization hole with a password on it."""
    return ";".join(f"User:{user}" for user in
                    (admin_user(opts), broker_user(opts), controller_user(opts)))


def topic_prefix(opts: dict) -> str:
    return _principal(opts.get("automq-client-topic-prefix"), "colors-")


def client_acls(opts: dict) -> list[dict]:
    """The client principal's complete authority, enumerated so it can be read
    and tested rather than inferred. No Create, no Alter, no ClusterAction, no
    TransactionalId — acceptance asserts the denials as well as the grants."""
    user, prefix = client_user(opts), topic_prefix(opts)
    return [{"principal": user, "resource-type": "topic",
             "pattern-type": "prefixed", "name": prefix,
             "operations": ["Describe", "Read", "Write"]},
            {"principal": user, "resource-type": "group",
             "pattern-type": "prefixed", "name": prefix,
             "operations": ["Describe", "Read"]}]
