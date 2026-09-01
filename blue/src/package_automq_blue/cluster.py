"""Everything that turns ``automq-node-count`` into concrete cluster facts.

This module exists because a three-node cluster has far more derived identity
than a single-node one, and every derivation is a place to be wrong in a way no
exit code reports: a broker that advertises the wrong name is reachable and
useless, a quorum string that disagrees between nodes forms no quorum at all,
and a certificate whose SAN list misses one broker fails only for the client
that happens to be routed there.

Everything here is a pure function of desired state plus the compute stage's
outputs, so the whole of it is reachable from the test suite and visible in the
goldens. Nothing in this file may read the environment, the filesystem, or the
network.
"""

from __future__ import annotations

DEFAULT_NODE_COUNT = 3


def _s(value) -> str:
    """Clojure's `str`: nil renders empty, booleans lowercase."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def node_count(opts: dict) -> int:
    n = opts.get("automq-node-count")
    return n if _int(n) else DEFAULT_NODE_COUNT


def indexes(opts: dict) -> list[int]:
    """Node indexes, ``0..n-1``. The index is the KRaft ``node.id``, the suffix
    in the machine label, and the ordinal in the broker name: one number, so the
    three can never disagree."""
    return list(range(node_count(opts)))


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
    """The cluster's base machine name (Compute Name Standard §1-2): the
    profile, unless desired state overrides it with ``vultr-name``."""
    override = _s(opts.get("vultr-name"))
    return _s(opts.get("profile")) if not override.strip() else override


def machine_name(opts: dict, i: int) -> str:
    """The label of machine ``i``. Numbered because there is more than one; the
    standard names the machine after the profile, and the index disambiguates
    without introducing a second naming scheme."""
    return f"{compute_name(opts)}-{i}"


def machine_names(opts: dict) -> list[str]:
    return [machine_name(opts, i) for i in indexes(opts)]


# --------------------------------------------------------------------- nodes

#: What a credential-free `build` renders in place of a compute output. Fixed
#: addresses from the documentation ranges (RFC 5737 / RFC 1918) so a build is
#: byte-identical on every workstation and the committed goldens mean something.
FALLBACK_NODE = {"ip": "192.0.2.10", "vpc-ip": "10.40.0.10",
                 "user": "root", "sudoer": "root"}


def fallback_nodes(opts: dict) -> list[dict]:
    return [{**FALLBACK_NODE,
             "index": i,
             "name": machine_name(opts, i),
             "ip": f"192.0.2.{10 + i}",
             "vpc-ip": f"10.40.0.{10 + i}",
             "broker-name": broker_name(opts, i)}
            for i in indexes(opts)]


def _by_index(params: list) -> dict[int, dict]:
    return {int(p.get("index")): p for p in params}


def nodes(opts: dict, params=None) -> list[dict]:
    """The node list the Ansible stage and the templates consume.

    ``params`` is the compute stage's output, a list of maps keyed by index. On
    a build there is none, so the fallbacks stand in. On a real run a missing or
    short list is a hard error rather than a silent partial cluster: rendering a
    two-voter quorum string for a three-node cluster would produce a cluster
    that starts and then cannot elect."""
    if not params:
        return fallback_nodes(opts)
    found = _by_index(list(params))
    result = []
    for i in indexes(opts):
        p = found.get(i) or {}
        carried = {k: p[k] for k in ("ip", "vpc-ip", "user", "sudoer") if k in p}
        result.append({**FALLBACK_NODE,
                       "index": i,
                       "name": machine_name(opts, i),
                       "broker-name": broker_name(opts, i),
                       **carried})
    return result


def missing_node_error(opts: dict, params=None) -> str | None:
    """The error for a compute output that does not cover every index, or that
    omits an address. Returned rather than raised so the workflow can report it
    the same way it reports every other failure."""
    if not params:
        return None
    found = _by_index(list(params))
    missing = [i for i in indexes(opts)
               if not (found.get(i)
                       and _s(found[i].get("ip")).strip()
                       and _s(found[i].get("vpc-ip")).strip())]
    if not missing:
        return None
    return ("the compute stage did not report an address for node"
            f"{'s' if len(missing) > 1 else ''} "
            f"{', '.join(str(i) for i in missing)}"
            ". Refusing to render a partial cluster: a quorum string that "
            "names fewer voters than the cluster has will start and then "
            "fail to elect a controller.")


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
