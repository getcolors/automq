#!/usr/bin/env python3
"""Everything this cluster keeps in object storage that is not AutoMQ's own data.

Four protocols share one file because they share one failure model — a process
that dies between two S3 calls — and because the rules for reading each other's
markers have to agree:

  adopt    bucket ownership, as an explicit {empty, init, ready} state machine
  genesis  what authorizes a node to format its metadata storage
  tls      the certificate node 0 issues and every node consumes
  lease    the mutual exclusion that keeps a rolling restart from being a
           simultaneous one

Everything is keyed under `_colors/<profile>/`. AutoMQ's own keys are
`<hash>/_kafka_<cluster-id>/…` at the bucket root, so the two cannot collide.

Credentials come from the environment (AUTOMQ_R2_ACCESS_KEY_ID /
AUTOMQ_R2_SECRET_ACCESS_KEY); nothing here reads a file for them, and nothing
here prints one.
"""

import argparse
import hashlib
import json
import os
import sys
import time
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

SCHEMA = 1
PREFIX = "_colors"


def client(endpoint, region):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=os.environ["AUTOMQ_R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AUTOMQ_R2_SECRET_ACCESS_KEY"],
        # Path style: the endpoint is an account host, not a bucket host.
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
    )


def die(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)


def key(profile, *parts):
    return "/".join([PREFIX, profile, *parts])


def get_json(s3, bucket, k):
    try:
        body = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    return json.loads(body)


def _sign_if_none_match(request, **_kwargs):
    request.headers.add_header("If-None-Match", "*")


def put_json(s3, bucket, k, payload, if_absent=False):
    """Write a marker. `if_absent` makes it a conditional create.

    The conditional form is what makes ownership a claim rather than a hope:
    two deployments that both observed an empty bucket cannot both succeed
    here, so exactly one proceeds and the other fails loudly.

    The header is attached with a botocore event hook rather than boto3's
    `IfNoneMatch` parameter, because the distribution's python3-boto3 predates
    that parameter and rejects it client-side, before any request is made.
    The wire protocol is older than the SDK's support for it, so speaking the
    protocol directly works on the boto3 that is actually installed — and
    avoids a pip/venv dependency on a host that has a perfectly good one.
    """
    args = {
        "Bucket": bucket,
        "Key": k,
        "Body": json.dumps(payload, sort_keys=True, indent=2).encode(),
        "ContentType": "application/json",
    }
    if not if_absent:
        s3.put_object(**args)
        return True

    s3.meta.events.register("before-sign.s3.PutObject", _sign_if_none_match)
    try:
        s3.put_object(**args)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("PreconditionFailed", "ConditionalRequestConflict") or status == 412:
            return False
        raise
    finally:
        s3.meta.events.unregister("before-sign.s3.PutObject", _sign_if_none_match)


def object_count(s3, bucket, limit=1):
    """How many objects the bucket holds, capped at `limit`.

    Paginated and unfiltered on purpose. A prefix check would miss exactly the
    thing that matters most — another AutoMQ cluster's hash-prefixed keys live
    at the bucket root, not under any prefix this package chooses.
    """
    paginator = s3.get_paginator("list_objects_v2")
    seen = 0
    for page in paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": 1000}):
        seen += len(page.get("Contents", []))
        if seen >= limit:
            return seen
    return seen


# ------------------------------------------------------------------- adoption


def identity(args, role):
    return {
        "schema": SCHEMA,
        "profile": args.profile,
        "cluster_id": args.cluster_id,
        "role": role,
        "endpoint": args.endpoint,
        "bucket": args.data_bucket if role == "data" else args.ops_bucket,
    }


def identity_matches(marker, want):
    return all(marker.get(k) == v for k, v in want.items())


def bucket_state(s3, bucket, profile):
    ready = get_json(s3, bucket, key(profile, "colors-ready"))
    if ready:
        return "ready", ready
    init = get_json(s3, bucket, key(profile, "colors-init"))
    if init:
        return "init", init
    return "empty", None


def cmd_adopt(args):
    s3 = client(args.endpoint, args.region)
    buckets = [("data", args.data_bucket), ("ops", args.ops_bucket)]
    states = {}
    for role, bucket in buckets:
        state, marker = bucket_state(s3, bucket, args.profile)
        states[role] = (state, marker, bucket)

    # Identity is immutable. A bucket, endpoint or cluster id that changed
    # underneath a cluster is not a new desired state to converge towards: the
    # old data still exists and is now unreferenced, and the new location is
    # empty. Attaching silently would look like success and lose everything.
    for role, (state, marker, bucket) in states.items():
        if marker is not None:
            want = identity(args, role)
            if not identity_matches(marker, want):
                die(
                    f"refusing to adopt {bucket}: its marker describes a different "
                    f"cluster.\n  stored:  {json.dumps({k: marker.get(k) for k in want})}\n"
                    f"  desired: {json.dumps(want)}\n"
                    "Storage identity is immutable. If this is a deliberate move, "
                    "migrate the data explicitly and converge onto a fresh "
                    "cluster id; do not repoint desired state at storage that "
                    "already belongs to something else."
                )

    pair = (states["data"][0], states["ops"][0])

    if pair == ("ready", "ready"):
        print(json.dumps({"state": "adopted", "txn": states["data"][1]["txn"]}))
        return

    # One side ready, the other still init. `ready` is written to the two
    # buckets in sequence, so a process that dies between them leaves exactly
    # this state — and treating it as fatal would wedge adoption permanently
    # over a partial write of a marker, not over any real disagreement. The
    # transaction ids must still match; that is what proves it is the same
    # adoption rather than two.
    if pair in (("ready", "init"), ("init", "ready")):
        if states["data"][1]["txn"] != states["ops"][1]["txn"]:
            die("refusing to adopt: one bucket is ready and the other carries "
                "an init marker from a different transaction. Two deployments "
                "have touched this storage; resolve by hand.")
        done_role = "data" if pair[0] == "ready" else "ops"
        todo_role = "ops" if done_role == "data" else "data"
        init = states[todo_role][1]
        put_json(s3, states[todo_role][2], key(args.profile, "colors-ready"),
                 dict(init, ready_at=int(time.time())))
        print(json.dumps({"state": "ready-completed", "txn": init["txn"]}))
        return

    if pair == ("init", "init"):
        if states["data"][1]["txn"] != states["ops"][1]["txn"]:
            die(
                "refusing to adopt: the two buckets carry init markers from "
                "different transactions, which means two deployments raced for "
                "this storage. Decide which one owns it, remove the loser's "
                "markers by hand, and retry."
            )
        print(json.dumps({"state": "resumed", "txn": states["data"][1]["txn"]}))
        return

    if pair == ("empty", "empty"):
        # Emptiness is proven before anything is claimed.
        for role, bucket in buckets:
            n = object_count(s3, bucket)
            if n:
                die(
                    f"refusing to adopt {bucket}: it already holds objects. This "
                    "package adopts only empty buckets, because AutoMQ writes "
                    "hash-prefixed keys at the bucket root and cannot be confined "
                    "to a prefix — sharing a bucket means interleaving with "
                    "whatever else is there. Use an empty bucket, or delete the "
                    "contents deliberately."
                )
        txn = uuid.uuid4().hex
        for role, bucket in buckets:
            payload = dict(identity(args, role), txn=txn, claimed_at=int(time.time()))
            if not put_json(s3, bucket, key(args.profile, "colors-init"), payload, if_absent=True):
                die(
                    f"refusing to adopt {bucket}: another process claimed it "
                    "while this one was checking. Exactly one deployment may own "
                    "a bucket; nothing has been changed here."
                )
        print(json.dumps({"state": "claimed", "txn": txn}))
        return

    # One side init or ready, the other empty. The written marker is
    # authoritative — the transaction id is recovered from it rather than from
    # a memory this process does not have — and the empty peer is claimed with
    # that same id.
    if pair in (("init", "empty"), ("empty", "init")):
        done_role = "data" if pair[0] == "init" else "ops"
        todo_role = "ops" if done_role == "data" else "data"
        txn = states[done_role][1]["txn"]
        todo_bucket = states[todo_role][2]
        if object_count(s3, todo_bucket):
            die(
                f"refusing to adopt {todo_bucket}: its peer is already claimed by "
                f"transaction {txn}, but this bucket holds objects it did not "
                "write. Point at an empty bucket for the missing half."
            )
        payload = dict(identity(args, todo_role), txn=txn, claimed_at=int(time.time()))
        if not put_json(s3, todo_bucket, key(args.profile, "colors-init"), payload, if_absent=True):
            die(f"refusing to adopt {todo_bucket}: another process claimed it first.")
        print(json.dumps({"state": "completed", "txn": txn}))
        return

    die(
        f"refusing to adopt: the buckets are in inconsistent states "
        f"(data={pair[0]}, ops={pair[1]}). A ready bucket beside an empty one "
        "means storage was replaced underneath a live cluster. Restore the "
        "missing bucket, or converge onto a fresh cluster id with two empty "
        "buckets; this package will not guess which."
    )


def cmd_ready(args):
    """Write the ready markers. Called once, after the smoke gates pass.

    Rewriting a marker that already exists would report a change on every
    converge and make the deployment's own idempotency claim unfalsifiable.
    """
    s3 = client(args.endpoint, args.region)
    wrote = False
    for role, bucket in (("data", args.data_bucket), ("ops", args.ops_bucket)):
        init = get_json(s3, bucket, key(args.profile, "colors-init"))
        if not init:
            die(f"refusing to mark {bucket} ready: it carries no init marker.")
        if get_json(s3, bucket, key(args.profile, "colors-ready")):
            continue
        put_json(s3, bucket, key(args.profile, "colors-ready"),
                 dict(init, ready_at=int(time.time())))
        wrote = True
    print("ready: written" if wrote else "ready: already marked")


# -------------------------------------------------------------------- genesis


def completed_nodes(s3, args):
    """Node records that reached `format-complete`."""
    done = []
    paginator = s3.get_paginator("list_objects_v2")
    prefix = key(args.profile, "nodes") + "/"
    for page in paginator.paginate(Bucket=args.ops_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            rec = get_json(s3, args.ops_bucket, obj["Key"]) or {}
            if rec.get("phase") == "complete":
                done.append(rec.get("node"))
    return done


def cmd_genesis_state(args):
    """Whether this cluster has ever been initialized.

    Derived from whether any node has COMPLETED a format — never from whether
    a genesis marker was claimed. Claiming first cost this package a cluster:
    a converge claimed genesis, failed during the format, and every later run
    then read "already initialized" and formatted its nodes WITHOUT bootstrap
    credentials. The result was a metadata log with no SCRAM records at all,
    which no amount of re-running could repair — the only symptom being
    "Authentication failed ... invalid credentials" from every principal,
    including ones nothing had ever changed.

    The rule that avoids it: a marker may record that something happened, but
    only evidence that it happened may be used to decide what to do next.
    """
    s3 = client(args.endpoint, args.region)
    done = completed_nodes(s3, args)
    g = get_json(s3, args.ops_bucket, key(args.profile, "genesis")) or {}
    print(json.dumps({"initialized": bool(done),
                      "formatted_nodes": sorted(n for n in done if n is not None),
                      "epoch": g.get("epoch")}))


def cmd_genesis_claim(args):
    """Record the genesis epoch, for provenance only.

    Called AFTER the formats succeed. Nothing decides anything from this
    marker; it exists so an operator can see when the cluster was born.
    """
    s3 = client(args.endpoint, args.region)
    payload = {
        "schema": SCHEMA,
        "profile": args.profile,
        "cluster_id": args.cluster_id,
        "epoch": int(time.time()),
    }
    created = put_json(s3, args.ops_bucket, key(args.profile, "genesis"), payload, if_absent=True)
    g = get_json(s3, args.ops_bucket, key(args.profile, "genesis"))
    print(json.dumps({"claimed": created, "epoch": g["epoch"]}))


def cmd_format_status(args):
    s3 = client(args.endpoint, args.region)
    rec = get_json(s3, args.ops_bucket, key(args.profile, "nodes", f"{args.node}.json"))
    print(json.dumps(rec or {"phase": "none"}))


def cmd_format_record(args):
    """Two-phase, and that is the whole point.

    A single record written before formatting cannot distinguish a converge
    that died mid-format from a disk that was lost afterwards — and the two
    demand opposite responses. `intent` says formatting was authorized and may
    be retried; `complete` says this node has real metadata, so missing
    metadata afterwards is data loss and must stop the run.
    """
    s3 = client(args.endpoint, args.region)
    k = key(args.profile, "nodes", f"{args.node}.json")
    prev = get_json(s3, args.ops_bucket, k) or {}
    payload = dict(
        prev,
        schema=SCHEMA,
        node=args.node,
        cluster_id=args.cluster_id,
        phase=args.phase,
        **{f"{args.phase}_at": int(time.time())},
    )
    put_json(s3, args.ops_bucket, k, payload)
    print(args.phase)


# ------------------------------------------------------------------------ tls


def cmd_tls_publish(args):
    s3 = client(args.endpoint, args.region)
    with open(args.cert, "rb") as fh:
        cert = fh.read()
    with open(args.keyfile, "rb") as fh:
        keydata = fh.read()
    bundle = {
        "schema": SCHEMA,
        "profile": args.profile,
        "names": args.names.split(","),
        "cert": cert.decode(),
        "key": keydata.decode(),
        "published_at": int(time.time()),
        "fingerprint": hashlib.sha256(cert).hexdigest(),
    }
    put_json(s3, args.ops_bucket, key(args.profile, "tls", "bundle.json"), bundle)
    print(bundle["fingerprint"])


def cmd_tls_fingerprint(args):
    s3 = client(args.endpoint, args.region)
    b = get_json(s3, args.ops_bucket, key(args.profile, "tls", "bundle.json"))
    print((b or {}).get("fingerprint", ""))


def cmd_tls_fetch(args):
    s3 = client(args.endpoint, args.region)
    b = get_json(s3, args.ops_bucket, key(args.profile, "tls", "bundle.json"))
    if not b:
        die("no certificate bundle has been published yet")
    os.makedirs(args.dir, mode=0o700, exist_ok=True)
    cert_path = os.path.join(args.dir, "fullchain.pem")
    key_path = os.path.join(args.dir, "privkey.pem")
    # Written by rename so a reader never sees a half-written key.
    for path, data in ((cert_path, b["cert"]), (key_path, b["key"])):
        tmp = path + ".tmp"
        with open(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as fh:
            fh.write(data)
        os.replace(tmp, path)
    print(b["fingerprint"])


# ---------------------------------------------------------------------- lease


def _etag(s3, bucket, k):
    try:
        return s3.head_object(Bucket=bucket, Key=k).get("ETag")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def _put_if_match(s3, bucket, k, payload, etag):
    """Replace an object only if it still has the ETag we read."""
    def handler(request, **_kwargs):
        request.headers.add_header("If-Match", etag)

    s3.meta.events.register("before-sign.s3.PutObject", handler)
    try:
        s3.put_object(
            Bucket=bucket, Key=k, ContentType="application/json",
            Body=json.dumps(payload, sort_keys=True, indent=2).encode(),
        )
        return True
    except ClientError as e:
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if e.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict") or status == 412:
            return False
        raise
    finally:
        s3.meta.events.unregister("before-sign.s3.PutObject", handler)


def cmd_lease_acquire(args):
    """Take the rolling-restart lease, or report that someone else holds it.

    Independent timers on three nodes observe a new certificate at the same
    moment, and each would pass its own local quorum check before any peer had
    gone down: a local check cannot order independent actors. This is what
    orders them. These are combined broker+controller nodes, so a simultaneous
    restart destroys the majority.

    Every transition is conditional. Taking a free lease is a conditional
    create; taking an EXPIRED lease is a conditional replace against the exact
    ETag that was read. An unconditional overwrite of an expired lease lets two
    contenders both read it, both write, and both believe they hold it — which
    is precisely the simultaneous restart the lease exists to prevent.
    """
    s3 = client(args.endpoint, args.region)
    k = key(args.profile, "lease", f"{args.name}.json")
    now = int(time.time())
    payload = {"schema": SCHEMA, "holder": args.holder, "at": now, "ttl": args.ttl}

    etag = _etag(s3, args.ops_bucket, k)
    if etag is None:
        acquired = put_json(s3, args.ops_bucket, k, payload, if_absent=True)
        print(json.dumps({"acquired": acquired,
                          "holder": args.holder if acquired else "raced"}))
        return

    held = get_json(s3, args.ops_bucket, k) or {}
    mine = held.get("holder") == args.holder
    expired = now - held.get("at", 0) >= held.get("ttl", args.ttl)
    if not mine and not expired:
        print(json.dumps({"acquired": False, "holder": held.get("holder")}))
        return

    # Conditional on the ETag we just read: a competitor that renewed or took
    # the lease in between invalidates it and we lose the race cleanly.
    won = _put_if_match(s3, args.ops_bucket, k, payload, etag)
    print(json.dumps({"acquired": won, "holder": args.holder if won else "raced"}))


def cmd_lease_release(args):
    """Release the lease, but only if we still hold it.

    An unconditional delete lets a node whose lease expired — while it was
    still restarting — delete its successor's lease on the way out, handing a
    third node a lease that is already in use.
    """
    s3 = client(args.endpoint, args.region)
    k = key(args.profile, "lease", f"{args.name}.json")
    held = get_json(s3, args.ops_bucket, k) or {}
    if held.get("holder") != args.holder:
        print(json.dumps({"released": False, "holder": held.get("holder")}))
        return
    try:
        s3.delete_object(Bucket=args.ops_bucket, Key=k)
    except ClientError:
        pass
    print(json.dumps({"released": True}))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profile", required=True)
    p.add_argument("--cluster-id", default="")
    p.add_argument("--endpoint", required=True)
    p.add_argument("--region", default="auto")
    p.add_argument("--data-bucket", default="")
    p.add_argument("--ops-bucket", default="")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("adopt").set_defaults(fn=cmd_adopt)
    sub.add_parser("ready").set_defaults(fn=cmd_ready)
    sub.add_parser("genesis-state").set_defaults(fn=cmd_genesis_state)
    sub.add_parser("genesis-claim").set_defaults(fn=cmd_genesis_claim)

    s = sub.add_parser("format-status")
    s.add_argument("--node", required=True)
    s.set_defaults(fn=cmd_format_status)

    s = sub.add_parser("format-record")
    s.add_argument("--node", required=True)
    s.add_argument("--phase", required=True, choices=["intent", "complete"])
    s.set_defaults(fn=cmd_format_record)

    s = sub.add_parser("tls-publish")
    s.add_argument("--cert", required=True)
    s.add_argument("--keyfile", required=True)
    s.add_argument("--names", required=True)
    s.set_defaults(fn=cmd_tls_publish)

    sub.add_parser("tls-fingerprint").set_defaults(fn=cmd_tls_fingerprint)

    s = sub.add_parser("tls-fetch")
    s.add_argument("--dir", required=True)
    s.set_defaults(fn=cmd_tls_fetch)

    s = sub.add_parser("lease-acquire")
    s.add_argument("--name", default="restart")
    s.add_argument("--holder", required=True)
    s.add_argument("--ttl", type=int, default=600)
    s.set_defaults(fn=cmd_lease_acquire)

    s = sub.add_parser("lease-release")
    s.add_argument("--name", default="restart")
    s.add_argument("--holder", required=True)
    s.set_defaults(fn=cmd_lease_release)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
