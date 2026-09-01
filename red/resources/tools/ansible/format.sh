#!/usr/bin/env bash
# Format this node's metadata storage — but only when formatting is the right
# answer, which is the entire content of this script.
#
# Three states are distinguishable and demand different things:
#
#   genesis      the cluster has never existed. Format WITH bootstrap records,
#                so the SCRAM principals exist in the metadata log from the
#                first moment and no window exists where the cluster is up
#                without credentials.
#   replacement  the cluster exists, this node does not. Format WITHOUT
#                bootstrap records: KRaft replicates SCRAM credentials in the
#                metadata log, and injecting a second copy into a replacement
#                voter's checkpoint would be inconsistent, not helpful.
#   disk loss    the cluster exists, this node completed a format before, and
#                its metadata is gone. That is data loss. Refuse, and say so.
#
# The distinction between the last two is why the format record is two-phase.
set -euo pipefail

NODE_ID="${1:?node id required}"
# Whether THIS run is the cluster's genesis, decided once on node 0 before any
# node formats and passed down. Deciding it per node would race: three nodes
# each querying "has the cluster been initialized?" before any of them claims
# it would all answer no, and a rerun after a partial failure would answer
# differently for different nodes.
GENESIS="${2:?genesis flag required}"
META=/var/lib/automq/metadata/meta.properties
KAFKA=/opt/automq/kafka/bin
# The path INSIDE the container. The host's /etc/automq/server.properties is
# bind-mounted to this location, and kafka-storage.sh runs in the container —
# handing it the host path fails with NoSuchFileException on a file that very
# much exists, one namespace away.
CONTAINER_CONFIG=/opt/automq/kafka/config/kraft/server.properties
STORE="/usr/local/bin/automq-store"

. /etc/automq/secrets/secrets.env

if [ -f "$META" ]; then
  # Identity is checked on every converge, not only when formatting: metadata
  # that exists but belongs to another cluster would otherwise be started
  # against this one's storage.
  have_cluster=$(sed -n 's/^cluster\.id=//p' "$META" | tr -d '\r')
  have_node=$(sed -n 's/^node\.id=//p' "$META" | tr -d '\r')
  # Absence is not agreement. A truncated or malformed meta.properties has no
  # cluster.id to disagree with, and treating that as "consistent" starts a
  # broker against storage whose identity is unknown.
  if [ -z "$have_cluster" ] || [ -z "$have_node" ]; then
    echo "FATAL: $META is missing cluster.id or node.id." >&2
    echo "The metadata directory exists but its identity cannot be read, so it" >&2
    echo "cannot be shown to belong to this cluster. Investigate before starting;" >&2
    echo "do not reformat until you know whether this disk holds real metadata." >&2
    exit 1
  fi
  if [ "$have_cluster" != "<{ automq-cluster-id }>" ]; then
    echo "FATAL: $META belongs to cluster $have_cluster, not <{ automq-cluster-id }>." >&2
    echo "Refusing to start. This disk is another cluster's; do not reformat it" >&2
    echo "unless you know that cluster is gone." >&2
    exit 1
  fi
  if [ "$have_node" != "$NODE_ID" ]; then
    echo "FATAL: $META claims node.id=$have_node but this host is node $NODE_ID." >&2
    exit 1
  fi
  echo "format: metadata present and consistent"
  exit 0
fi

phase=$("$STORE" format-status --node "$NODE_ID" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("phase","none"))')

if [ "$phase" = "complete" ] && [ "${AUTOMQ_ALLOW_REFORMAT:-false}" != "true" ]; then
  cat >&2 <<EOF
FATAL: node $NODE_ID completed a format before, but its metadata storage is
empty. That is disk loss, not first provisioning, and reformatting silently is
exactly what Kafka refuses to do for you: it would rejoin the quorum as an
empty voter and could truncate the log the surviving nodes are serving.

Recover deliberately, then re-run:
  - if the disk is genuinely lost, this node must be rebuilt as a replacement.
    Confirm the remaining nodes hold a majority, then re-run this converge with
    AUTOMQ_ALLOW_REFORMAT=true to authorize one reformat of node $NODE_ID.
  - if the disk was merely unmounted, mount it and re-run; nothing is wrong.
EOF
  exit 1
fi

"$STORE" format-record --node "$NODE_ID" --phase intent >/dev/null

args=(--cluster-id "<{ automq-cluster-id }>" --config "$CONTAINER_CONFIG")

if [ "$GENESIS" = "true" ]; then
  # Genesis. One salt per principal, computed once on node 0 and shared, so
  # all voters write byte-identical bootstrap records.
  it="$AUTOMQ_SCRAM_ITERATIONS"
  args+=(--add-scram "SCRAM-SHA-512=[name=<{ admin-user }>,salt=${AUTOMQ_ADMIN_SALT},saltedpassword=${AUTOMQ_ADMIN_SALTED},iterations=${it}]")
  args+=(--add-scram "SCRAM-SHA-512=[name=<{ broker-user }>,salt=${AUTOMQ_BROKER_SALT},saltedpassword=${AUTOMQ_BROKER_SALTED},iterations=${it}]")
  args+=(--add-scram "SCRAM-SHA-512=[name=<{ client-user }>,salt=${AUTOMQ_CLIENT_SALT},saltedpassword=${AUTOMQ_CLIENT_SALTED},iterations=${it}]")
  echo "format: genesis, with bootstrap credentials"
else
  echo "format: replacement node, credentials come from the quorum"
fi

docker run --rm \
  -v /etc/automq/server.properties:"$CONTAINER_CONFIG":ro \
  -v /var/lib/automq:/var/lib/automq \
  --entrypoint "$KAFKA/kafka-storage.sh" \
  "<{ automq-image }>" format "${args[@]}"

"$STORE" format-record --node "$NODE_ID" --phase complete >/dev/null
echo "format: complete"
