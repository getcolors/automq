#!/usr/bin/env bash
# Apply the client principal's ACLs. Idempotent: kafka-acls.sh --add is a
# no-op for a binding that already exists, so this converges rather than
# accumulating.
#
# The grants are deliberately narrow and deliberately enumerated. 9092 faces
# the internet, and a public endpoint whose only authenticated identity is a
# superuser is an authorization hole with a password on it.
set -euo pipefail

KAFKA=/opt/automq/kafka/bin
BOOTSTRAP="$1"

run() {
  docker exec automq "$KAFKA/kafka-acls.sh" \
    --bootstrap-server "$BOOTSTRAP" \
    --command-config /etc/automq/admin.properties "$@"
}

# Produce and consume on the deployment's own topic namespace, and nothing
# else. No Create: topics are desired state, not something a client invents.
# No Alter, no ClusterAction, no TransactionalId.
run --add --allow-principal "User:<{ client-user }>" \
    --operation Describe --operation Read --operation Write \
    --topic "<{ topic-prefix }>" --resource-pattern-type prefixed >/dev/null

run --add --allow-principal "User:<{ client-user }>" \
    --operation Describe --operation Read \
    --group "<{ topic-prefix }>" --resource-pattern-type prefixed >/dev/null

echo "acl: applied"
