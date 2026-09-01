#!/usr/bin/env bash
# Reconcile the SCRAM credentials against desired state.
#
# A SCRAM credential cannot be compared: `kafka-configs --describe` exposes the
# iteration count and the salt, never anything derivable back to a plaintext.
# The only test of "is the stored credential the one we want?" is to try to log
# in with the one we want.
#
# Genesis bootstrap covers the first converge. This covers everything after it:
# a rotated password, a principal renamed in desired state, or a credential
# that was never written because an earlier run formatted without bootstrap
# records.
#
# One honest limitation, stated rather than papered over: repairs go through
# the admin principal, so if ADMIN itself cannot authenticate there is no
# authenticated path left and no amount of converging fixes it. That case is
# reported loudly, because the recovery is a reformat and an operator must
# decide to do it.
set -uo pipefail

BOOTSTRAP="${1:?internal bootstrap required}"
KAFKA=/opt/automq/kafka/bin
ADMIN=/etc/automq/admin.properties
. /etc/automq/secrets/secrets.env

probe() {
  # $1 principal, $2 password. Returns 0 when the credential authenticates.
  local user="$1" pass="$2" f
  f=$(mktemp /etc/automq/.probe.XXXXXX)
  chmod 0600 "$f"
  cat > "$f" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${user}" password="${pass}";
EOF
  docker cp "$f" automq:/tmp/probe.properties >/dev/null 2>&1
  local rc=1
  if docker exec automq "$KAFKA/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
       --command-config /tmp/probe.properties --list >/dev/null 2>&1; then rc=0; fi
  docker exec automq rm -f /tmp/probe.properties >/dev/null 2>&1
  rm -f "$f"
  return $rc
}

upsert() {
  local user="$1" pass="$2"
  docker exec automq "$KAFKA/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
    --command-config "$ADMIN" --alter \
    --add-config "SCRAM-SHA-512=[iterations=${AUTOMQ_SCRAM_ITERATIONS},password=${pass}]" \
    --entity-type users --entity-name "$user" >/dev/null 2>&1
}

if ! probe "<{ admin-user }>" "$AUTOMQ_ADMIN_PASSWORD"; then
  cat >&2 <<EOF
FATAL: the admin principal <{ admin-user }> cannot authenticate.

Every repair path runs through it, so nothing here can fix this. Either the
stored credential differs from the generated one, or the metadata log holds no
SCRAM records at all — which happens when a cluster was formatted without
bootstrap records. Check:

  docker exec automq $KAFKA/kafka-dump-log.sh --cluster-metadata-decoder \\
    --files /var/lib/automq/metadata/bootstrap.checkpoint | grep -c USER_SCRAM_CREDENTIAL_RECORD

Zero means the cluster can never authenticate anyone and must be reformatted:
wipe /var/lib/automq/metadata on every node, delete the nodes/ records and the
genesis marker from the ops bucket, and converge again.
EOF
  exit 1
fi

changed=0
for pair in "<{ client-user }>:$AUTOMQ_CLIENT_PASSWORD" "<{ broker-user }>:$AUTOMQ_BROKER_PASSWORD"; do
  user="${pair%%:*}"; pass="${pair#*:}"
  if probe "$user" "$pass"; then
    echo "scram: $user authenticates"
  else
    echo "scram: $user does not authenticate, upserting"
    upsert "$user" "$pass"
    if probe "$user" "$pass"; then
      echo "scram: upserted $user"
      changed=1
    else
      echo "FATAL: $user still cannot authenticate after an upsert." >&2
      exit 1
    fi
  fi
done

[ "$changed" = 1 ] || echo "scram: no drift"
