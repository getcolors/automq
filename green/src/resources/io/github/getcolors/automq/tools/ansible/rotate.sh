#!/usr/bin/env bash
# Rotate the client SASL password.
#
# Read this before running it: a SCRAM upsert for the same principal and
# mechanism REPLACES the credential immediately. There is no "prove the new one
# first, then retire the old one" for a single principal — Kafka does not offer
# it, and a script that claimed to do it would be lying. So this is an atomic,
# disruptive replace: every client still holding the old password is
# disconnected and will fail to reconnect until it is updated.
#
# For a zero-downtime rotation, do it as a migration instead: create a second
# principal (automq-2) with the same ACLs, move clients to it, then delete the
# first. That is an operator procedure, not something to automate for a
# deployment with one client principal.
set -euo pipefail

BOOTSTRAP="${1:-<{ bootstrap-internal }>}"
KAFKA=/opt/automq/kafka/bin
ADMIN=/etc/automq/admin.properties
SECRETS=/etc/automq/secrets/secrets.env

[ "$(id -u)" -eq 0 ] || { echo "automq-rotate must run as root" >&2; exit 1; }
. "$SECRETS"

new=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')

docker exec automq "$KAFKA/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --command-config "$ADMIN" --alter \
  --add-config "SCRAM-SHA-512=[iterations=${AUTOMQ_SCRAM_ITERATIONS},password=${new}]" \
  --entity-type users --entity-name "<{ client-user }>" >/dev/null

# Verify by authenticating. This is also how the converge detects drift: the
# stored salt cannot be compared against a plaintext, so the only real test of
# a SCRAM credential is whether it logs in.
probe=$(mktemp); trap 'rm -f "$probe"' EXIT
cat > "$probe" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="<{ client-user }>" password="${new}";
EOF
docker cp "$probe" automq:/tmp/rotate-probe.properties >/dev/null
if ! docker exec automq "$KAFKA/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
     --command-config /tmp/rotate-probe.properties --list >/dev/null 2>&1; then
  echo "FATAL: the new credential does not authenticate. The old one has already" >&2
  echo "been replaced; recover with automq-rotate again or re-run the converge." >&2
  exit 1
fi
docker exec automq rm -f /tmp/rotate-probe.properties || true

umask 077
tmp=$(mktemp /etc/automq/secrets/.secrets.XXXXXX)
sed "s|^AUTOMQ_CLIENT_PASSWORD=.*|AUTOMQ_CLIENT_PASSWORD=${new}|" "$SECRETS" > "$tmp"
chmod 0600 "$tmp"; mv "$tmp" "$SECRETS"
/usr/local/bin/automq-render-config >/dev/null

echo "rotated. Existing clients are disconnected until they use the new password:"
echo "  automq-credential"
