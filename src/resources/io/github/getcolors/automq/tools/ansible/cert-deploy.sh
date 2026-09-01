#!/usr/bin/env bash
# Pull the published certificate, rebuild the keystore, and restart this broker
# if it is already running — one node at a time, cluster-wide.
#
# The serialization is a lease held in object storage, not a local health check.
# Three nodes watching one published bundle observe it at the same moment, and
# each would pass its own quorum check before any peer had gone down: a local
# check cannot order independent actors. These are combined broker+controller
# nodes, so a simultaneous restart destroys the majority.
set -euo pipefail

NODE_ID="${1:?node id required}"
BOOTSTRAP="${2:?internal bootstrap required}"
TLS=/etc/automq/tls
STORE=/usr/local/bin/automq-store

. /etc/automq/secrets/secrets.env
install -d -m 0700 "$TLS"

published=$("$STORE" tls-fingerprint)
[ -n "$published" ] || { echo "cert-deploy: nothing published yet"; exit 0; }
current=$(cat "$TLS/fingerprint" 2>/dev/null || true)

if [ "$published" = "$current" ] && [ -s "$TLS/keystore.p12" ]; then
  echo "cert-deploy: up to date"
  exit 0
fi

"$STORE" tls-fetch --dir "$TLS" >/dev/null

# Build into a temporary file, verify it, and only then move it into place: a
# keystore that is half-written or unreadable takes the listener down at the
# next restart, which is precisely when nobody is watching.
tmp=$(mktemp "$TLS/.keystore.XXXXXX")
openssl pkcs12 -export \
  -in "$TLS/fullchain.pem" -inkey "$TLS/privkey.pem" \
  -out "$tmp" -name automq \
  -passout "pass:${AUTOMQ_KEYSTORE_PASSWORD}"
openssl pkcs12 -info -in "$tmp" -passin "pass:${AUTOMQ_KEYSTORE_PASSWORD}" -nokeys >/dev/null
chmod 0600 "$tmp"
mv "$tmp" "$TLS/keystore.p12"

if ! docker ps --format '{{.Names}}' | grep -qx automq; then
  # First converge: the broker has not started yet, so there is nothing to
  # restart and no quorum to protect.
  echo "$published" > "$TLS/fingerprint"
  echo "cert-deploy: keystore installed, broker not yet running"
  exit 0
fi

deadline=$(( $(date +%s) + 900 ))
while :; do
  got=$("$STORE" lease-acquire --holder "node-$NODE_ID" --ttl 600 \
        | python3 -c 'import json,sys; print(str(json.load(sys.stdin)["acquired"]).lower())')
  [ "$got" = "true" ] && break
  [ "$(date +%s)" -lt "$deadline" ] || { echo "cert-deploy: lease not acquired in time" >&2; exit 1; }
  sleep 15
done
trap '"$STORE" lease-release >/dev/null 2>&1 || true' EXIT

# Only restart into a healthy quorum: if the cluster is already one voter down,
# taking another is how a rolling restart becomes an outage.
voters=$(docker exec automq /opt/automq/kafka/bin/kafka-metadata-quorum.sh \
           --bootstrap-server "$BOOTSTRAP" \
           --command-config /etc/automq/admin.properties describe --status 2>/dev/null \
         | grep -c 'CurrentVoters\|LeaderId' || true)
if [ "${voters:-0}" -eq 0 ]; then
  echo "cert-deploy: quorum not answering, refusing to restart" >&2
  exit 1
fi

docker restart automq >/dev/null

for _ in $(seq 1 60); do
  if docker exec automq /opt/automq/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server "$BOOTSTRAP" \
       --command-config /etc/automq/admin.properties >/dev/null 2>&1; then
    echo "$published" > "$TLS/fingerprint"
    echo "cert-deploy: restarted on $published"
    exit 0
  fi
  sleep 10
done

echo "cert-deploy: broker did not come back after restart" >&2
exit 1
