#!/usr/bin/env bash
# Substitute the secrets into the broker configuration and the admin client
# properties, on the host, at 0600.
#
# The templates carry @PLACEHOLDER@ markers rather than values so that no
# password is ever written to .colors/, to a committed golden, or to an Ansible
# variable. tmpfile+rename so a broker starting concurrently never reads a
# half-written file.
set -euo pipefail

. /etc/automq/secrets/secrets.env

src=/etc/automq/server.properties.in
dst=/etc/automq/server.properties
umask 077

# Reporting "changed" on every run makes the converge's own idempotency claim
# unfalsifiable. Compare what we are about to write with what is there.
before=$(sha256sum "$dst" 2>/dev/null | cut -d" " -f1)

render() {
  sed -e "s|@CONTROLLER_PASSWORD@|${AUTOMQ_CONTROLLER_PASSWORD}|g" \
      -e "s|@BROKER_PASSWORD@|${AUTOMQ_BROKER_PASSWORD}|g" \
      -e "s|@KEYSTORE_PASSWORD@|${AUTOMQ_KEYSTORE_PASSWORD}|g" \
      "$1"
}

tmp=$(mktemp /etc/automq/.server.XXXXXX)
render "$src" > "$tmp"
chmod 0600 "$tmp"
mv "$tmp" "$dst"

# The administrative client path: the INTERNAL listener, as the admin
# principal. On-host tooling authenticates like everything else — there is no
# anonymous shortcut anywhere in this cluster.
tmp=$(mktemp /etc/automq/.admin.XXXXXX)
cat > "$tmp" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="<{ admin-user }>" password="${AUTOMQ_ADMIN_PASSWORD}";
EOF
chmod 0600 "$tmp"
mv "$tmp" /etc/automq/admin.properties

# The client principal's own properties, used by the smoke gates to prove that
# an ACL-scoped identity can do what it should and cannot do what it should not.
tmp=$(mktemp /etc/automq/.client.XXXXXX)
cat > "$tmp" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="<{ client-user }>" password="${AUTOMQ_CLIENT_PASSWORD}";
EOF
chmod 0600 "$tmp"
mv "$tmp" /etc/automq/client.properties

after=$(sha256sum "$dst" 2>/dev/null | cut -d" " -f1)
if [ "$before" != "$after" ]; then
  echo "config: changed"
else
  echo "config: unchanged"
fi
