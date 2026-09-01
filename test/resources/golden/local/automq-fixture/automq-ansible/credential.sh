#!/usr/bin/env bash
# Print the client SASL password. Root only, and separate from `automq-status`
# on purpose: routine health output must not carry a credential.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "automq-credential must run as root" >&2; exit 1; }
. /etc/automq/secrets/secrets.env
cat <<EOF
bootstrap: automq.example.com:9092
principal: automq
password:  ${AUTOMQ_CLIENT_PASSWORD}
mechanism: SCRAM-SHA-512 over SASL_SSL
EOF
