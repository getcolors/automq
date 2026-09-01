#!/usr/bin/env bash
# Issue the cluster's certificate. Node 0 only.
#
# One issuer, for two reasons that reinforce each other. Three nodes renewing
# on their own timers would each write and then delete the shared
# _acme-challenge TXT record for the bootstrap name while a sibling was still
# being validated — a race that appears months after the converge that created
# it. And issuing everywhere would mean shipping a zone-editing Cloudflare
# token to every publicly reachable broker, so that compromising any one of
# them yields DNS control over the whole zone.
#
# The SAN list is enumerated, never wildcarded: the bootstrap name plus each
# broker's own name. A wildcard would still need the apex as a separate SAN and
# would cover names this cluster does not serve.
set -euo pipefail

LEGO=/usr/local/bin/lego
DATA=/etc/automq/lego
NAMES="$1"
STORE=/usr/local/bin/automq-store

install -d -m 0700 "$DATA"

domains=()
IFS=',' read -ra parts <<< "$NAMES"
for d in "${parts[@]}"; do domains+=(--domains "$d"); done

# CLOUDFLARE_DNS_API_TOKEN is exported by the caller and never written to disk.
if [ -d "$DATA/certificates" ] && compgen -G "$DATA/certificates/*.crt" >/dev/null; then
  set +e
  "$LEGO" --accept-tos --email "fixture@example.com" \
      --dns cloudflare --path "$DATA" "${domains[@]}" renew --days 30
  rc=$?
  set -e
  # lego exits non-zero when there is nothing to renew on some versions; the
  # certificate on disk is the evidence, not the exit code.
  if [ $rc -ne 0 ]; then echo "cert: renew returned $rc, checking material"; fi
else
  "$LEGO" --accept-tos --email "fixture@example.com" \
      --dns cloudflare --path "$DATA" "${domains[@]}" run
fi

first="${parts[0]}"
crt="$DATA/certificates/${first}.crt"
key="$DATA/certificates/${first}.key"
[ -s "$crt" ] && [ -s "$key" ] || { echo "cert: no certificate material at $crt" >&2; exit 1; }

fp=$("$STORE" tls-publish --cert "$crt" --keyfile "$key" --names "$NAMES")
echo "cert: published $fp"
