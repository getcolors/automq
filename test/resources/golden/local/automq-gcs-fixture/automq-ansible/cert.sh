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
#
# lego 5.x moved --accept-tos, --email, --domains and --path out of the global
# options and under the subcommand; `lego --accept-tos run …` fails with
# "flag provided but not defined: -accept-tos". The invocation below matches
# the one the agent-network package already runs in production against the
# same zone: subcommand first, LEGO_PATH from the environment, short flags.
set -euo pipefail

NAMES="${1:?comma-separated certificate names required}"
STORE=/usr/local/bin/automq-store
export LEGO_PATH=/etc/automq/lego

install -d -m 0700 "$LEGO_PATH"
umask 077

IFS=',' read -ra parts <<< "$NAMES"
domains=()
for d in "${parts[@]}"; do domains+=(-d "$d"); done

# A private CA is an explicit alternative for deployments without a DNS zone.
# The CA private key stays on the issuer; only its public certificate is exported.
if [ 'private-ca' = private-ca ]; then
  ca=/etc/automq/ca
  install -d -m 0700 "$ca"
  if [ ! -s "$ca/ca.key" ] || [ ! -s "$ca/ca.crt" ]; then
    published=$("$STORE" tls-fingerprint)
    [ -z "$published" ] || { echo 'cert: restore the existing issuer CA before renewing' >&2; exit 1; }
    openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 3650 \
      -keyout "$ca/ca.key" -out "$ca/ca.crt" -subj '/CN=AutoMQ automq-gcs-fixture CA' \
      -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign' >/dev/null 2>&1
  fi
  crt="$ca/fullchain.pem"
  key="$ca/server.key"
  if [ ! -s "$crt" ] || ! openssl x509 -noout -checkend 2592000 -in "$crt" >/dev/null 2>&1 \
      || [ "$(cat "$ca/names" 2>/dev/null || true)" != "$NAMES" ]; then
    san=''
    for address in "${parts[@]}"; do san="${san:+$san,}IP:$address"; done
    printf 'subjectAltName=%s\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n' "$san" > "$ca/server.ext"
    openssl req -new -newkey rsa:3072 -nodes -sha256 -keyout "$key" -out "$ca/server.csr" \
      -subj '/CN=AutoMQ automq-gcs-fixture' >/dev/null 2>&1
    openssl x509 -req -in "$ca/server.csr" -CA "$ca/ca.crt" -CAkey "$ca/ca.key" \
      -CAcreateserial -out "$ca/server.crt" -days 90 -sha256 -extfile "$ca/server.ext" >/dev/null 2>&1
    cat "$ca/server.crt" "$ca/ca.crt" > "$crt"
    printf '%s' "$NAMES" > "$ca/names"
  fi
else
# --dns.propagation.disable-rns: lego otherwise polls the zone's authoritative
# nameservers itself and refuses to proceed until every one agrees, which is a
# check Cloudflare's anycast estate does not satisfy the way lego expects.
common=(--dns cloudflare --dns.resolvers 1.1.1.1:53 --dns.propagation.disable-rns)

first="${parts[0]}"
crt="$LEGO_PATH/certificates/${first}.crt"
key="$LEGO_PATH/certificates/${first}.key"

if [[ ! -s $crt ]]; then
  /usr/local/bin/lego run -a -m "claude@ululi.it" \
    "${domains[@]}" "${common[@]}" >&2
elif ! openssl x509 -noout -checkend 2592000 -in "$crt" >/dev/null 2>&1; then
  # Thirty days out. Renewing earlier burns rate limit; renewing later leaves
  # no room for a failed attempt to be noticed and fixed.
  /usr/local/bin/lego renew -m "claude@ululi.it" \
    "${domains[@]}" "${common[@]}" >&2
else
  echo "cert: current certificate is valid for more than 30 days"
fi

fi

[ -s "$crt" ] && [ -s "$key" ] || { echo "cert: no certificate material at $crt" >&2; exit 1; }

# Publish when the published bundle is missing or stale — but not on every
# converge, or the play can never report an unchanged run.
local_fp=$(sha256sum "$crt" | cut -d" " -f1)
published_fp=$("$STORE" tls-fingerprint 2>/dev/null || true)
if [ "$local_fp" != "$published_fp" ]; then
  fp=$("$STORE" tls-publish --cert "$crt" --keyfile "$key" --names "$NAMES")
  echo "cert: published $fp"
else
  echo "cert: already published $published_fp"
fi
