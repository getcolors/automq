#!/usr/bin/env bash
# Generate this cluster's secrets, once, on node 0.
#
# Create-once is not tidiness. Every converge renders server.properties from
# these values, and the SCRAM salt is bootstrapped into the metadata log at
# genesis: regenerating either would rewrite configuration on every run and
# leave the stored credential disagreeing with the one the brokers were formatted
# with. Randomness inside a converge destroys determinism.
#
# The salted password is computed here, once, for the same reason. Kafka's
# `--add-scram` will happily take a plaintext password and derive a *random*
# salt on each node it runs on — which for a three-voter genesis means three
# divergent bootstrap records for one user. Passing the explicit
# salt/saltedpassword form is what makes all three identical.
set -euo pipefail

SECRETS=/etc/automq/secrets/secrets.env
ITERATIONS=8192

install -d -m 0700 /etc/automq/secrets

if [ -f "$SECRETS" ]; then
  echo "secrets: already present, leaving untouched"
  exit 0
fi

umask 077
tmp=$(mktemp /etc/automq/secrets/.secrets.XXXXXX)
trap 'rm -f "$tmp"' EXIT

python3 - "$tmp" "$ITERATIONS" <<'PY'
import base64, hashlib, secrets, sys

out_path, iterations = sys.argv[1], int(sys.argv[2])

def password():
    # URL-safe and shell-safe: these values are substituted into a JAAS line
    # and passed through client property files, where a quote or a backslash
    # would be a parsing bug rather than extra entropy.
    return secrets.token_urlsafe(32)

def scram(pw):
    salt = secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha512", pw.encode(), salt, iterations)
    return base64.b64encode(salt).decode(), base64.b64encode(salted).decode()

lines = ["# Generated once by secrets.sh. Never edit; never copy off this host.",
         f"AUTOMQ_SCRAM_ITERATIONS={iterations}"]

for role in ("CLIENT", "ADMIN", "BROKER"):
    pw = password()
    salt, salted = scram(pw)
    lines += [f"AUTOMQ_{role}_PASSWORD={pw}",
              f"AUTOMQ_{role}_SALT={salt}",
              f"AUTOMQ_{role}_SALTED={salted}"]

# The controller principal authenticates with PLAIN from a static JAAS entry,
# so it has a password and deliberately no SCRAM material: the controller
# quorum must not depend on the metadata log it is trying to form.
lines.append(f"AUTOMQ_CONTROLLER_PASSWORD={password()}")
# The PKCS#12 store the EXTERNAL listener reads.
lines.append(f"AUTOMQ_KEYSTORE_PASSWORD={password()}")

with open(out_path, "w") as fh:
    fh.write("\n".join(lines) + "\n")
PY

chmod 0600 "$tmp"
mv "$tmp" "$SECRETS"
trap - EXIT
echo "secrets: generated"
