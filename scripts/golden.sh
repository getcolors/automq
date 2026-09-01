#!/usr/bin/env bash
set -euo pipefail

# Green's regression net against the committed goldens: render every fixture and
# diff against committed output. scripts/parity.sh is the net across colours.
#
# Two fixtures, because the SSH Keypair Standard has two modes and a package
# conforms only if both hold. `colors.yml` is keygen mode (no vultr-ssh-keys):
# the compute template must declare the profile-named vultr_ssh_key resource
# and reference it by attribute. `optout.yml` supplies an explicit key id and
# must create nothing.
#
# Keygen paths are rendered from a fixed placeholder home on :build, never from
# $HOME, so these goldens mean the same thing on every workstation.
#
#   ./scripts/golden.sh            check
#   ./scripts/golden.sh --accept   regenerate after an intended change

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

accept=0
[[ ${1:-} == --accept ]] && accept=1

status=0
for variant in colors optout; do
  fixture="$tmp/$variant.yml"
  sed "s#WORKDIR#$tmp/work#" "$root/test/fixtures/$variant.yml" > "$fixture"
  sed -i "s#^workdir: .colors#workdir: $tmp/work#" "$fixture"
  (cd "$root/green" && AUTOMQ_LIB_ROOT="$root" ./green build -f "$fixture" >/dev/null)

  profile=$(sed -n 's/^profile: //p' "$fixture")
  actual="$tmp/work/$profile"
  golden="$root/test/resources/golden/local/$profile"

  # No rendered artefact may carry a real secret into a committed golden.
  # Checked before --accept copies anything. POSIX grep on purpose: a missing
  # binary inside `if` is simply false, so the guard must not depend on one
  # that may be absent.
  if grep -rEq 'BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY|github_pat_|ghp_|gho_|ghu_|ghs_|ghr_' "$actual"; then
    echo "golden: a credential-shaped value was rendered in $profile" >&2; exit 1
  fi
  # Every operator secret must reach the host as an Ansible lookup resolved at
  # execution time, never as a value templated into generated output. If one of
  # these expressions stops appearing, something started rendering the secret
  # itself and the next `bb golden:accept` would commit it.
  for par in AUTOMQ_R2_ACCESS_KEY_ID AUTOMQ_R2_SECRET_ACCESS_KEY CLOUDFLARE_API_TOKEN; do
    if ! grep -q "lookup('env', 'COLORS_PAR_$par')" "$actual/automq-ansible/main.yml"; then
      echo "golden: $profile no longer renders COLORS_PAR_$par as a lookup" >&2; exit 1
    fi
  done
  # The broker configuration must carry placeholders, not passwords: the
  # substitution happens on the host, at 0600, and nothing generated here may
  # ever hold the real value.
  for marker in @CONTROLLER_PASSWORD@ @BROKER_PASSWORD@ @KEYSTORE_PASSWORD@; do
    if ! grep -q "$marker" "$actual/automq-ansible/server.properties"; then
      echo "golden: $profile no longer renders the $marker placeholder" >&2; exit 1
    fi
  done

  # An absent Selmer key renders as an empty string rather than failing, so a
  # template that names a value nothing supplies produces `port = ""` — valid
  # HCL, accepted by build, golden, dry-run and validate alike, and rejected
  # only by the provider on a real apply. Catch the whole class here.
  if grep -rEn '=[[:space:]]*""$' "$actual"/*/*.tf; then
    echo "golden: $profile rendered an empty value into a tofu template" >&2; exit 1
  fi

  # A Selmer tag that survived rendering is a typo or an unsupplied key, and
  # an ellipsis inside a Jinja expression is documentation that will be parsed
  # as code by whichever engine owns the delimiter. Both cost a converge.
  if grep -rn '<{' "$actual"; then
    echo "golden: $profile left an unrendered Selmer tag" >&2; exit 1
  fi
  if grep -rn '{{[^}]*…' "$actual"; then
    echo "golden: $profile renders an ellipsis inside a Jinja expression" >&2; exit 1
  fi

  # Every rendered shell script must actually parse. Template substitution can
  # produce syntactically broken shell from a perfectly readable template — an
  # empty value where a word was expected is the usual way — and the first
  # place that would otherwise surface is a converge against real machines.
  for script in "$actual"/*/*.sh; do
    [ -e "$script" ] || continue
    bash -n "$script" || { echo "golden: $profile rendered unparseable shell in $script" >&2; exit 1; }
  done
  for py in "$actual"/*/*.py; do
    [ -e "$py" ] || continue
    python3 -c "import ast,sys; ast.parse(open(sys.argv[1]).read())" "$py" \
      || { echo "golden: $profile rendered unparseable python in $py" >&2; exit 1; }
  done

  # A build that reached the real ~/.ssh would leak the operator's home into
  # committed bytes and make the goldens workstation-specific.
  if grep -rq "$HOME/.ssh" "$actual"; then
    echo "golden: $profile rendered a real home directory; build must use the placeholder" >&2; exit 1
  fi
  # SSH Config Standard §6: the local stage takes addresses and the alias as
  # Ansible extra-vars, never through Selmer, so its rendered playbook carries
  # no address at all. A dotted quad here means someone templated a run-time
  # fact and the goldens stopped being workstation-independent.
  if grep -rEq '([0-9]{1,3}\.){3}[0-9]{1,3}' "$actual/automq-ansible-local"; then
    echo "golden: $profile rendered an address into the local ssh_config stage" >&2; exit 1
  fi

  if [[ $accept == 1 ]]; then
    rm -rf "$golden"; mkdir -p "$(dirname "$golden")"; cp -a "$actual" "$golden"; continue
  fi
  [[ -d "$golden" ]] || { echo "golden missing for $profile; inspect build then run bb golden:accept" >&2; exit 1; }
  diff -ru "$golden" "$actual" || status=1
done

exit "$status"
