#!/usr/bin/env bash
set -euo pipefail

# What every copied payload must do, checked on a copy rather than on the
# repository symlink — a launcher is installed into a deployment as a file, and
# the failures worth catching are the ones that only appear once it is detached
# from this checkout.
#
# Three payloads, one contract: dispatch to the tested library, carry exactly
# one managed pin site, resolve a working tree through <COLOUR>_LIB_ROOT, find
# colors.yml by walking upward, and hold no lifecycle logic of their own.

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
checks=0
fail(){ echo "launcher: FAIL — $*" >&2; exit 1; }
ok(){ checks=$((checks+1)); echo "  ok — $*"; }

green="$root/skills/package-automq-green/green"
red="$root/skills/package-automq-red/red"
blue="$root/skills/package-automq-blue/blue"

for payload in "$green" "$red" "$blue"; do
  [ -f "$payload" ] || fail "payload is missing: $payload"
  for bad in 'tofu/' 'ansible/'; do
    ! grep -qE "$bad" "$payload" || fail "$payload contains package logic: $bad"
  done
  # `delete` must not be reachable without the desired-state guard being lifted
  # deliberately, and no launcher may be where that decision is made.
  ! grep -qi 'prevent.destroy' "$payload" || fail "$payload reasons about the destroy guard"
done
ok 'every payload dispatches to its library and holds no lifecycle logic'

# ------------------------------------------------------------------- green

grep -q 'io.github.getcolors.automq.workflow/workflow' "$green" || fail 'green workflow dispatch is missing'
! grep -qE 'defn.*-step' "$green" || fail 'green launcher defines a step'
grep -qE '\(def \^:private automq-sha (nil|"[0-9a-f]{40}")\)' "$green" || fail 'invalid green pin site'
ok 'green has one managed immutable pin site'

mkdir "$tmp/bare"
cp "$green" "$tmp/bare/green"; chmod +x "$tmp/bare/green"
if grep -q '(def \^:private automq-sha nil)' "$green"; then
  out=$(cd "$tmp/bare" && ./green build 2>&1 || true)
  grep -q AUTOMQ_LIB_ROOT <<<"$out" || fail 'an unpinned green payload did not explain AUTOMQ_LIB_ROOT'
  ok 'unstamped green payload fails with an actionable working-tree override'
else
  ok 'green payload carries a real package commit pin'
fi

mkdir "$tmp/project"
cp "$green" "$tmp/project/green"; chmod +x "$tmp/project/green"
cp "$root/test/fixtures/colors.yml" "$tmp/project/colors.yml"
(cd "$tmp/project" && AUTOMQ_LIB_ROOT="$root" ./green build >/dev/null) || fail 'AUTOMQ_LIB_ROOT green build failed'
[ -f "$tmp/project/.colors/automq-fixture/automq-infrastructure/main.tf" ] || fail 'copied green payload rendered nothing'
grep -q 'resource "vultr_vpc" "cluster"' "$tmp/project/.colors/automq-fixture/automq-infrastructure/main.tf" || fail 'rendered compute has no VPC'
ok 'green working-tree override renders from a copied payload'
mkdir -p "$tmp/project/deep/path"
(cd "$tmp/project/deep/path" && AUTOMQ_LIB_ROOT="$root" ../../green build >/dev/null) || fail 'green upward desired-state search failed'
ok 'green finds colors.yml by walking upward'

out=$(cd "$tmp/project" && AUTOMQ_LIB_ROOT="$root" ./green nonsense 2>&1 || true)
grep -q Usage <<<"$out" || fail 'unknown green command has no usage'
for verb in build create delete validate; do
  grep -q "\"$verb\"" "$green" || fail "green is missing command $verb"
done
ok 'green lifecycle commands are dispatchable'

# --------------------------------------------------------------------- red

grep -q 'package-automq-red' "$red" || fail 'red does not resolve its library'
grep -qE '"package-automq-red": (null|"github:getcolors/automq#[0-9a-f]{40}"),' "$red" \
  || fail 'invalid red pin site'
ok 'red has one managed immutable pin site'

mkdir "$tmp/red-project"
cp "$red" "$tmp/red-project/red"; chmod +x "$tmp/red-project/red"
cp "$root/test/fixtures/colors.yml" "$tmp/red-project/colors.yml"
(cd "$tmp/red-project" && AUTOMQ_LIB_ROOT="$root/red" ./red build >/dev/null) || fail 'AUTOMQ_LIB_ROOT red build failed'
grep -q 'resource "vultr_vpc" "cluster"' "$tmp/red-project/.colors/automq-fixture/automq-infrastructure/main.tf" || fail 'red rendered compute has no VPC'
ok 'red working-tree override renders from a copied payload'
mkdir -p "$tmp/red-project/deep/path"
(cd "$tmp/red-project/deep/path" && AUTOMQ_LIB_ROOT="$root/red" ../../red build >/dev/null) || fail 'red upward desired-state search failed'
ok 'red finds colors.yml by walking upward'

if grep -q '"package-automq-red": null,' "$red"; then
  out=$(cd "$tmp/red-project" && RED_NO_BOOTSTRAP=1 ./red build 2>&1 || true)
  grep -qE 'AUTOMQ_LIB_ROOT|cannot resolve' <<<"$out" \
    || fail 'an unpinned red payload did not explain AUTOMQ_LIB_ROOT'
  ok 'unstamped red payload fails with an actionable working-tree override'
else
  ok 'red payload carries a real package commit pin'
fi

# -------------------------------------------------------------------- blue

grep -q 'package_automq_blue' "$blue" || fail 'blue does not resolve its library'
grep -qE '^# (dependencies = \[\]|package-automq-blue = \{ git = "https://github.com/getcolors/automq.git", rev = "[0-9a-f]{40}", subdirectory = "blue" \})$' "$blue" \
  || fail 'invalid blue pin site'
ok 'blue has one managed immutable pin site'

mkdir "$tmp/blue-project"
cp "$blue" "$tmp/blue-project/blue"; chmod +x "$tmp/blue-project/blue"
cp "$root/test/fixtures/colors.yml" "$tmp/blue-project/colors.yml"
(cd "$tmp/blue-project" && AUTOMQ_LIB_ROOT="$root" ./blue build >/dev/null) || fail 'AUTOMQ_LIB_ROOT blue build failed'
grep -q 'resource "vultr_vpc" "cluster"' "$tmp/blue-project/.colors/automq-fixture/automq-infrastructure/main.tf" || fail 'blue rendered compute has no VPC'
ok 'blue working-tree override renders from a copied payload'
mkdir -p "$tmp/blue-project/deep/path"
(cd "$tmp/blue-project/deep/path" && AUTOMQ_LIB_ROOT="$root" ../../blue build >/dev/null) || fail 'blue upward desired-state search failed'
ok 'blue finds colors.yml by walking upward'

# --------------------------------------------------------------- repository

[ -L "$root/green/green" ] && [ "$(readlink "$root/green/green")" = ../skills/package-automq-green/green ] \
  || fail 'green/green is not the payload symlink'
[ -L "$root/red/red" ] && [ "$(readlink "$root/red/red")" = ../skills/package-automq-red/red ] \
  || fail 'red/red is not the payload symlink'
[ -L "$root/blue/blue" ] && [ "$(readlink "$root/blue/blue")" = ../skills/package-automq-blue/blue ] \
  || fail 'blue/blue is not the payload symlink'
ok 'each colour entry point is the payload symlink'

echo "launcher: $checks checks passed"
