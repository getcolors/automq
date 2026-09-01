#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
launcher="$root/skills/package-automq-green/green"
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
checks=0
fail(){ echo "launcher: FAIL — $*" >&2; exit 1; }
ok(){ checks=$((checks+1)); echo "  ok — $*"; }

[ -f "$launcher" ] || fail 'payload launcher is missing'
grep -q 'io.github.getcolors.automq.workflow/workflow' "$launcher" || fail 'workflow dispatch is missing'
for bad in 'defn.*-step' 'tofu/' 'ansible/'; do
  ! grep -qE "$bad" "$launcher" || fail "launcher contains package logic: $bad"
done
ok 'dispatches to the library and contains no lifecycle logic'

grep -qE '\(def \^:private automq-sha (nil|"[0-9a-f]{40}")\)' "$launcher" || fail 'invalid pin site'
ok 'has one managed immutable pin site'

mkdir "$tmp/bare"
cp "$launcher" "$tmp/bare/green"; chmod +x "$tmp/bare/green"
if grep -q '(def \^:private automq-sha nil)' "$launcher"; then
  out=$(cd "$tmp/bare" && ./green build 2>&1 || true)
  grep -q AUTOMQ_LIB_ROOT <<<"$out" || fail 'an unpinned launcher did not explain AUTOMQ_LIB_ROOT'
  ok 'unstamped payload fails with an actionable working-tree override'
else
  ok 'payload carries a real package commit pin'
fi

mkdir "$tmp/project"
cp "$launcher" "$tmp/project/green"; chmod +x "$tmp/project/green"
cp "$root/test/fixtures/colors.yml" "$tmp/project/colors.yml"
(cd "$tmp/project" && AUTOMQ_LIB_ROOT="$root" ./green build >/dev/null) || fail 'AUTOMQ_LIB_ROOT build failed'
[ -f "$tmp/project/.colors/automq-fixture/automq-infrastructure/main.tf" ] || fail 'copied payload rendered nothing'
grep -q 'vultr_vpc2' "$tmp/project/.colors/automq-fixture/automq-infrastructure/main.tf" || fail 'rendered compute has no VPC'
ok 'working-tree override renders from a copied payload'
mkdir -p "$tmp/project/deep/path"
(cd "$tmp/project/deep/path" && AUTOMQ_LIB_ROOT="$root" ../../green build >/dev/null) || fail 'upward desired-state search failed'
ok 'finds colors.yml by walking upward'

out=$(cd "$tmp/project" && AUTOMQ_LIB_ROOT="$root" ./green nonsense 2>&1 || true)
grep -q Usage <<<"$out" || fail 'unknown command has no usage'
for verb in build create delete validate; do
  grep -q "\"$verb\"" "$launcher" || fail "missing command $verb"
done
ok 'lifecycle commands are dispatchable'

# `delete` must not be reachable without the desired-state guard being lifted
# deliberately, and the launcher must not be where that decision is made.
! grep -qi 'prevent.destroy' "$launcher" || fail 'launcher reasons about the destroy guard'
ok 'the destroy guard is not decided in the copied payload'

[ -L "$root/green" ] && [ "$(readlink "$root/green")" = skills/package-automq-green/green ] || fail 'root green is not the payload symlink'
ok 'root launcher is the payload symlink'
echo "launcher: $checks checks passed"
