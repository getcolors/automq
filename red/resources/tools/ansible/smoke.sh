#!/usr/bin/env bash
# The on-host gates. Run once, from node 0, before the ready marker is written.
#
# Exit codes are not evidence. Every gate below asks the cluster what it has —
# what the quorum reports, what comes back out of a topic, what objects exist in
# R2, what an unauthorized principal is refused — because a broker that started
# and does nothing useful exits zero all day.
set -euo pipefail

BOOTSTRAP="${1:?internal bootstrap required}"
KAFKA=/opt/automq/kafka/bin
ADMIN=/etc/automq/admin.properties
CLIENT=/etc/automq/client.properties
TOPIC="<{ topic-prefix }>acceptance"
EXPECT_NODES=<{ node-count }>
STORE=/usr/local/bin/automq-store
pass=0

k() { docker exec automq "$KAFKA/$@"; }
gate() { pass=$((pass+1)); echo "  ok   — $*"; }
fail() { echo "  FAIL — $*" >&2; exit 1; }

echo "smoke: gates"

# 1 — every broker registered, not merely every process started.
brokers=$(k kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" \
            --command-config "$ADMIN" 2>/dev/null \
          | grep -c 'id: ' || true)
[ "$brokers" -eq "$EXPECT_NODES" ] || fail "expected $EXPECT_NODES brokers, metadata reports $brokers"
gate "$brokers brokers registered"

# 2 — a quorum with a leader. Voters listed while dead would still be listed,
# so the leader is the part that matters.
quorum=$(k kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" \
           --command-config "$ADMIN" describe --status 2>/dev/null)
leader=$(sed -n 's/.*LeaderId:[[:space:]]*\([0-9]\+\).*/\1/p' <<<"$quorum" | head -1)
voters=$(grep -o 'CurrentVoters:.*' <<<"$quorum" | grep -o '"id"' | wc -l || true)
[ -n "$leader" ] || fail "the controller quorum reports no leader"
[ "${voters:-0}" -eq "$EXPECT_NODES" ] || fail "expected $EXPECT_NODES voters, quorum reports ${voters:-0}"
gate "controller quorum: $voters voters, leader $leader"

# 3 — a round trip through object storage. RF=1 is deliberate: durability is R2.
#
# The topic is recreated, not reused. These gates run on every converge, and a
# topic that survives from the last one still holds its records — so producing
# 500 and consuming 500 reads the OLD 500 and the exact-match assertion fails
# on a cluster that is working perfectly. A gate that only passes the first
# time is not a gate. Deletion is asynchronous, so wait for the name to leave
# the topic list before recreating it.
if k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" \
     --list 2>/dev/null | grep -qx "$TOPIC"; then
  k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" \
    --delete --topic "$TOPIC" >/dev/null 2>&1 || true
  for _ in $(seq 1 60); do
    k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" \
      --list 2>/dev/null | grep -qx "$TOPIC" || break
    sleep 2
  done
fi
k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" \
  --create --if-not-exists --topic "$TOPIC" \
  --partitions <{ automq-topic-partitions }> --replication-factor 1 >/dev/null
sent=$(mktemp); got=$(mktemp)
trap 'rm -f "$sent" "$got"' EXIT
seq 1 500 | sed 's/^/smoke-/' > "$sent"
docker exec -i automq "$KAFKA/kafka-console-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --producer.config "$ADMIN" \
  --topic "$TOPIC" < "$sent" >/dev/null 2>&1
k kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
  --consumer.config "$ADMIN" --topic "$TOPIC" --from-beginning \
  --max-messages 500 --timeout-ms 120000 2>/dev/null | sort > "$got"
diff <(sort "$sent") "$got" >/dev/null || fail "produced 500 records, consumed a different set"
gate "500 records produced and consumed back exactly"

# 4 and 5 — the storage tier is really R2. Without this the cluster could be
# writing to local disk and every gate above would still pass.
for role in data ops; do
  bucket=$([ "$role" = data ] && echo "<{ automq-data-r2-bucket }>" || echo "<{ automq-ops-r2-bucket }>")
  n=$(python3 - "$bucket" <<'PY'
import os, sys, boto3
from botocore.config import Config
s3 = boto3.client("s3", endpoint_url="<{ automq-r2-endpoint }>", region_name="<{ automq-r2-region }>",
                  aws_access_key_id=os.environ["AUTOMQ_R2_ACCESS_KEY_ID"],
                  aws_secret_access_key=os.environ["AUTOMQ_R2_SECRET_ACCESS_KEY"],
                  config=Config(s3={"addressing_style": "path"}))
seen = 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=sys.argv[1]):
    for o in page.get("Contents", []):
        if not o["Key"].startswith("_colors/"):
            seen += 1
print(seen)
PY
)
  [ "${n:-0}" -gt 0 ] || fail "$bucket holds no AutoMQ objects: the storage tier is not R2"
  gate "$bucket holds $n AutoMQ objects"
done

# 6 — authentication and authorization actually refuse things. A cluster that
# accepts everything passes every other gate in this file.
bad=$(mktemp); trap 'rm -f "$sent" "$got" "$bad"' EXIT
cat > "$bad" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="<{ admin-user }>" password="definitely-not-the-password";
EOF
docker cp "$bad" automq:/tmp/bad.properties >/dev/null
if k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config /tmp/bad.properties \
     --list >/dev/null 2>&1; then
  fail "a wrong password was accepted"
fi
gate "a wrong password is refused"

if k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
  fail "an unauthenticated connection was accepted"
fi
gate "an unauthenticated connection is refused"

# The client principal is ACL-scoped, not a superuser: it may use its own topic
# namespace and must not be able to administer the cluster.
if k kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT" \
     --entity-type brokers --describe --all >/dev/null 2>&1; then
  fail "the client principal was allowed a cluster-level operation"
fi
gate "the client principal is denied a cluster-level operation"

if k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CLIENT" \
     --create --topic "outside-prefix-$$" --partitions 1 --replication-factor 1 >/dev/null 2>&1; then
  fail "the client principal created a topic outside its prefix"
fi
gate "the client principal cannot create a topic outside <{ topic-prefix }>"

echo "smoke: $pass gates passed"
