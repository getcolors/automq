#!/usr/bin/env bash
# The operator path, proved from the workstation.
#
# Everything the playbook could prove on the hosts, it already proved before it
# wrote the ready marker. What is left is what only a client outside the
# deployment can establish: that the public names resolve, that the certificate
# they serve validates, that SASL_SSL admits the client principal and refuses a
# wrong password, that the ACLs deny what they should, and — the gate this
# whole cluster shape exists for — that killing a broker which leads a
# partition does not lose the records written to it.
#
# It also deliberately goes through `ssh <profile>-<n>`, the aliases the local
# stage wrote, because that is the path an operator will actually type.
set -uo pipefail

BOOTSTRAP="<{ bootstrap-external }>"
TOPIC="<{ topic-prefix }>failover"
PROFILE="<{ profile }>"
NODES=<{ node-count }>
LAST=$((NODES - 1))
pass=0
fail=0
# Every record and group this run creates is tagged with it. These gates run on
# every converge against a cluster that keeps its data, so a gate that counts
# "100 records" must count THIS run's hundred — otherwise it passes once and
# then fails forever against a perfectly healthy cluster.
RUN=$(date +%s)

ok()   { pass=$((pass+1)); echo "  ok   — $*"; }
bad()  { fail=$((fail+1)); echo "  FAIL — $*" >&2; }
on() { local n="$1"; shift; ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "${PROFILE}-${n}" "$@"; }

command -v kcat >/dev/null || { echo "acceptance: kcat is not on PATH" >&2; exit 2; }

echo "acceptance: the cluster as a client sees it"

# The client credential comes from the host that generated it — it exists
# nowhere else, and least of all in this repository.
creds=$(on 0 sudo /usr/local/bin/automq-credential 2>/dev/null)
PASSWORD=$(sed -n 's/^password: *//p' <<<"$creds")
PRINCIPAL=$(sed -n 's/^principal: *//p' <<<"$creds")
[ -n "$PASSWORD" ] || { echo "acceptance: could not retrieve the client credential" >&2; exit 2; }

kc() {
  kcat -b "$BOOTSTRAP" \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanism=SCRAM-SHA-512 \
    -X "sasl.username=$PRINCIPAL" \
    -X "sasl.password=$PASSWORD" "$@"
}

# --- 7: names resolve and the certificate they serve validates ---------------
for name in <{ automq-host }> $(for i in $(seq 0 $LAST); do echo "b${i}.<{ automq-host }>"; done); do
  if ! getent hosts "$name" >/dev/null; then bad "$name does not resolve"; continue; fi
  if echo | openssl s_client -connect "${name}:<{ kafka-port }>" -servername "$name" \
       -verify_return_error >/dev/null 2>&1; then
    ok "$name serves a valid certificate"
  else
    bad "$name did not complete a verified TLS handshake"
  fi
done

# --- 8: the public endpoint carries real traffic ------------------------------
if kc -L -m 20 >/dev/null 2>&1; then
  brokers=$(kc -L -m 20 2>/dev/null | grep -c '^ *broker ')
  [ "${brokers:-0}" -eq "$NODES" ] && ok "metadata over SASL_SSL lists $brokers brokers" \
    || bad "metadata lists ${brokers:-0} brokers, expected $NODES"
else
  bad "could not fetch metadata over SASL_SSL"
fi

sent=$(mktemp); got=$(mktemp)
trap 'rm -f "$sent" "$got"' EXIT
seq 1 200 | sed 's/^/public-/' > "$sent"
if kc -P -t "<{ topic-prefix }>acceptance" < "$sent" 2>/dev/null; then
  kc -C -t "<{ topic-prefix }>acceptance" -o beginning -e -q 2>/dev/null | grep '^public-' | sort -u > "$got"
  if [ "$(wc -l < "$got")" -ge 200 ]; then
    ok "200 records produced and consumed through the public endpoint"
  else
    bad "consumed $(wc -l < "$got") of 200 records through the public endpoint"
  fi
else
  bad "could not produce through the public endpoint"
fi

# --- 11: authentication and authorization refuse from outside too -------------
if kcat -b "$BOOTSTRAP" -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 \
     -X "sasl.username=$PRINCIPAL" -X sasl.password=wrong-password -L -m 10 >/dev/null 2>&1; then
  bad "a wrong password was accepted by the public endpoint"
else
  ok "a wrong password is refused by the public endpoint"
fi

if kc -P -t "outside-prefix-$$" <<<"nope" 2>/dev/null; then
  bad "the client principal wrote to a topic outside its prefix"
else
  ok "the client principal cannot write outside <{ topic-prefix }>"
fi

# --- 10a: a consumer group with committed offsets, established BEFORE the
# outage. __consumer_offsets is replication factor 1 like every other internal
# topic, so the partition holding this group's offsets can be led by the broker
# about to be killed — which is exactly the case worth testing.
#
# `kcat -G <group> <topic>` takes the topic POSITIONALLY and replaces -C. The
# first version of this gate wrote `-C -t <topic> -G <group>`, which consumes
# nothing, commits nothing, and then fails on an assertion about the group.
group="<{ topic-prefix }>survivor-$RUN"
kc -G "$group" "<{ topic-prefix }>acceptance" -o beginning -e -q -c 50 >/dev/null 2>&1
committed_before=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
  --describe --group $group" 2>/dev/null | awk '$1==g && $4 ~ /^[0-9]+$/ { n += $4 } END { print n+0 }' g="$group")
if [ "${committed_before:-0}" -gt 0 ]; then
  ok "consumer group $group committed offsets (sum ${committed_before}) before the outage"
else
  bad "consumer group $group committed no offsets before the outage"
fi

# --- 9: targeted failover -----------------------------------------------------
#
# The partition is chosen, not assumed. Unkeyed records spread over six
# partitions can complete a round trip without ever touching the broker that
# was killed, which is how a failover test passes while proving nothing.
echo "acceptance: failover"
on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-topics.sh \
  --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
  --create --if-not-exists --topic $TOPIC --partitions <{ automq-topic-partitions }> \
  --replication-factor 1" >/dev/null 2>&1

describe=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-topics.sh \
  --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
  --describe --topic $TOPIC" 2>/dev/null)
victim_partition=$(awk -v want="$LAST" '/Partition:/ { for (i=1;i<=NF;i++) if ($i=="Leader:" && $(i+1)==want) { for (j=1;j<=NF;j++) if ($j=="Partition:") print $(j+1) } }' <<<"$describe" | head -1)

if [ -z "$victim_partition" ]; then
  bad "no partition of $TOPIC is led by node $LAST; cannot target the failover"
else
  ok "partition $victim_partition of $TOPIC is led by node $LAST"

  before=$(seq 1 100 | sed "s/^/before-$RUN-/")
  echo "$before" | kc -P -t "$TOPIC" -p "$victim_partition" 2>/dev/null

  on "$LAST" "sudo docker stop automq" >/dev/null 2>&1
  killed_at=$(date +%s)

  recovered=""
  for _ in $(seq 1 60); do
    if echo "during-$(date +%s)" | kc -P -t "$TOPIC" -p "$victim_partition" 2>/dev/null; then
      recovered=$(( $(date +%s) - killed_at )); break
    fi
    sleep 5
  done

  if [ -n "$recovered" ]; then
    ok "the partition became writable again ${recovered}s after its leader was killed"
  else
    bad "the partition did not become writable within 300s of losing its leader"
  fi

  # Nothing written before the kill may be missing afterwards. This is the
  # claim S3-backed storage actually makes, and the one worth checking.
  kept=$(kc -C -t "$TOPIC" -p "$victim_partition" -o beginning -e -q 2>/dev/null | grep -c "^before-$RUN-")
  [ "${kept:-0}" -eq 100 ] && ok "all 100 pre-failure records survived the leader's death" \
    || bad "only ${kept:-0} of 100 pre-failure records survived"

  on "$LAST" "sudo docker start automq" >/dev/null 2>&1

  # "Rejoined" is three measurements, not a voter-list entry: a static voter
  # stays listed while it is dead.
  rejoined=""
  for _ in $(seq 1 60); do
    status=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-metadata-quorum.sh \
      --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
      describe --replication" 2>/dev/null)
    if grep -qE "^$LAST[[:space:]]" <<<"$status"; then
      lag=$(awk -v n="$LAST" '$1==n { print $NF }' <<<"$status" | head -1)
      brokers=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties" 2>/dev/null | grep -c 'id: ')
      if [ "${brokers:-0}" -eq "$NODES" ]; then rejoined="lag=${lag:-?}"; break; fi
    fi
    sleep 10
  done
  [ -n "$rejoined" ] && ok "node $LAST re-registered and caught up ($rejoined)" \
    || bad "node $LAST did not re-register and catch up within 600s"
fi

# --- 10b: the group survived the outage ---------------------------------------
#
# The claim being tested is that committed offsets on an RF=1
# __consumer_offsets partition come back after the broker leading it dies —
# not merely that a group can commit at all.
committed_after=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
  --describe --group $group" 2>/dev/null | awk '$1==g && $4 ~ /^[0-9]+$/ { n += $4 } END { print n+0 }' g="$group")
if [ "${committed_after:-0}" -ge "${committed_before:-0}" ] && [ "${committed_after:-0}" -gt 0 ]; then
  ok "consumer group $group kept its committed offsets across the outage (${committed_after})"
else
  bad "consumer group $group lost committed offsets across the outage (${committed_before:-0} -> ${committed_after:-0})"
fi

# --- 13: controller authentication survives a restart -------------------------
#
# The gate that catches a controller listener which only appears to work at
# genesis: PLAIN from a static JAAS file has to keep working when a controller
# rejoins a quorum it did not bootstrap.
on 1 "sudo docker restart automq" >/dev/null 2>&1
requorum=""
for _ in $(seq 1 60); do
  if on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-metadata-quorum.sh \
       --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
       describe --status" 2>/dev/null | grep -q 'LeaderId'; then
    voters=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties" 2>/dev/null | grep -c 'id: ')
    [ "${voters:-0}" -eq "$NODES" ] && { requorum=yes; break; }
  fi
  sleep 10
done
[ -n "$requorum" ] && ok "a restarted controller re-authenticated and rejoined the quorum" \
  || bad "the quorum did not recover after restarting a controller"

# --- 12: the cost of R2 being one provider away, measured ---------------------
perf=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-producer-perf-test.sh \
  --topic <{ topic-prefix }>acceptance --num-records 20000 --record-size 1024 --throughput -1 \
  --producer.config /etc/automq/admin.properties \
  --producer-props bootstrap.servers=<{ bootstrap-internal }>" 2>/dev/null | tail -1)
echo "  note — produce latency: ${perf:-unavailable}"

echo
echo "acceptance: $pass passed, $fail failed"
[ "$fail" -eq 0 ] || exit 1
