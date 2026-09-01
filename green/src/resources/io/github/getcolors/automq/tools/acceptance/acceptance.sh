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
# Rendered from the same derivation the DNS records, advertised listeners and
# certificate SANs use, so a non-default broker prefix cannot make this loop
# test names nothing serves.
IFS=',' read -ra CERT_NAMES <<< "<{ certificate-names-csv }>"
TOPIC="<{ topic-prefix }>failover"
PROFILE="<{ profile }>"
NODES=<{ node-count }>
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
for name in "${CERT_NAMES[@]}"; do
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

# Recreated every run. Leadership drifts after a previous failover, so a topic
# left from last time can easily have no partition led by any particular node —
# which is what "no partition is led by node 2" meant on a healthy cluster.
on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-topics.sh \
  --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
  --delete --topic $TOPIC" >/dev/null 2>&1 || true
for _ in $(seq 1 30); do
  on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-topics.sh \
    --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
    --list" 2>/dev/null | grep -qx "$TOPIC" || break
  sleep 2
done
on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-topics.sh \
  --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
  --create --if-not-exists --topic $TOPIC --partitions <{ automq-topic-partitions }> \
  --replication-factor 1" >/dev/null 2>&1

describe=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-topics.sh \
  --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
  --describe --topic $TOPIC" 2>/dev/null)

# The victim is whichever non-zero node actually leads a partition. Fixing it to
# the last node asserts something about placement that nothing guarantees; node
# 0 is excluded only because it is this script's administrative path.
victim=""
victim_partition=""
while read -r part leader; do
  [ -n "$leader" ] || continue
  [ "$leader" = "0" ] && continue
  victim="$leader"; victim_partition="$part"; break
done < <(awk '/Partition:/ { p=""; l=""; for (i=1;i<=NF;i++) { if ($i=="Partition:") p=$(i+1); if ($i=="Leader:") l=$(i+1) } if (p!="" && l!="") print p, l }' <<<"$describe")

if [ -z "$victim" ]; then
  bad "no partition of $TOPIC is led by a non-zero node; cannot target the failover"
else
  ok "partition $victim_partition of $TOPIC is led by node $victim"

  before=$(seq 1 100 | sed "s/^/before-$RUN-/")
  # Verify the produce, rather than assuming it. If this silently fails, the
  # survival check later reports "0 of 100 survived" — a data-loss claim about
  # records that were never written.
  if ! echo "$before" | kc -P -t "$TOPIC" -p "$victim_partition" 2>/dev/null; then
    bad "could not produce the pre-failure records to partition $victim_partition"
  fi
  produced=0
  for _ in $(seq 1 12); do
    produced=$(kc -C -t "$TOPIC" -p "$victim_partition" -o beginning -e -q 2>/dev/null | grep -c "^before-$RUN-")
    [ "${produced:-0}" -ge 100 ] && break
    sleep 5
  done
  [ "${produced:-0}" -ge 100 ] && ok "100 pre-failure records are readable before the kill" \
    || bad "only ${produced:-0} of 100 pre-failure records were readable before the kill"

  on "$victim" "sudo docker stop automq" >/dev/null 2>&1
  killed_at=$(date +%s)
  # Whatever happens next — an assertion failing, an ssh error, an interrupt —
  # the broker must come back. Without this the script can leave a live cluster
  # one node down.
  trap 'on "$victim" "sudo docker start automq" >/dev/null 2>&1 || true' EXIT INT TERM

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
  #
  # Retried, because a partition whose leader just died is briefly not
  # fetchable while it is reassigned, and a single read at the wrong moment
  # returns nothing. Reporting that as "0 of 100 survived" is a data-loss
  # claim about records that are sitting safely in object storage — the most
  # alarming thing this gate could say, and it would be false.
  kept=0
  for _ in $(seq 1 24); do
    n=$(kc -C -t "$TOPIC" -p "$victim_partition" -o beginning -e -q 2>/dev/null | grep -c "^before-$RUN-")
    [ "${n:-0}" -gt "$kept" ] && kept=$n
    [ "$kept" -ge 100 ] && break
    sleep 5
  done
  [ "$kept" -ge 100 ] && ok "all 100 pre-failure records survived the leader's death" \
    || bad "only ${kept} of 100 pre-failure records were readable within 120s of the failover"

  on "$victim" "sudo docker start automq" >/dev/null 2>&1
  trap - EXIT INT TERM

  # "Rejoined" is three measurements, not a voter-list entry: a static voter
  # stays listed while it is dead. The replication table gives NodeId,
  # LogEndOffset, Lag and Status, so lag and log-end offset are checkable
  # rather than merely recorded.
  rejoined=""
  for _ in $(seq 1 60); do
    status=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-metadata-quorum.sh \
      --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties \
      describe --replication" 2>/dev/null)
    leader_leo=$(awk '$NF=="Leader" { print $3 }' <<<"$status" | head -1)
    node_row=$(awk -v n="$victim" '$1==n { print }' <<<"$status" | head -1)
    node_leo=$(awk '{ print $3 }' <<<"$node_row")
    node_lag=$(awk '{ print $4 }' <<<"$node_row")
    brokers=$(on 0 "sudo docker exec automq /opt/automq/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server <{ bootstrap-internal }> --command-config /etc/automq/admin.properties" 2>/dev/null | grep -c 'id: ')
    if [ "${brokers:-0}" -eq "$NODES" ] && [ -n "$node_leo" ] && [ -n "$leader_leo" ] \
       && [ "${node_lag:-999}" -le 10 ] && [ "$node_leo" -ge $(( leader_leo - 10 )) ]; then
      rejoined="lag=${node_lag} logEndOffset=${node_leo} leader=${leader_leo}"
      break
    fi
    sleep 10
  done
  [ -n "$rejoined" ] && ok "node $victim re-registered and caught up ($rejoined)" \
    || bad "node $victim did not re-register with bounded lag within 600s"
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
controller_victim=$(( NODES > 1 ? 1 : 0 ))
on "$controller_victim" "sudo docker restart automq" >/dev/null 2>&1
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
[ -n "$requorum" ] && ok "a restarted controller (node $controller_victim) re-authenticated and rejoined the quorum" \
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
