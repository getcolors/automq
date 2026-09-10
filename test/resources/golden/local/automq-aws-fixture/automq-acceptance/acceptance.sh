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

# Retain public gate measurements even when the workflow runner buffers output.
# Credentials are captured into variables below and never printed.
RESULT_LOG="$(dirname "$0")/result.log"
: > "$RESULT_LOG"
exec > >(tee -a "$RESULT_LOG") 2> >(tee -a "$RESULT_LOG" >&2)

BOOTSTRAP="192.0.2.10:9092"
# Rendered from the same derivation the DNS records, advertised listeners and
# certificate SANs use, so a non-default broker prefix cannot make this loop
# test names nothing serves.
IFS=',' read -ra CERT_NAMES <<< "192.0.2.10,192.0.2.11,192.0.2.12"
TOPIC="colors-failover"
PROFILE="automq-aws-fixture"
NODES=3
pass=0
fail=0
# Every record and group this run creates is tagged with it. These gates run on
# every converge against a cluster that keeps its data, so a gate that counts
# "100 records" must count THIS run's hundred — otherwise it passes once and
# then fails forever against a perfectly healthy cluster.
RUN="$(date +%s)-$$"

ok()   { pass=$((pass+1)); echo "  ok   — $*"; }
bad()  { fail=$((fail+1)); echo "  FAIL — $*" >&2; }
# A phase deadline includes time spent inside clients and SSH, not only sleeps.
DEADLINE=0
bounded() {
  local limit="$1"; shift
  if [ "$DEADLINE" -gt 0 ]; then
    local remaining=$((DEADLINE - SECONDS))
    [ "$remaining" -gt 0 ] || return 124
    [ "$remaining" -ge "$limit" ] || limit=$remaining
  fi
  timeout --foreground --kill-after=2 "${limit}s" "$@"
}
phase_pause() {
  local delay="$1" remaining=$((DEADLINE - SECONDS))
  [ "$remaining" -gt 0 ] || return 1
  [ "$remaining" -ge "$delay" ] || delay=$remaining
  sleep "$delay"
}
on() {
  local n="$1"; shift
  bounded 180 ssh -o BatchMode=yes -o ConnectTimeout=10 \
    -o ServerAliveInterval=15 -o ServerAliveCountMax=2 \
    -o StrictHostKeyChecking=accept-new "${PROFILE}-${n}" "$@"
}

sent=""; got=""; offsets_before=""; offsets_after=""; stopped_node=""
cleanup() {
  local rc=$?
  trap - EXIT
  DEADLINE=0
  if [ -n "$stopped_node" ]; then
    if ! on "$stopped_node" "sudo docker start automq" >/dev/null 2>&1; then
      echo "acceptance: could not restore node $stopped_node during cleanup" >&2
      rc=1
    fi
  fi
  rm -f -- "$sent" "$got" "$offsets_before" "$offsets_after"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

command -v timeout >/dev/null || { echo "acceptance: timeout is not on PATH" >&2; exit 2; }
command -v kcat >/dev/null || { echo "acceptance: kcat is not on PATH" >&2; exit 2; }

echo "acceptance: the cluster as a client sees it"

# The client credential comes from the host that generated it — it exists
# nowhere else, and least of all in this repository.
creds=$(on 0 sudo /usr/local/bin/automq-credential 2>/dev/null)
PASSWORD=$(sed -n 's/^password: *//p' <<<"$creds")
PRINCIPAL=$(sed -n 's/^principal: *//p' <<<"$creds")
[ -n "$PASSWORD" ] || { echo "acceptance: could not retrieve the client credential" >&2; exit 2; }

# Fetch only the public CA over the deployment's authenticated SSH connection.
TLS_ARGS=()
VERIFY_ARGS=()
if [ 'private-ca' = private-ca ]; then
  CA_FILE="${AUTOMQ_CA_FILE:-$(dirname "$0")/ca.crt}"
  on 0 sudo cat /etc/automq/ca/ca.crt > "$CA_FILE" || { echo 'acceptance: CA export failed' >&2; exit 2; }
  openssl x509 -in "$CA_FILE" -noout >/dev/null || exit 2
  TLS_ARGS=(-X "ssl.ca.location=$CA_FILE")
  VERIFY_ARGS=(-CAfile "$CA_FILE")
  echo "  note — client CA: $CA_FILE"
fi

kc() {
  bounded 30 kcat "${TLS_ARGS[@]}" -b "$BOOTSTRAP" \
    -X security.protocol=SASL_SSL \
    -X sasl.mechanism=SCRAM-SHA-512 \
    -X "sasl.username=$PRINCIPAL" \
    -X "sasl.password=$PASSWORD" -X message.timeout.ms=10000 "$@"
}

# --- 7: names resolve and the certificate they serve validates ---------------
for name in "${CERT_NAMES[@]}"; do
  if ! getent hosts "$name" >/dev/null; then bad "$name does not resolve"; continue; fi
  if [ 'private-ca' = private-ca ]; then identity=(-verify_ip "$name"); else identity=(-verify_hostname "$name"); fi
  if echo | bounded 30 openssl s_client "${VERIFY_ARGS[@]}" "${identity[@]}" -connect "${name}:9092" -servername "$name" \
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
seq 1 200 | sed "s/^/public-$RUN-/" | sort > "$sent"
if kc -P -t "colors-acceptance" < "$sent" 2>/dev/null; then
  if kc -C -t "colors-acceptance" -o beginning -e -q 2>/dev/null \
       | grep "^public-$RUN-" | sort -u > "$got" && cmp -s "$sent" "$got"; then
    ok "200 records produced and consumed through the public endpoint"
  else
    bad "consumed $(wc -l < "$got") of 200 records through the public endpoint"
  fi
else
  bad "could not produce through the public endpoint"
fi

# --- 11: authentication and authorization refuse from outside too -------------
if bounded 30 kcat "${TLS_ARGS[@]}" -b "$BOOTSTRAP" -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 \
     -X "sasl.username=$PRINCIPAL" -X sasl.password=wrong-password -L -m 10 >/dev/null 2>&1; then
  bad "a wrong password was accepted by the public endpoint"
else
  ok "a wrong password is refused by the public endpoint"
fi

if kc -P -t "outside-prefix-$$" <<<"nope" 2>/dev/null; then
  bad "the client principal wrote to a topic outside its prefix"
else
  ok "the client principal cannot write outside colors-"
fi

# --- 10a: a consumer group with committed offsets, established BEFORE the
# outage. __consumer_offsets is replication factor 1 like every other internal
# topic, so the partition holding this group's offsets can be led by the broker
# about to be killed — which is exactly the case worth testing.
#
# `kcat -G <group> <topic>` takes the topic POSITIONALLY and replaces -C. The
# first version of this gate wrote `-C -t <topic> -G <group>`, which consumes
# nothing, commits nothing, and then fails on an assertion about the group.
group="colors-survivor-$RUN"
kc -G "$group" "colors-acceptance" -o beginning -e -q -c 50 >/dev/null 2>&1
offsets_before=$(mktemp); offsets_after=$(mktemp)
read_offsets() {
  on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties \
    --describe --group $group" 2>/dev/null \
    | awk '$1==g && $3 ~ /^[0-9]+$/ && $4 ~ /^[0-9]+$/ { print $2, $3, $4 }' g="$group" | sort
}
if read_offsets > "$offsets_before" && [ -s "$offsets_before" ] \
    && awk '{n += $3} END {exit !(n > 0)}' "$offsets_before"; then
  ok "consumer group $group committed per-partition offsets before the outage"
else
  bad "consumer group $group committed no measurable offsets before the outage"
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
on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties \
  --delete --topic $TOPIC" >/dev/null 2>&1 || true
for _ in $(seq 1 30); do
  on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-topics.sh \
    --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties \
    --list" 2>/dev/null | grep -qx "$TOPIC" || break
  sleep 2
done
on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties \
  --create --if-not-exists --topic $TOPIC --partitions 6 \
  --replication-factor 1" >/dev/null 2>&1

describe=$(on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties \
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

  before=$(seq 1 100 | sed "s/^/before-$RUN-/" | sort)
  # Verify the produce, rather than assuming it. If this silently fails, the
  # survival check later reports "0 of 100 survived" — a data-loss claim about
  # records that were never written.
  if ! echo "$before" | kc -P -t "$TOPIC" -p "$victim_partition" 2>/dev/null; then
    bad "could not produce the pre-failure records to partition $victim_partition"
    exit 1
  fi
  produced=0
  DEADLINE=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if kc -C -t "$TOPIC" -p "$victim_partition" -o beginning -e -q 2>/dev/null \
        | grep "^before-$RUN-" | sort -u > "$got" && cmp -s <(printf '%s\n' "$before") "$got"; then
      produced=100; break
    fi
    phase_pause 5 || break
  done
  DEADLINE=0
  if [ "$produced" -ne 100 ]; then
    bad "the complete pre-failure record set was not readable before the kill"
    exit 1
  fi
  ok "100 pre-failure records are readable before the kill"

  stopped_node="$victim"
  if ! on "$victim" "sudo docker stop automq" >/dev/null 2>&1; then
    bad "could not stop node $victim; failover was not exercised"
    exit 1
  fi
  killed_at=$SECONDS

  recovered=""
  DEADLINE=$((SECONDS + 300))
  while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if echo "during-$RUN" | kc -P -t "$TOPIC" -p "$victim_partition" 2>/dev/null; then
      recovered=$((SECONDS - killed_at)); break
    fi
    phase_pause 5 || break
  done
  DEADLINE=0

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
  DEADLINE=$((SECONDS + 120))
  while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if kc -C -t "$TOPIC" -p "$victim_partition" -o beginning -e -q 2>/dev/null \
        | grep "^before-$RUN-" | sort -u > "$got" && cmp -s <(printf '%s\n' "$before") "$got"; then
      kept=100; break
    fi
    phase_pause 5 || break
  done
  DEADLINE=0
  [ "$kept" -ge 100 ] && ok "all 100 pre-failure records survived the leader's death" \
    || bad "only ${kept} of 100 pre-failure records were readable within 120s of the failover"

  if ! on "$victim" "sudo docker start automq" >/dev/null 2>&1; then
    bad "could not restart node $victim after the fault injection"
    exit 1
  fi
  stopped_node=""

  # "Rejoined" is three measurements, not a voter-list entry: a static voter
  # stays listed while it is dead. The replication table gives NodeId,
  # LogEndOffset, Lag and Status, so lag and log-end offset are checkable
  # rather than merely recorded.
  rejoined=""
  DEADLINE=$((SECONDS + 600))
  while [ "$SECONDS" -lt "$DEADLINE" ]; do
    status=$(on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-metadata-quorum.sh \
      --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties \
      describe --replication" 2>/dev/null)
    leader_leo=$(awk '$NF=="Leader" { print $3 }' <<<"$status" | head -1)
    node_row=$(awk -v n="$victim" '$1==n { print }' <<<"$status" | head -1)
    node_leo=$(awk '{ print $3 }' <<<"$node_row")
    node_lag=$(awk '{ print $4 }' <<<"$node_row")
    brokers=$(on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties" 2>/dev/null | grep -c 'id: ')
    if [ "${brokers:-0}" -eq "$NODES" ] && [ -n "$node_leo" ] && [ -n "$leader_leo" ] \
       && [ "${node_lag:-999}" -le 10 ] && [ "$node_leo" -ge $(( leader_leo - 10 )) ]; then
      rejoined="lag=${node_lag} logEndOffset=${node_leo} leader=${leader_leo}"
      break
    fi
    phase_pause 10 || break
  done
  DEADLINE=0
  [ -n "$rejoined" ] && ok "node $victim re-registered and caught up ($rejoined)" \
    || bad "node $victim did not re-register with bounded lag within 600s"
fi

# --- 10b: the group survived the outage ---------------------------------------
#
# The claim being tested is that committed offsets on an RF=1
# __consumer_offsets partition come back after the broker leading it dies —
# not merely that a group can commit at all.
if read_offsets > "$offsets_after" && [ -s "$offsets_before" ] \
    && awk 'NR==FNR {expected[$1 SUBSEP $2]=$3; count++; next}
            {key=$1 SUBSEP $2; if (key in expected && $3 >= expected[key]) {seen[key]=1}}
            END {for (key in expected) if (!(key in seen)) exit 1; if (!count) exit 1}' \
       "$offsets_before" "$offsets_after"; then
  ok "consumer group $group kept every partition's committed offset across the outage"
else
  bad "consumer group $group lost or could not verify a committed partition offset"
fi

# --- 13: controller authentication survives a restart -------------------------
#
# The gate that catches a controller listener which only appears to work at
# genesis: PLAIN from a static JAAS file has to keep working when a controller
# rejoins a quorum it did not bootstrap.
controller_victim=$(( NODES > 1 ? 1 : 0 ))
if ! on "$controller_victim" "sudo docker restart automq" >/dev/null 2>&1; then
  bad "could not restart controller node $controller_victim; re-authentication was not exercised"
  exit 1
fi
requorum=""
DEADLINE=$((SECONDS + 600))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
  if on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-metadata-quorum.sh \
       --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties \
       describe --status" 2>/dev/null | grep -q 'LeaderId'; then
    voters=$(on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server 10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094 --command-config /etc/automq/admin.properties" 2>/dev/null | grep -c 'id: ')
    [ "${voters:-0}" -eq "$NODES" ] && { requorum=yes; break; }
  fi
  phase_pause 10 || break
done
DEADLINE=0
[ -n "$requorum" ] && ok "a restarted controller (node $controller_victim) re-authenticated and rejoined the quorum" \
  || bad "the quorum did not recover after restarting a controller"

# --- 12: the cost of R2 being one provider away, measured ---------------------
if perf=$(on 0 "sudo docker exec -e KAFKA_HEAP_OPTS=-Xmx256m automq /opt/automq/kafka/bin/kafka-producer-perf-test.sh \
  --topic colors-acceptance --num-records 20000 --record-size 1024 --throughput -1 \
  --producer.config /etc/automq/admin.properties \
  --producer-props bootstrap.servers=10.73.1.10:9094,10.73.1.11:9094,10.73.1.12:9094" 2>/dev/null | tail -1)
then
  if [ -n "$perf" ] && [[ "$perf" == *records*sent* ]]; then
    ok "producer performance workload completed: $perf"
  else
    bad "producer performance workload returned no completion report"
  fi
else
  bad "producer performance workload failed"
fi

echo
echo "acceptance: $pass passed, $fail failed"
[ "$fail" -eq 0 ] || exit 1
