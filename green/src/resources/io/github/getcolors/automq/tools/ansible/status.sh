#!/usr/bin/env bash
# Health, and only health. The password lives behind `automq-credential`, so
# the command an operator runs a hundred times cannot leak it into scrollback,
# a screen share, or a support transcript.
set -uo pipefail

BOOTSTRAP="${1:-<{ bootstrap-internal }>}"
KAFKA=/opt/automq/kafka/bin
ADMIN=/etc/automq/admin.properties

k() { docker exec automq "$KAFKA/$@" 2>/dev/null; }

echo "container:"
docker ps --filter name=automq --format '  {{.Status}}' || echo "  not running"

echo "quorum:"
k kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" \
  describe --status | sed 's/^/  /' || echo "  unavailable"

echo "brokers:"
k kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" \
  | grep 'id: ' | sed 's/^/  /' || echo "  unavailable"

echo "partitions:"
under=$(k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" \
          --describe --under-replicated-partitions | wc -l)
offline=$(k kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN" \
            --describe --unavailable-partitions | wc -l)
echo "  under-replicated: ${under:-unknown}"
echo "  offline:          ${offline:-unknown}"

echo "jvm:"
docker stats --no-stream --format '  mem {{.MemUsage}}  cpu {{.CPUPerc}}' automq 2>/dev/null \
  || echo "  unavailable"

echo "certificate:"
if [ -s /etc/automq/tls/fullchain.pem ]; then
  end=$(openssl x509 -in /etc/automq/tls/fullchain.pem -noout -enddate | cut -d= -f2)
  secs=$(( $(date -d "$end" +%s) - $(date +%s) ))
  echo "  expires in $(( secs / 86400 )) days ($end)"
  openssl x509 -in /etc/automq/tls/fullchain.pem -noout -ext subjectAltName | sed 's/^/  /'
else
  echo "  no certificate installed"
fi

# Deliberately labelled. These come from a log that rotates, resets on restart,
# and can double-count; they are a hint about where to look, not an accounting
# of what happened. Real metering would mean exporting JMX, which this
# deployment does not do — see the README.
echo "diagnostics (best-effort, from the current log only):"
auth=$(docker logs automq 2>&1 | grep -c 'Authentication failed' || true)
s3err=$(docker logs automq 2>&1 | grep -ci 'S3Exception\|software.amazon.awssdk.*Exception' || true)
echo "  authentication failures: ${auth:-0}"
echo "  s3 errors:               ${s3err:-0}"
