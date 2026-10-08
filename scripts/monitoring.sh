#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Deliberately isolated from .env, platform-poc and the developer's services.
export KAFKA_HOST_PORT=39092 REDIS_HOST_PORT=36379 POSTGRES_HOST_PORT=35432 DYNAMODB_HOST_PORT=38000
if [[ -f .monitoring/jars.env ]]; then
  set -a
  source .monitoring/jars.env
  set +a
fi
compose=(docker compose -p platform-messaging-monitoring -f compose.yml -f monitoring/compose.yml)
case "${1:-help}" in
  up)
    bash scripts/check-kafka-volume.sh "${compose[@]}"
    for module in messaging-api messaging-pre-send-manager messaging-carrier-http-sender \
      messaging-webhook-receive-api messaging-result-manager messaging-complete-manager \
      messaging-webhook-sender messaging-tcp-sender messaging-publication-recovery-app; do
      [[ -f "$module/target/$module-1.0-SNAPSHOT.jar" ]] || { echo 'Build the application JARs first.' >&2; exit 1; }
    done
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
      JAVA_HOME="$(/usr/libexec/java_home -v 21)"
      export JAVA_HOME
    fi
    ./mvnw -q -pl verification-tools -am -DskipTests package
    mkdir -p .monitoring
    if [[ ! -f .monitoring/grafana-admin ]]; then
      (umask 077; java scripts/testing/MonitorArtifacts.java token > .monitoring/grafana-admin)
    fi
    java scripts/testing/MonitorArtifacts.java snapshot
    set -a
    source .monitoring/jars.env
    set +a
    "${compose[@]}" up -d api
    curl --fail --silent --show-error --retry 30 --retry-delay 2 --retry-all-errors \
      --max-time 5 http://127.0.0.1:38080/readyz > /dev/null
    "${compose[@]}" build collector
    "${compose[@]}" up -d --no-deps --remove-orphans mock prometheus loki alloy grafana redis-exporter postgres-exporter collector
    "${compose[@]}" restart prometheus alloy
    python3 scripts/monitoring/main_flow.py wait-job --job prometheus --timeout 60
    for service in pre-send skt-sender webhook-receive result-manager complete-manager \
      webhook-sender kt-sender lgu-sender tcp-sender publication-recovery; do
      "${compose[@]}" up -d --no-deps "$service"
      python3 scripts/monitoring/main_flow.py wait-job --job "$service" --timeout 180
    done
    echo 'Grafana: http://localhost:13000  (local read-only viewer)'
    ;;
  status) "${compose[@]}" ps ;;
  stop) "${compose[@]}" stop ;;
  logs) "${compose[@]}" logs --tail=80 "${2:-grafana}" ;;
  demo) shift; exec python3 scripts/monitoring/main_flow.py demo "$@" ;;
  verify) exec python3 scripts/monitoring/main_flow.py verify ;;
  diagnose) exec python3 scripts/monitoring/main_flow.py diagnose ;;
  recovery-test) shift; exec python3 scripts/monitoring/main_flow.py recovery-test "$@" ;;
  *) echo 'Usage: bash scripts/monitoring.sh <up|status|stop|logs SERVICE|demo|verify|diagnose|recovery-test>' ;;
esac
