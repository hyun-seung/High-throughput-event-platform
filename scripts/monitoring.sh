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
compose=(docker compose -p platform-monitoring -f compose.yml -f monitoring/compose.yml)
case "${1:-help}" in
  up)
    bash scripts/check-kafka-volume.sh "${compose[@]}"
    for module in event-api delivery-ingress-worker dispatch-worker external-api-simulator; do
      [[ -f "$module/target/$module-1.0-SNAPSHOT.jar" ]] || { echo 'Build the application JARs first.' >&2; exit 1; }
    done
    mkdir -p .monitoring
    if [[ ! -f .monitoring/grafana-admin ]]; then
      (umask 077; java scripts/testing/MonitorArtifacts.java token > .monitoring/grafana-admin)
    fi
    java scripts/testing/MonitorArtifacts.java snapshot
    set -a
    source .monitoring/jars.env
    set +a
    "${compose[@]}" up -d --build
    echo 'Grafana: http://localhost:13000  (local read-only viewer)'
    ;;
  status) "${compose[@]}" ps ;;
  stop) "${compose[@]}" stop ;;
  logs) "${compose[@]}" logs --tail=80 "${2:-grafana}" ;;
  demo) shift; exec bash scripts/검증-실행.sh 모니터링-데모 "$@" ;;
  benchmark)
    shift
    [[ -x .poc-tools/venv/bin/python ]] || { echo 'Run python3 scripts/poc/환경_준비.py first.' >&2; exit 1; }
    exec .poc-tools/venv/bin/python scripts/monitoring/성능_측정.py "$@"
    ;;
  verify) exec bash scripts/검증-실행.sh 모니터링-검증 ;;
  *) echo 'Usage: bash scripts/monitoring.sh <up|status|stop|logs SERVICE|demo|benchmark|verify>' ;;
esac
