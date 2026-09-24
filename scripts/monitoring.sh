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
    for module in event-api delivery-ingress-worker dispatch-worker external-api-simulator; do
      [[ -f "$module/target/$module-1.0-SNAPSHOT.jar" ]] || { echo 'Build the application JARs first.' >&2; exit 1; }
    done
    mkdir -p .monitoring
    if [[ ! -f .monitoring/grafana-admin ]]; then
      (umask 077; python3 -c 'import secrets; print(secrets.token_urlsafe(32))' > .monitoring/grafana-admin)
    fi
    python3 scripts/monitoring/snapshot_jars.py
    set -a
    source .monitoring/jars.env
    set +a
    "${compose[@]}" up -d --build
    echo 'Grafana: http://localhost:13000  (local read-only viewer)'
    ;;
  status) "${compose[@]}" ps ;;
  stop) "${compose[@]}" stop ;;
  logs) "${compose[@]}" logs --tail=80 "${2:-grafana}" ;;
  demo) shift; exec python3 scripts/monitoring/demo.py "$@" ;;
  benchmark)
    shift
    [[ -x .poc-tools/venv/bin/python ]] || { echo 'Run python3 scripts/poc/setup.py first.' >&2; exit 1; }
    exec .poc-tools/venv/bin/python scripts/monitoring/benchmark.py "$@"
    ;;
  verify) exec python3 scripts/monitoring/verify.py ;;
  *) echo 'Usage: bash scripts/monitoring.sh <up|status|stop|logs SERVICE|demo|benchmark|verify>' ;;
esac
