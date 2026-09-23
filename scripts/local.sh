#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

case "${1:-help}" in
  infra)
    docker compose up -d --wait --wait-timeout 180
    ;;
  init-db)
    docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U delivery -d delivery < scripts/local-init.sql
    ;;
  init-policy)
    user_id=$(docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U delivery -d delivery -Atc "SELECT id FROM users WHERE username = 'local-user'")
    [[ "$user_id" =~ ^[0-9]+$ ]] || { echo 'Run init-db first: local-user not found.' >&2; exit 1; }
    # Populate missing fields only; preserve existing policies and usage counters.
    policy_key="request-control:{user:$user_id}:policy"
    for pair in blocked:false tpsEnabled:true requestsPerSecond:100 burstCapacity:100 quotaEnabled:true monthlyLimit:100000; do
      docker compose exec -T redis redis-cli -n "${REDIS_DATABASE:-0}" HSETNX "$policy_key" "${pair%%:*}" "${pair#*:}" > /dev/null
    done
    echo "Local request policy initialized for user $user_id."
    ;;
  smoke)
    exec python3 scripts/local-smoke.py
    ;;
  build)
    "${MAVEN_BIN:-./mvnw}" clean package
    ;;
  run)
    module="${2:-}"
    case "$module" in
      event-api|delivery-ingress-worker|dispatch-worker|external-api-simulator) ;;
      *) echo 'Usage: bash scripts/local.sh run <event-api|delivery-ingress-worker|dispatch-worker|external-api-simulator>' >&2; exit 2 ;;
    esac
    export SPRING_PROFILES_ACTIVE=dev
    export KAFKA_BOOTSTRAP_SERVERS="localhost:${KAFKA_HOST_PORT:-9092}"
    export REDIS_HOST=localhost REDIS_PORT="${REDIS_HOST_PORT:-6379}"
    export DB_URL="jdbc:postgresql://localhost:${POSTGRES_HOST_PORT:-5432}/delivery"
    export DB_USERNAME=delivery DB_PASSWORD=delivery
    export DYNAMODB_ENDPOINT="http://localhost:${DYNAMODB_HOST_PORT:-8000}"
    export EXTERNAL_API_BASE_URL="http://localhost:${MOCK_PROVIDER_PORT:-8090}"
    export SERVER_PORT="${DELIVERY_API_PORT:-8080}"
    if [[ "$module" == external-api-simulator ]]; then
      export SERVER_PORT="${MOCK_PROVIDER_PORT:-8090}"
    fi
    if [[ "$module" == event-api ]]; then
      : "${JWT_SECRET:?Copy .env.example to .env and set JWT_SECRET}"
    fi
    jar="$module/target/$module-1.0-SNAPSHOT.jar"
    [[ -f "$jar" ]] || { echo 'Build first: bash scripts/local.sh build' >&2; exit 1; }
    java_bin="${JAVA_HOME:+$JAVA_HOME/bin/}java"
    java_version=$("$java_bin" -XshowSettings:properties -version 2>&1) || { echo 'Cannot run Java. Set JAVA_HOME to a JDK 21 installation.' >&2; exit 1; }
    java_major=$(awk '$1 == "java.specification.version" { print $3 }' <<< "$java_version")
    [[ "$java_major" == 21 ]] || { echo "Java 21 is required (found $java_major). Set JAVA_HOME to a JDK 21 installation." >&2; exit 1; }
    exec "$java_bin" -jar "$jar" --server.port="$SERVER_PORT"
    ;;
  status)
    docker compose ps
    ;;
  down)
    docker compose down
    ;;
  *)
    echo 'Usage: bash scripts/local.sh <infra|init-db|init-policy|build|run MODULE|smoke|status|down>'
    ;;
esac
