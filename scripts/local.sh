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
    bash scripts/check-kafka-volume.sh docker compose
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
    message_usage_key="message:usage:{client:$user_id}:policy"
    for pair in tpsLimit:100 quotaGENERAL:100000 quotaNOTI:100000 quotaADV:100000 quotaALERT:100000; do
      docker compose exec -T redis redis-cli -n "${REDIS_DATABASE:-0}" HSETNX "$message_usage_key" "${pair%%:*}" "${pair#*:}" > /dev/null
    done
    echo "Local request and message usage policies initialized for user $user_id."
    ;;
  cdc)
    # The local connector captures only the reference tables used by PRE-SEND-MANAGER.
    docker compose up -d --wait --wait-timeout 180 postgres kafka redis
    docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U delivery -d delivery < scripts/cdc/init.sql
    docker compose --profile cdc up -d --wait --wait-timeout 180 debezium-connect
    connect_url="http://127.0.0.1:${DEBEZIUM_HOST_PORT:-18083}"
    curl --fail --silent --show-error --max-time 5 --retry 20 --retry-delay 2 --retry-connrefused "$connect_url/connectors" > /dev/null
    curl --fail --silent --show-error --request PUT --header 'Content-Type: application/json' \
      --data-binary @scripts/cdc/connector-config.json \
      "$connect_url/connectors/messaging-reference-postgres/config" > /dev/null
    echo "Local reference CDC connector registered."
    ;;
  smoke)
    shift
    exec bash scripts/검증-실행.sh 로컬-점검 "$@"
    ;;
  build)
    "${MAVEN_BIN:-./mvnw}" clean package
    ;;
  run)
    module="${2:-}"
    case "$module" in
      messaging-api|messaging-publication-recovery-app|messaging-reference-cache|messaging-webhook-receive-api|messaging-http-sender|delivery-ingress-worker|dispatch-worker|external-api-simulator|receipt-api|delivery-result-worker) ;;
      *) echo 'Usage: bash scripts/local.sh run <messaging-api|messaging-publication-recovery-app|messaging-reference-cache|messaging-webhook-receive-api|messaging-http-sender|delivery-ingress-worker|dispatch-worker|external-api-simulator|receipt-api|delivery-result-worker>' >&2; exit 2 ;;
    esac
    export SPRING_PROFILES_ACTIVE=dev
    export KAFKA_BOOTSTRAP_SERVERS="localhost:${KAFKA_HOST_PORT:-9092}"
    export REDIS_HOST=localhost REDIS_PORT="${REDIS_HOST_PORT:-6379}"
    export DB_URL="jdbc:postgresql://localhost:${POSTGRES_HOST_PORT:-5432}/delivery"
    export DB_USERNAME=delivery DB_PASSWORD=delivery
    export DYNAMODB_ENDPOINT="http://localhost:${DYNAMODB_HOST_PORT:-8000}"
    export EXTERNAL_TCP_PORT="${MOCK_TCP_PORT:-8093}" SIMULATOR_TCP_PORT="${MOCK_TCP_PORT:-8093}"
    export EXTERNAL_API_BASE_URL="http://localhost:${MOCK_PROVIDER_PORT:-8090}"
    export SERVER_PORT="${DELIVERY_API_PORT:-8080}"
    case "$module" in
      external-api-simulator) export SERVER_PORT="${MOCK_PROVIDER_PORT:-8090}" ;;
      delivery-ingress-worker) export SERVER_PORT="${INGRESS_HTTP_PORT:-8091}" ;;
      dispatch-worker) export SERVER_PORT="${DISPATCH_HTTP_PORT:-8092}" ;;
      receipt-api) export SERVER_PORT="${RECEIPT_API_PORT:-8094}" ;;
      delivery-result-worker) export SERVER_PORT="${RESULT_HTTP_PORT:-8095}" ;;
      messaging-publication-recovery-app) export SERVER_PORT="${PUBLISHER_HTTP_PORT:-8096}" ;;
      messaging-reference-cache) export SERVER_PORT="${MESSAGING_REFERENCE_CACHE_PORT:-8098}" ;;
      messaging-webhook-receive-api) export SERVER_PORT="${MESSAGE_WEBHOOK_PORT:-8099}" ;;
      messaging-http-sender) export SERVER_PORT="${MESSAGING_HTTP_SENDER_PORT:-8097}" ;;
    esac
    if [[ "$module" == messaging-api ]]; then
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
    echo 'Usage: bash scripts/local.sh <infra|init-db|init-policy|cdc|build|run MODULE|smoke|status|down>'
    ;;
esac
