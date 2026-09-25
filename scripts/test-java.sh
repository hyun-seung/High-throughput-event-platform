#!/usr/bin/env bash
# External orchestration: Bash + Docker Compose. All application assertions run in Java/JUnit.
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:-unit}
case "$mode" in unit|integration) ;; *) echo 'Usage: bash scripts/test-java.sh [unit|integration]' >&2; exit 2;; esac
java_bin="${JAVA_HOME:+$JAVA_HOME/bin/}java"
version=$("$java_bin" -XshowSettings:properties -version 2>&1)
[[ $(awk '$1 == "java.specification.version" {print $3}' <<< "$version") == 21 ]] || { echo 'Set JAVA_HOME to JDK 21.' >&2; exit 1; }
run_id="java-test-$(date -u +%Y%m%d%H%M%S)-$$"
evidence="$PWD/.poc-results/$run_id"
mkdir -p "$evidence"
echo "EVIDENCE $evidence"
printf 'assertions=Java/JUnit\nrunner=Maven Wrapper\norchestration=Bash+Docker Compose\nmode=%s\n' "$mode" > "$evidence/tools.txt"
git rev-parse HEAD > "$evidence/base-commit.txt"
"$java_bin" -version 2> "$evidence/java-version.txt"
# Unit mode cannot accidentally connect to a caller's integration databases.
unset DYNAMODB_TEST_ENDPOINT KAFKA_TEST_BOOTSTRAP_SERVERS POSTGRES_TEST_URL REDIS_TEST_PORT
maven_command=(./mvnw clean verify)
if [[ "$mode" == integration ]]; then
  export KAFKA_HOST_PORT=${JAVA_TEST_KAFKA_PORT:-49092} REDIS_HOST_PORT=${JAVA_TEST_REDIS_PORT:-46379}
  export POSTGRES_HOST_PORT=${JAVA_TEST_POSTGRES_PORT:-45432} DYNAMODB_HOST_PORT=${JAVA_TEST_DYNAMODB_PORT:-48000}
  compose=(docker compose -p "$run_id" -f "$PWD/compose.yml")
  cleanup() {
    status=$?
    trap - EXIT
    # Remove only this test project's containers/network to avoid exhausting Docker address pools.
    # No --volumes: named data volumes remain available with the execution evidence.
    if "${compose[@]}" down --timeout 20 >> "$evidence/cleanup.log" 2>&1; then
      printf '{"containersStopped":true,"containersRemoved":true,"networkRemoved":true,"volumesRetained":true}\n' > "$evidence/cleanup.json"
    else
      printf '{"containersStopped":false,"volumesRetained":true}\n' > "$evidence/cleanup.json"
      status=1
    fi
    exit "$status"
  }
  trap cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  # Fresh unique project only. Do not recreate any developer/monitoring containers.
  "${compose[@]}" create > "$evidence/infra.log" 2>&1
  docker start "$run_id-dynamodb-data-init-1" >> "$evidence/infra.log"
  [[ $(docker wait "$run_id-dynamodb-data-init-1") == 0 ]]
  containers=()
  for service in kafka redis postgres dynamodb-local; do
    containers+=("$run_id-$service-1")
    docker start "$run_id-$service-1" >> "$evidence/infra.log"
  done
  ready=false
  for ((attempt=0; attempt<120; attempt++)); do
    states=$(docker inspect --format '{{.State.Status}} {{.State.Health.Status}}' "${containers[@]}")
    if [[ $(sort -u <<< "$states") == 'running healthy' ]]; then ready=true; break; fi
    sleep 1
  done
  [[ "$ready" == true ]] || { echo 'Integration infrastructure did not become healthy.' >&2; exit 1; }
  docker inspect --format '{{.Name}} {{.Config.Image}} {{.State.Health.Status}}' "${containers[@]}" > "$evidence/infra-health.txt"
  export DYNAMODB_TEST_ENDPOINT="http://localhost:$DYNAMODB_HOST_PORT"
  export KAFKA_TEST_BOOTSTRAP_SERVERS="localhost:$KAFKA_HOST_PORT"
  export POSTGRES_TEST_URL="jdbc:postgresql://localhost:$POSTGRES_HOST_PORT/delivery"
  export POSTGRES_TEST_USER=delivery POSTGRES_TEST_PASSWORD=delivery REDIS_TEST_PORT=$REDIS_HOST_PORT
  maven_command+=(-Pintegration-tests)
fi
# Clean eliminates stale Surefire reports; do not mix previous runs in the result count.
set +e
"${maven_command[@]}" > "$evidence/maven.log" 2>&1
maven_status=$?
"$java_bin" scripts/testing/JavaTestReport.java "$mode" > "$evidence/summary.json" 2> "$evidence/report-error.log"
report_status=$?
set -e
cat "$evidence/summary.json"
[[ $maven_status == 0 && $report_status == 0 ]] || { echo "Java verification failed: $evidence/maven.log" >&2; exit 1; }
echo "PASS Java $mode"
