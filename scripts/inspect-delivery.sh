#!/usr/bin/env bash
# External tool: Bash launches a read-only Java operations CLI. No application workers are started.
set -euo pipefail
cd "$(dirname "$0")/.."
jar=delivery-result-worker/target/delivery-result-worker-1.0-SNAPSHOT.jar
[[ -f "$jar" ]] || { echo 'Build first: ./mvnw package' >&2; exit 1; }
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" -Dloader.main=event.delivery.result.operations.DeliveryOperationsCli \
  -Dlogback.configurationFile=scripts/testing/dlt-logback.xml \
  -cp "$jar" org.springframework.boot.loader.launch.PropertiesLauncher "$@"
