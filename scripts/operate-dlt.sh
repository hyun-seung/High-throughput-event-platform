#!/usr/bin/env bash
# External tool: Bash launches Java. held/status read metadata; recheck durably queues a reviewed record.
set -euo pipefail
cd "$(dirname "$0")/.."
jar=delivery-ingress-worker/target/delivery-ingress-worker-1.0-SNAPSHOT.jar
[[ -f "$jar" ]] || { echo 'Build first: ./mvnw package' >&2; exit 1; }
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" -Dloader.main=event.delivery.ingress.dlt.DltOperationsCli \
  -Dlogback.configurationFile=scripts/testing/dlt-logback.xml \
  -cp "$jar" org.springframework.boot.loader.launch.PropertiesLauncher "$@"
