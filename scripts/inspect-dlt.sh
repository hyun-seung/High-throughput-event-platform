#!/usr/bin/env bash
# Java read-only DLT triage. External tool: Bash only launches the packaged Java entry point.
set -euo pipefail
cd "$(dirname "$0")/.."
jar=delivery-ingress-worker/target/delivery-ingress-worker-1.0-SNAPSHOT.jar
[[ -f "$jar" ]] || { echo 'Build first: ./mvnw package' >&2; exit 1; }
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" -Dloader.main=event.delivery.ingress.dlt.DltInspector \
  -Dlogback.configurationFile=scripts/testing/dlt-logback.xml \
  -cp "$jar" org.springframework.boot.loader.launch.PropertiesLauncher "$@"
