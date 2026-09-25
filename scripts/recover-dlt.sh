#!/usr/bin/env bash
# External tool: Bash launches Java. 'plan' is read-only; apply/resume are explicit operations.
set -euo pipefail
cd "$(dirname "$0")/.."
jar=delivery-ingress-worker/target/delivery-ingress-worker-1.0-SNAPSHOT.jar
[[ -f "$jar" ]] || { echo 'Build first: ./mvnw package' >&2; exit 1; }
exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" -Dloader.main=event.delivery.ingress.dlt.DltRecoveryCli \
  -Dlogback.configurationFile=scripts/testing/dlt-logback.xml \
  -cp "$jar" org.springframework.boot.loader.launch.PropertiesLauncher "$@"
