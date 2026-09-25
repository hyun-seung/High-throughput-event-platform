#!/usr/bin/env bash
# Read-only preflight for an existing Compose Kafka container. Never migrate or remove data.
set -euo pipefail
containers=$("$@" ps -aq kafka)
while IFS= read -r container; do
  [[ -n "$container" ]] || continue
  parent_volume=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/var/lib/kafka"}}{{.Name}}{{end}}{{end}}' "$container")
  if [[ -n "$parent_volume" ]]; then
    echo 'Kafka 데이터 경로가 이전 마운트 구조입니다. 재생성 전에 실제 /var/lib/kafka/data 볼륨을 확인·백업·이관해야 합니다.' >&2
    echo '기존 컨테이너와 볼륨은 변경하지 않았습니다. docs/47-Kafka-장애-시-접수-보류와-결과-복구-검증.md의 이관 절차를 참고하세요.' >&2
    exit 1
  fi
done <<< "$containers"
