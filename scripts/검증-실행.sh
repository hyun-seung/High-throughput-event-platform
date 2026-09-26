#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat <<'EOF'
사용법: bash scripts/검증-실행.sh <이름> [추가 옵션]

준비/검증: 준비, 도구-테스트, Java-단위, Java-통합
실제 JVM 종료: 강제종료, 정상종료, 1차-호출중-종료, 2차-호출중-종료, 고객-통지중-종료
전체 흐름: 전체흐름, 전체흐름-부하, 수신결과, 수명주기
저장소 장애: 카프카, 카프카-복제, 레디스, 포스트그레스, 다이나모DB
성능 PoC: 성능-PoC
그 밖의 도구: 로컬-점검, DDB-인덱스, DDB-분리, 모니터링-데모, 모니터링-측정,
             모니터링-검증(Java), 대시보드-검증(Java), 정체-분석(Java), 실행파일-보관(Java)

준비와 실제 JVM 시험에는 JDK 21이 필요하고, 실제 JVM 시험에는 Docker도 필요합니다.
추가 옵션은 해당 도구에 그대로 전달합니다.
EOF
}

scenario="${1:-}"
[[ -n "$scenario" ]] || { usage; exit 2; }
shift

if command -v /usr/libexec/java_home >/dev/null 2>&1; then
  JAVA_HOME="$(/usr/libexec/java_home -v 21)"
  export JAVA_HOME
fi

case "$scenario" in
  도움말|목록|-h|--help) usage; exit 0 ;;
  준비) exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" scripts/testing/PocSetup.java "$@" ;;
  Java-단위) exec bash scripts/test-java.sh unit "$@" ;;
  Java-통합) exec bash scripts/test-java.sh integration "$@" ;;
  정체-분석|모니터링-검증|모니터링-데모|대시보드-검증|로컬-점검)
    ./mvnw -q -pl verification-tools -am -DskipTests package
    java_bin=java
    [[ -z "${JAVA_HOME:-}" ]] || java_bin="$JAVA_HOME/bin/java"
    command=stall
    [[ "$scenario" != 모니터링-검증 ]] || command=monitoring
    [[ "$scenario" != 모니터링-데모 ]] || command=demo
    [[ "$scenario" != 대시보드-검증 ]] || command=dashboards
    [[ "$scenario" != 로컬-점검 ]] || command=local-smoke
    exec "$java_bin" -jar verification-tools/target/verification-tools-1.0-SNAPSHOT.jar "$command" "$@" ;;
esac

python_bin=.poc-tools/venv/bin/python
[[ -x "$python_bin" ]] || { echo "먼저 bash scripts/검증-실행.sh 준비 를 실행하세요." >&2; exit 1; }

case "$scenario" in
  도구-테스트|파이썬-테스트)
    "$python_bin" -m unittest discover -s scripts/poc -p 'test_*.py' "$@"
    java scripts/testing/LogHealth.java self-test
    "$python_bin" -m unittest discover -s monitoring/collector -p 'test_*.py' "$@"
    exec "$python_bin" -m unittest discover -s scripts/dynamodb -p 'test_*.py' "$@" ;;
  강제종료) script=scripts/poc/프로세스_복구.py ;;
  정상종료) script=scripts/poc/정상종료_복구.py ;;
  1차-호출중-종료) script=scripts/poc/업체_호출중_정상종료_복구.py ;;
  2차-호출중-종료) script=scripts/poc/이차_TCP_정상종료_복구.py ;;
  고객-통지중-종료) script=scripts/poc/고객_통지_정상종료_복구.py ;;
  전체흐름) script=scripts/poc/전체_흐름.py ;;
  전체흐름-부하) script=scripts/poc/전체_흐름_부하.py ;;
  수신결과) script=scripts/poc/수신결과_흐름.py ;;
  수명주기) script=scripts/poc/수명주기_흐름.py ;;
  카프카) script=scripts/poc/카프카_복구.py ;;
  카프카-복제) script=scripts/poc/카프카_복제_재연결.py ;;
  레디스) script=scripts/poc/레디스_복구.py ;;
  포스트그레스) script=scripts/poc/포스트그레스_복구.py ;;
  다이나모DB) script=scripts/poc/다이나모DB_복구.py ;;
  성능-PoC) script=scripts/poc/실행.py ;;
  DDB-인덱스) script=scripts/dynamodb/수명주기_인덱스.py ;;
  DDB-분리) script=scripts/dynamodb/원본_단계_분리.py ;;
  모니터링-측정) script=scripts/monitoring/성능_측정.py ;;
  실행파일-보관) exec java scripts/testing/MonitorArtifacts.java snapshot ;;
  *) usage >&2; exit 2 ;;
esac

exec "$python_bin" "$script" "$@"
