# Java 중심 테스트와 외부 도구 사용 기준

2026-09-25 사용자 결정. 업무 기능·저장 상태·오류 분류·회귀 테스트는 **Java/JUnit 중심**으로 작성한다. 성능 부하 생성은 **k6**를 사용한다. 외부 도구를 쓸 때는 이름·역할·검증 범위를 결과에 명시한다.

## 1. 실행 역할

| 구분 | 도구 | 검증 대상 |
|---|---|---|
| 기능/단위 회귀 | Java 21, JUnit Jupiter, Mockito, Maven Wrapper/Surefire | 재시도·멱등성·만료·응답 분류와 서비스 로직 |
| 실제 저장소 통합 | 동일 Java/JUnit 테스트 + AWS SDK, Kafka client, JDBC 등 실제 클라이언트 | DynamoDB 경합/쓰기 횟수, Kafka 인계·DLT·offset, SQL 원자 저장·정리 |
| 통합 환경 실행 | **Bash + Docker Compose** (`scripts/test-java.sh`) | 격리된 Kafka·DynamoDB Local·PostgreSQL·Redis 준비·상태 확인·종료. 업무 판정은 Java 테스트가 수행 |
| 프로세스/서비스 장애 시나리오 | 기존 **Python 3 + Docker CLI/Compose + subprocess**, boto3·confluent-kafka·HTTP 클라이언트 | 실제 JAR 실행·SIGKILL·서비스 중단/복구·여러 저장소와 고객 수신 대조 |
| 성능 부하 생성 | **Grafana k6 1.8.1**, `scripts/poc/load.js` | 일정 유입률, 요청·오류·지연·dropped_iterations |
| 성능 실행 보조 | 기존 Python `실행.py`·`전체_흐름_부하.py` | k6 실행·프로세스 관리·지표 수집·입력/이력/고객 수신 대조. Python이 부하 발생기가 아님 |
| 관측 | Prometheus·Grafana·Loki·Alloy | 구간 지표·시계열·로그, 시험의 업무 정합성 판정을 대신하지 않음 |

기존 Python 장애 시험을 삭제하거나 Java 메서드 호출만으로 바꾸지 않는다. 프로세스 종료와 실제 HTTP 고객 수신을 검증하는 독립적인 역할이 있다. 신규 업무 판정 로직을 Python에 복제하지 않으며 관련 규칙은 Java 테스트에 먼저 추가한다. 환경 관리 필요만으로 Testcontainers를 새 의존성으로 추가하지는 않았다. 현재 실제 저장소 JUnit 테스트를 Docker Compose로 실행한다.

Python이 이 검증에 기술적으로 필수인 것은 아니다. Java의 `ProcessBuilder`와 Docker CLI로도 같은 격리 시험을 만들 수 있다. 현재 Python 도구는 실제 프로세스·컨테이너를 별도 시험 프로세스에서 중단하고 Kafka·SQL·DynamoDB·고객 수신을 대조하는 기존 실행기다. 단순히 JUnit 메서드 시험으로 교체하면 실제 종료 경계를 잃으므로, Java 이관 시에는 이 실행기의 환경 생성·소유권 확인·종료·증거 보관까지 같은 범위로 옮겨야 한다. 모니터링 수집기와 대시보드 생성기 역시 운영 도구여서 Java 업무 테스트만으로 대체되지 않는다. 남아 있는 Python 파일에는 한글 역할 이름을 붙였고 `bash scripts/검증-실행.sh 목록`에서 주요 시나리오를 찾을 수 있다.

## 2. 표준 실행

```sh
# JAVA_HOME을 JDK 21로 지정
bash scripts/test-java.sh unit
bash scripts/test-java.sh integration
```

- unit은 저장소 테스트 환경 변수를 지우고 `./mvnw clean verify`를 실행한다. 일부 실제 소켓 테스트도 포함하므로 순수 메서드 단위 시험만을 뜻하지 않는다. 외부 저장소 테스트는 미실행으로 따로 집계한다.
- integration은 고유 `java-test-...` Compose 프로젝트를 만들고 네 저장소를 준비한 뒤 `./mvnw clean verify -Pintegration-tests`를 실행한다. 업무 검증은 모두 `src/test/java`에 있다.
- integration profile은 필수 로컬 접속 설정이 없으면 Maven Enforcer로 실패한다. [공식 requireProperty 규칙](https://maven.apache.org/enforcer/enforcer-rules/requireProperty.html). 환경 누락으로 통합 테스트가 전부 건너뛰어졌는데 성공으로 해석하는 일을 막는다.
- JDK 표준 XML 파서로 Surefire 결과를 집계한다. 통합 실행에 skipped가 하나라도 있으면 실행 스크립트도 실패한다. Maven 실패와 집계 실패를 모두 검사한다.
- clean으로 이전 Surefire 결과를 없앤 뒤 실행한다. `.poc-results/<run>/`에 Java 버전·도구 역할·기준 커밋·Maven 로그·실행/미실행/실패/오류 집계·환경 종료 결과를 남긴다.
- 기본 포트는 Kafka 49092, Redis 46379, PostgreSQL 45432, DynamoDB 48000이다. 이미 사용 중이면 기존 서비스를 건드리지 않고 실패한다. `JAVA_TEST_KAFKA_PORT`, `JAVA_TEST_REDIS_PORT`, `JAVA_TEST_POSTGRES_PORT`, `JAVA_TEST_DYNAMODB_PORT`로 바꾼다. 동시 실행은 포트를 서로 다르게 지정한다.
- 종료 시 해당 실행이 소유한 컨테이너·네트워크는 `docker compose down`으로 제거하고 데이터 볼륨은 보존한다(`--volumes` 미사용). 이전의 stop만 하는 방식은 테스트 네트워크 누적으로 Docker 주소 풀이 소진돼 변경했다. 기존 개발/모니터링 프로젝트나 업무 토픽은 건드리지 않는다. 통합 실행은 Docker가 필요하며 서버/AZ 장애 보장을 대체하지 않는다.

## 3. k6 성능 기준

성능 발생기는 기존에도 k6였으며 앞으로도 이를 유지한다. [constant-arrival-rate 공식 설명](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/)에 따라 목표 유입률을 설정하되 VU 부족으로 발생한 dropped_iterations를 확인한다. 시나리오 iteration 1회에 메시지 요청 1회를 보낸다는 현재 스크립트 조건에서만 rate를 메시지 유입 TPS로 해석한다.

`POC_PREALLOCATED_VUS`·`POC_MAX_VUS`를 명시적으로 조절할 수 있게 했다. 목표 10,000 TPS를 그대로 초기 VU 10,000개로 사용하면서 최대 VU 1,000개로 설정되는 기존 모순을 제거했다. 현재 기본 상한은 용량 증명이 아니며, 실제 부하 시 생성기 자원·응답 지연을 보고 값을 정한다. 잘못된 수치나 최대 VU < 초기 VU는 시작 전에 거부한다.

k6 API 접수 수·응답 지연과 서비스의 최종 고객 수신 TPS는 다르다. 성능 결과에는 k6 버전·스크립트 해시·목표/실제 유입률·dropped_iterations, SQL/고객 수신/DDB 정리 대조와 적체를 함께 기록한다. **개발 완료 후** 설정·튜닝 → Pod별 지속 성능 → 전체 10,000 TPS 검증 순서는 유지한다. 이번에는 본격 부하를 실행하지 않는다.

## 4. Grafana

- 통합 관제: http://localhost:13000/d/delivery-overview
- 홈: http://localhost:13000

이 주소는 해당 개발 PC의 localhost다. 이번 확인에서 기존 Grafana가 중지돼 있어 동일 컨테이너·볼륨으로 재시작했고 health의 database=ok와 통합 관제 API HTTP 200을 확인했다. 모니터링 Kafka는 별도로 중단돼 있으므로 관련 서비스/패널이 모두 정상이라는 의미는 아니다. 기존 Kafka 데이터 이관 없이 전체 Compose를 재생성하지 않았다.

## 5. 실행 결과

격리 실행 `java-test-20260925140528-16402`에서 **Java/JUnit 301개 실행, 미실행 0, 실패/오류 0**, 전체 Maven clean verify를 통과했다. 이전 환경 없이 실행한 148개와는 실제 저장소 통합 실행 범위가 다르다. `.poc-results/` 원문과 [집계·도구·환경·k6 설정 근거](검증-결과/2026-09-25-Java-통합-301개와-k6-설정-검증.json)를 남겼고 소유 컨테이너는 정지했다.

k6 inspect로 목표 rate 10,000의 초기/최대 VU 설정이 유효한지 확인했고, 초기 10·최대 1의 잘못된 조합은 시작 전에 거부했다. HTTP 트래픽은 보내지 않았다.

## 6. 한글 도구 이름과 실행 메뉴 (2026-09-26)

저장소의 Python 파일 39개는 역할을 알 수 있는 한글 파일명으로 변경했다. `unittest discover`가 시험 파일을 찾도록 시험 파일에만 `test_` 접두어를 유지했다. 내부 import, Docker 수집기 이미지, Bash 명령과 문서의 실행 경로도 함께 변경했다. 주요 시나리오는 `bash scripts/검증-실행.sh 목록`으로 확인하고, `bash scripts/검증-실행.sh 준비`로 의존성을 설치한 뒤 `bash scripts/검증-실행.sh 고객-통지중-종료`처럼 실행한다.

한글 경로에서 Python 시험 60개가 통과했고, DynamoDB 마이그레이션 시험 3개는 별도 환경이 없어 기존 조건대로 건너뛰었다. 모든 PoC 진입점의 `--help`를 확인했으며 고객 통지 중 SIGTERM 시험을 실제 Docker·JVM으로 재실행해 같은 묶음의 2회 수신과 최종 완료를 확인했다. [검증 기록](검증-결과/2026-09-26-한글-Python-실행경로-검증.json). 이 파일명 변경이 전체 성능 시험이나 모든 장애 시나리오의 재실행을 뜻하지는 않는다.
