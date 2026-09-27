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

다음 묶음에서 모니터링 JAR 복사 도구를 `scripts/testing/MonitorArtifacts.java`로 옮기고 Python 파일을 제거했다. Java 표준 라이브러리만으로 소스 JAR의 SHA-256을 계산해 해시별 경로에 보관하고 복사본을 다시 검사한다. Grafana 관리자 토큰도 같은 도구의 32바이트 `SecureRandom` 값으로 생성한다. 네 개의 임시 JAR로 보관·재실행·환경 파일·토큰 길이를 검증했다. 따라서 현재 남은 Python 파일은 38개다.

로그 오류 판정도 `scripts/testing/LogHealth.java`로 옮겼다. 모니터링 성능 실행기는 저장한 서비스 로그를 Java 판정기의 표준 입력으로 보내고 JSON 결과를 읽는다. 이전 Python 구현과 정상·오류·20줄 샘플 제한을 포함한 5개 입력의 결과가 일치했고 Java 자체 검증도 통과했다. Python 판정기와 전용 Python 시험 파일을 제거했으므로 현재 남은 Python 파일은 **36개**다. `bash scripts/검증-실행.sh 도구-테스트 -q`는 Python 도구 시험과 Java 로그 판정을 함께 실행한다.

보존 성능 자료의 정체 분석은 `verification-tools` Java 모듈로 옮겼다. `bash scripts/검증-실행.sh 정체-분석 <측정 디렉터리>`가 도구를 빌드하고 저장된 JSONL·gzip 지표·audit 로그만 읽어 보고서를 출력한다. 2026-09-24의 100 TPS 자료에서 Python과 Java의 **29개 구간 전체 JSON 결과가 정확히 일치**했고 표준 Java 단위 검증 190개도 통과했다. [대조 기록](검증-결과/2026-09-26-Java-정체-분석-이관-검증.json). 기존 Python 분석 파일을 제거해 현재 남은 Python 파일은 **35개**다.

Grafana·Prometheus·Loki 읽기 검증도 `verification-tools`의 `monitoring` 명령으로 옮겼다. `bash scripts/monitoring.sh verify`와 한글 실행 메뉴가 이를 호출한다. 같은 로컬 HTTP fixture를 Python·Java에 제공했을 때 정상 결과(질의 2건·오류 0건)와 대시보드 불일치·민감 로그 결과(오류 2건)가 일치했고 표준 Java 단위 검증 191개도 통과했다. 실제 Grafana는 중지돼 있어 이번 이관 검증에서 운영 스택 전체를 조회하지 않았다. [이관 검증 기록](검증-결과/2026-09-26-Java-모니터링-검증-이관.json). 남은 Python 파일은 **34개**다.

모니터링 데모 요청 생성기도 `verification-tools`의 `demo` 명령으로 옮겼다. `bash scripts/monitoring.sh demo --rate 20 --seconds 30` 또는 `bash scripts/검증-실행.sh 모니터링-데모`로 실행한다. Java 도구는 기존의 단독 실행 잠금, 초당 요청 간격, 10번째 요청의 멱등성 키 재사용, 선택적 강제 실패 1건과 JSON 요약을 유지한다. 로컬 HTTP fixture로 요청 10건·고유 ID 9개·강제 실패 1건과 잠금 충돌을 검증했다. 실제 모니터링 스택에 트래픽을 보내는 시험은 수행하지 않았다. 남은 Python 파일은 **33개**다.

PoC 환경 준비는 `scripts/testing/PocSetup.java`로 옮겼다. `bash scripts/검증-실행.sh 준비`가 Python 가상환경과 고정 버전 의존성을 준비하고, k6 체크섬 목록과 압축 파일의 SHA-256을 확인한 뒤 실행 파일만 설치한다. `bash scripts/검증-실행.sh 준비 self-test`로 임시 ZIP/TAR 추출과 체크섬 불일치 거부를 검증했다. 실제 다운로드와 의존성 재설치는 이번 이관 검증에서 실행하지 않았다. 환경 준비 단계가 Python 스크립트를 필요로 하지는 않지만, 남은 PoC 시험 실행에는 Python 가상환경이 필요하다. 기존 Python 준비 파일을 제거해 남은 Python 파일은 **32개**다.

Grafana 대시보드는 배포에 사용되는 JSON 7개를 편집 원본으로 삼는다. 이전 Python 생성기를 다시 실행해도 커밋된 JSON에 변경이 없음을 확인한 뒤 생성기를 제거했다. `bash scripts/검증-실행.sh 대시보드-검증`은 Java에서 파일 목록, UID, 패널 ID·격자, 데이터소스·질의·링크를 검사한다. 현재 JSON 7개와 잘못된 fixture에 대한 Java 검증을 통과했다. 남은 Python 파일은 **31개**다.

로컬 API→Kafka→업체 점검 도구를 `verification-tools`의 `local-smoke` 명령으로 옮겼다. 기존 `bash scripts/local.sh smoke [--metrics]`와 한글 실행 메뉴는 Java 도구를 호출한다. 인증·멱등성 키 재전송·ORIGIN/STEP ACCEPTED 상태·업체 처리 시각을 확인하고, 선택적으로 관리 포트 인증·단계/DB 지표·외부 업체의 실제 호출 1회를 대조한다. 로컬 HTTP fixture와 상태 fixture로 전체 판정 경로를 확인했다. 실제 실행 중인 Kafka·DynamoDB·앱을 대상으로 한 smoke는 이번 이관에서 실행하지 않았다. 남은 Python 파일은 **30개**다.

수명주기 GSI 보정 도구도 `verification-tools`의 `lifecycle-index` 명령으로 옮겼다. `bash scripts/검증-실행.sh DDB-인덱스`는 기본값에서 ORIGIN·STEP의 인덱스만 조회하고, 명시적 `--apply`에서만 GSI 생성과 지정한 UUID의 조건부 보강을 수행한다. Java 테스트에서 로컬 URL 제한, 조회 전용 경로, 명시한 ID만 질의·보강하는 경로와 상태별 조건식을 확인했다. 실제 DynamoDB에 대한 `--apply`는 실행하지 않았다. 남은 Python 파일은 **29개**다.

원본·단계 테이블 분리 도구도 `verification-tools`의 `split-table` 명령으로 옮겼다. `bash scripts/검증-실행.sh DDB-분리`는 기본값에서 원본을 읽고 대상의 누락·충돌을 확인하며, `--apply --writers-stopped`가 함께 있을 때만 조건부 복사와 사후 대조를 수행한다. Python 통합 시험 3개는 Java 통합 시험으로 옮겼다. 모의 DB 시험과 로컬 DynamoDB의 고유 임시 원본·테스트 키를 사용한 통합 시험 3개에서 dry-run·반복·충돌·응답 유실 재개를 확인했다. 기존 `delivery_state` 데이터는 복사하지 않았다. Python 이관 파일과 전용 시험 파일을 제거해 남은 Python 파일은 **27개**다.

모니터링 collector의 Kafka offset·적체 지표와 API 관리 지표 relay를 `verification-tools`의 `collector` 명령으로 옮겼다. 새 Docker 이미지는 Java 21 JAR를 실행하며, 성능 측정기는 컨테이너 안의 `collector-inside` 명령으로 지표·업체 횟수를 조회한다. 이미 실행 중인 이전 Python collector에는 측정기의 호환 경로가 남아 있다. Java fixture에서 401 토큰 재발급과 오래된 적체 지표 숨김을 확인했고, 새 이미지를 빌드해 일회용 컨테이너의 `/health`와 Kafka 미연결 시 `probe_up=0`을 확인했다. 실행 중인 모니터링 스택은 교체하지 않았으며 실제 Kafka offset 대조는 이번에 실행하지 않았다. Python 수집기와 전용 시험을 제거해 남은 Python 파일은 **25개**다.

전체 흐름 부하 측정의 입력·SQL 이력·고객 수신 결과 대조와 5단계 지연시간 계산은 `verification-tools`의 `full-flow-reconcile` 명령으로 옮겼다. Python 부하 실행기는 시작 시 Java 도구를 빌드하고 각 구간의 증거 JSON을 표준 입력으로 전달한다. `bash scripts/검증-실행.sh 전체흐름-대조 < evidence.json`으로 판정만 따로 재실행할 수 있다. 기존 Python 판정 시험 7개 시나리오를 Java 시험으로 옮겼고, Python 시험 파일을 제거해 남은 Python 파일은 **24개**다. 실제 부하 실행은 별도 환경 점검 후 수행한다.

격리형 성능 PoC의 요청별 HTTP·Kafka·DynamoDB·업체 호출 증거 대조는 `verification-tools`의 `poc-reconcile` 명령으로 옮겼다. Python 실행기는 실제 시스템에서 증거를 수집하고 Java 판정기에 표준 입력으로 전달한다. `bash scripts/검증-실행.sh PoC-대조 < evidence.json`으로 판정을 재실행할 수 있다. 기존 Python 시험 10개가 Java 판정기 호출 경로에서 통과하며, Java 직접 시험도 추가했다. Kafka·DB 조회와 입력 기록 읽기는 아직 Python에 있어 Python 파일 수는 **24개**다. 실제 성능 부하 측정은 이번 이관에서 수행하지 않았다.

두 부하 실행기가 공유하는 k6 JSONL 입력 기록 검사와 완료 판정도 Java의 `manifest`, `input-complete` 명령으로 옮겼다. `bash scripts/검증-실행.sh 입력기록 <requests.jsonl>`은 시작·결과 이벤트를 대조해 JSON으로 출력하고, `bash scripts/검증-실행.sh 입력완료 <계획> <시작> <응답> <반복> <누락>`은 경계 도착 요청 1건을 포함한 완료 여부를 출력한다. Python 실행기는 Java 결과를 받아 후속 증거와 연결한다. 중복·고아 결과·키 불일치·미응답 입력과 경계값을 Java 단위 시험으로 검증했다. Kafka·DB 조회가 남아 있어 Python 파일 수는 **24개**다.

격리형 PoC의 ORIGIN·STEP 증거 조회는 `verification-tools`의 `poc-items` 명령으로 옮겼다. `bash scripts/검증-실행.sh PoC-항목 http://localhost:28000 < deliveries.json`은 전달된 요청 ID의 META를 먼저 읽고 실제 실행 ID에 해당하는 ATTEMPT를 읽어 DynamoDB 항목 JSON을 출력한다. 두 테이블 모두 일관 읽기와 필요한 속성만의 투영을 사용하며, 100키 단위 분할과 미처리 키 재시도를 유지한다. 모의 클라이언트에서 실행 ID 매핑·읽기 조건·재시도·분할을 검증했고, 실행 중인 로컬 DynamoDB의 ORIGIN·STEP에 존재하지 않는 ID를 읽어 빈 결과를 확인했다. 실제 항목이 있는 경로는 이번 이관에서 조회하지 않았다. Python 파일 수는 **24개**다.

PoC와 모니터링 성능 실행기의 Kafka 증거 수집도 `verification-tools`의 장기 실행 `poc-kafka` 명령으로 옮겼다. Python 실행기는 시작할 때 Java 소비자를 한 번 띄우고 JSONL 요청으로 snapshot·records를 받아 측정 중 연결을 유지한다. Java 시험은 소비자 그룹 offset·미초기화 그룹·DLT의 미정의 lag, 요청 전후 구간 읽기, 만료된 증거와 offset 간격 거부를 검증한다. 이관 커밋 당시에는 Kafka 브로커를 대상으로 한 실제 증거 수집을 수행하지 않았다. Python 파일 수는 **24개**다.

2026-09-27에는 Kafka probe 이관 후 실제 API→Kafka→DynamoDB→업체 smoke를 고유 Compose 프로젝트에서 재실행했다. 기존 `platform-poc`은 과거 Kafka 부모 경로 마운트를 가진 채 정지돼 있어 데이터 보호 검사에서 재시작을 거부했다. `--project platform-poc-smoke-20260927a`로 신규 볼륨을 분리해 5 TPS × 2초의 고유·중복 두 구간을 실행했고, 각각 입력 11/10건·고유 11/5건·문제 0건·누락 응답 0건으로 통과했다. 새 프로젝트 컨테이너도 종료했고 기존 컨테이너·볼륨은 변경하지 않았다. 이는 고객 최종 수신·정리나 지속 성능을 측정한 결과가 아니다. [실행 근거](검증-결과/2026-09-27-Java-Kafka-PoC-smoke.json).

이어서 고유 프로젝트의 전체 흐름 부하를 10 TPS × 2초로 재실행했다. 첫 시도는 Docker 기본 네트워크 주소 풀 부족으로 Compose 생성 전에 중단됐고, 미시작 k6 프로세스의 종료 처리에서 원래 오류를 가리던 문제를 수정했다. 이번 세션에서 만든 smoke 프로젝트의 종료된 컨테이너·네트워크만 제거하고 데이터 볼륨은 보존한 뒤 재시도했다. 요청 20건 전부 SQL 저장·고객 수신·정리를 마쳤고 업체 호출은 각 1회, 최종 Kafka lag와 Redis 대기는 0이며 ORIGIN·STEP 잔여 항목도 없었다. 소유 컨테이너·네트워크는 제거하고 볼륨·증거를 보존했다. 2초 입력의 소규모 회귀이며 지속 TPS의 증거로 해석하지 않는다. [전체 흐름 근거](검증-결과/2026-09-27-Java-전체흐름-smoke.json).

한글 경로에서 Python 시험 60개가 통과했고, DynamoDB 마이그레이션 시험 3개는 별도 환경이 없어 기존 조건대로 건너뛰었다. 모든 PoC 진입점의 `--help`를 확인했으며 고객 통지 중 SIGTERM 시험을 실제 Docker·JVM으로 재실행해 같은 묶음의 2회 수신과 최종 완료를 확인했다. [검증 기록](검증-결과/2026-09-26-한글-Python-실행경로-검증.json). 이 파일명 변경이 전체 성능 시험이나 모든 장애 시나리오의 재실행을 뜻하지는 않는다.

이관 후 표준 `Java-통합` 실행에서 Java/JUnit **461개 실행, 미실행·실패·오류 0개**를 확인했다. Kafka·Redis·PostgreSQL·DynamoDB Local을 실제로 연결했고, 고유 Compose 프로젝트의 컨테이너·네트워크만 제거하고 볼륨과 로그를 보존했다. 이는 저장소 통합 회귀이며 프로세스 장애 시나리오나 지속 부하의 재실행은 아니다. [통합 회귀 근거](검증-결과/2026-09-27-Java-통합-461개-회귀-검증.json).

## 7. 남은 Python 이관 목록 (2026-09-27)

현재 Python 파일 **24개**는 시나리오 실행기 16개, 실행기 시험 7개, Java 도구 연결 파일 `검증_근거.py` 1개다. 실행기 16개는 `실행.py`·`전체_흐름.py`·`전체_흐름_부하.py`·`성능_측정.py`의 환경/부하 관리 4개와, `수신결과_흐름.py`·`수명주기_흐름.py`·`프로세스_복구.py`·`정상종료_복구.py`·`업체_호출중_정상종료_복구.py`·`이차_TCP_정상종료_복구.py`·`고객_통지_정상종료_복구.py`·`카프카_복구.py`·`카프카_복제_재연결.py`·`레디스_복구.py`·`포스트그레스_복구.py`·`다이나모DB_복구.py`의 실제 흐름/장애 실행 12개다. 일곱 `test_*.py` 파일은 이 실행기의 환경 소유권·증거 판정을 시험한다. 파일 수가 그대로여도 판정 로직을 Java로 옮기는 중이므로 이관 완료와 혼동하지 않는다.

우선 순서는 남은 Python 증거 판정과 시험을 Java로 이전한 뒤, 공통 Docker/JVM 실행기와 각 장애 시나리오를 실제 종료·재시작 경계까지 Java로 이전하고, 마지막에 k6/모니터링 실행기를 정리하는 것이다. 이번 묶음에서는 Kafka 중단 중 만료 결과가 SQL·고객에게 조기 인계되지 않고 원래 기한·업체 효과를 보존하는 판정을 `kafka-expiry` Java 명령으로 옮겼다. Python 실행기는 수집한 JSON을 전달한다. Docker 소유권 확인과 실제 브로커 중단·재시작은 아직 Python에 있다.

Java 판정 시험 3개와 남은 Python 도구 시험 45개를 통과했고, Python→Java 호출에서 정상 증거는 수락·조기 SQL 저장 증거는 거부됨을 확인했다. `카프카 --help` 실행 경로도 확인했다. 실제 브로커를 중단하는 시나리오는 이번 묶음에서 재실행하지 않았다.

다음으로 Redis 중단 중에도 SQL 결과와 고객 204 수신·정리가 완료되고 업체 효과는 1회인지를 확인하는 판정을 `redis-complete` Java 명령으로 옮겼다. Kafka 판정도 공통 Python→Java 증거 호출 경로를 사용한다. Java 판정 시험 3개와 남은 Python 도구 시험 43개가 통과했고, 두 명령 모두 Python 연결에서 정상·불일치 증거를 구별했다. `레디스 --help` 실행 경로를 확인했으며 실제 Redis 중단 시나리오는 이번 묶음에서 재실행하지 않았다. Python 파일 수는 **24개**로, 실행기 이관은 계속 필요하다.

PostgreSQL 중단 중의 SQL probe 실패·JVM PID 유지·결과 저장 실패 계측·Kafka offset 보존과 lag·ORIGIN/STEP 최종 결과·업체 효과·고객 조기 전달 금지 판정은 `postgres-outage` Java 명령으로 옮겼다. Python 실행기는 중단/복구와 증거 수집을 계속 담당한다. Java 판정 시험 3개와 남은 Python 도구 시험 40개가 통과했고, Python 연결에서 정상 증거와 조기 고객 수신 증거를 구별했다. 실제 PostgreSQL 중단 시나리오는 이번 묶음에서 재실행하지 않았으며 Python 파일은 **24개**다.
