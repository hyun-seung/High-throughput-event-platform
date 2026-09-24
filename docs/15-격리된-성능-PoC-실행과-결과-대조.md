# 격리된 성능 PoC 실행과 결과 대조

작성일: 2026-09-24. [1차 검증 계획](13-1차-성능-PoC-구간별-검증-계획.md)의 API→Kafka→Ingress→Dispatch→HTTP 업체 접수 저장 경로를 실행하는 도구다. B·C 구간의 직접 입력, TCP·최종 웹훅·고객 통지 부하는 아직 지원하지 않는다.

## 실행 방법

JDK 21, Python 3.11 이상, Docker Compose, curl이 필요하다. macOS/Linux의 arm64/amd64를 지원한다. 실제 검증 환경은 macOS arm64·Python 3.14다. 저장소 루트에서 실행한다.

```bash
# 공식 k6 1.8.1 다운로드·체크섬 확인, 전용 Python 가상환경 설치
python3 scripts/poc/setup.py

# JDK 21의 JAVA_HOME 설정 후 현재 앱 빌드
./mvnw package

# 대조 로직 회귀 시험
.poc-tools/venv/bin/python -m unittest discover -s scripts/poc -p 'test_*.py' -v

# 고유 요청 및 중복 요청 소량 검증
.poc-tools/venv/bin/python scripts/poc/run.py --suite smoke

# 소량 검증 → 10 TPS 1분 워밍업 → 10/50/100 TPS 각 3분
.poc-tools/venv/bin/python scripts/poc/run.py --suite baseline

# 동일 조건 재측정 → 두 Worker 동시성만 변경 → Ingress linger만 변경
# 각 실행은 소량 검증, 10 TPS 1분 워밍업, 100 TPS 20초 중복 입력,
# 50/100 TPS 각 3분을 포함한다. 실패하면 해당 실행의 후속 단계만 중단한다.
.poc-tools/venv/bin/python scripts/poc/run.py --suite comparison --worker-concurrency 1 --ingress-linger-ms 5
.poc-tools/venv/bin/python scripts/poc/run.py --suite comparison --worker-concurrency 3 --ingress-linger-ms 5
.poc-tools/venv/bin/python scripts/poc/run.py --suite comparison --worker-concurrency 3 --ingress-linger-ms 0
```

`--delay-ms 50` 또는 `200`으로 시뮬레이터의 처리 후 응답 지연을 바꿀 수 있다. 앱이 받는 값은 `0~60000ms`이며 실제 발송 read timeout보다 크면 정상 기준선이 실패할 수 있다. 적체 해소 관찰은 기본 300초, `--drain-seconds 2~300`으로 변경한다. 시작 조건이 맞지 않거나 어느 단계가 실패하면 이후 부하 증가는 중단한다.

k6는 공식 릴리스의 checksum manifest 자체와 플랫폼별 압축 파일을 SHA-256으로 검증한다. Python 의존성은 전이 의존성까지 `requirements.txt`에 버전을 고정한다. 실행 환경에는 실제 설치 버전도 기록한다. 도구는 `.poc-tools/`, 결과는 `.poc-results/`에 두며 Git에는 넣지 않는다.

## 시험 환경과 기존 데이터 격리

`platform-poc` Compose 프로젝트의 별도 컨테이너·named volume을 쓴다. `.env`의 개발 프로젝트 설정 대신 다음 포트를 명시적으로 전달한다. 같은 프로젝트를 동시에 실행하는 것은 파일 lock으로 막는다. 앱 포트가 사용 중이면 기존 프로세스를 종료하지 않고 실패한다.

| 대상 | 시험 포트 |
|---|---:|
| Kafka / PostgreSQL / Redis / DynamoDB Local | 29092 / 25432 / 26379 / 28000 |
| API / Ingress HTTP / Dispatch HTTP / 시뮬레이터 | 28080 / 28081 / 28082 / 28090 |
| API / Ingress / Dispatch / 시뮬레이터 관리 | 29080 / 29081 / 29082 / 29090 |

각 앱은 한 개, Kafka 파티션 3개·복제 1, PoC 도구의 Worker consumer 동시성 기본값은 기준선 재현용 1이며 JVM heap은 앱당 `-Xms128m -Xmx512m`이다. 컨테이너 자원 제한은 Compose 기본값이며 호스트 자원을 공유한다. 이 로컬 결과는 운영 AWS DynamoDB·다중 AZ 내구성이나 서버별 독립 성능을 의미하지 않는다.

`--worker-concurrency 1~3`은 두 Worker의 `INGRESS_CONCURRENCY`·`DISPATCH_CONCURRENCY`에 적용한다. `--ingress-linger-ms 0|5`는 Ingress Producer의 `INGRESS_KAFKA_LINGER_MS`만 바꾼다. API Producer 설정은 유지한다. PoC 도구 기본값은 기존 조건인 1/5이며 실제 선택 값은 `environment.json`에 남긴다. 파티션별 순차 처리, RECORD ack, 후속 발행 ack 대기, DynamoDB 조건부 저장은 유지한다. 앱 시작 로그의 consumer별 partition 할당과 Kafka 지표로 설정 적용을 확인한다. 앱 자체의 동시성 기본값은 후속 시험으로 검증한 3이므로, 앱 기본값과 기준선 재현용 PoC 기본값을 구분한다.

동시성 3은 listener container 세 개로 파티션을 나눠 처리하는 설정이다. 같은 key는 같은 partition에서 처리되며, 동시성 자체가 외부 멱등성을 대신하지 않는다. [Spring Kafka 동시 처리 공식 문서](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html). `linger.ms`는 작은 배치를 모으는 대기 상한이다. 0이 모든 부하에서 유리하다고 가정하지 않고 순차 ack 대기 구조에서 비교한다. [Kafka Producer 공식 문서](https://kafka.apache.org/40/configuration/producer-configs/).

시험 DB에만 `local-user`를 준비한다. 해당 시험 고객의 Redis 정책은 TPS 2000·burst 2000·월 Quota 1,000,000으로 설정하고 전후 값을 저장한다. 인증과 제한 기능은 켜둔다. 종료 시 정책 필드를 원복하며 사용량 counter는 보존한다. 강제 종료로 원복하지 못했으면 `policy.json`과 `policy-restored.json`을 확인한다. 운영 고객 정책은 수정하지 않는다.

앱 프로세스와 시험 컨테이너는 종료 시 멈추지만 원본·offset·DB·volume은 삭제하지 않는다. 다음 실행은 기존 적체가 0인지 확인한다. 자동 offset reset이나 기존 완료 상태 삭제는 하지 않는다. 결과 미기록 상태의 강제 재발송도 하지 않는다.

macOS에서는 runner 실행 중에만 `caffeinate -i`로 유휴 절전을 방지한다. 시스템 설정은 영구 변경하지 않는다. 30초 이상 걸리는 지표 수집이나 wall clock/단조 시계 불연속은 측정 실패로 남긴다. 노트북 강제 절전·Docker 정지 등으로 중단된 실행은 정상 성능 수치로 사용하지 않는다.

## 부하와 요청 명세

k6의 `constant-arrival-rate`를 사용해 느린 응답이 입력률을 자동으로 줄이지 않게 한다. VU 부족으로 생성하지 못한 요청은 `dropped_iterations`로 실패 판정한다. [k6 일정 도착률 실행기](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/).

본문은 고정 1,009byte JSON, 고객 1개, HTTP 1차 접수 경로다. 결과 조회·2차·최종 웹훅은 호출하지 않는다. UUID 실행 ID와 단계명·iteration으로 key를 만들며 중복 시험은 연속된 두 iteration에 같은 key를 사용한다. 시뮬레이터 중복 제거는 항상 끈다. 시뮬레이터 추적 한도는 실행당 100,000 key이며 현재 baseline의 입력은 한도 이내다.

HTTP 호출 직전에 `start`, 완료 후 `result`를 JSONL로 남긴다. key·iteration·시각·응답 상태·반환 deliveryId·HTTP 지연만 저장하고 JWT·본문은 기록하지 않는다. 503·응답 미수신도 제외하지 않는다. 종료 경계에서 k6가 1건을 추가 실행할 수 있어 `계획 건수~계획+1건`을 허용하되 모든 시작·응답·k6 완료 횟수가 일치해야 한다. 부족한 입력·미응답·추가 2건 이상·dropped iteration은 실패다. 실제 건수는 그대로 보고한다.

## 수집과 정합성 판정

대략 5초 간격으로 네 앱의 Prometheus 자료와 Kafka offset을 읽는다. 수집 시간이 더해지므로 실제 간격은 timestamp를 사용한다. 앱·부하 생성기·수집기의 CPU/RSS/VSZ를 기록하고 두 번 중 한 번은 Docker 자원도 기록한다. 이 수집 비용과 요청별 로그 출력도 같은 호스트의 시험 부하에 포함된다.

Kafka lag는 끝 offset에서 **기존 Worker group의 committed offset**을 빼서 계산한다. fetch 위치 기반 client 지표를 대신 쓰지 않는다. 커밋이 없는 새 partition은 현재 설정인 earliest 기준 log 시작 offset을 쓴다. 보관 범위 밖 commit은 오류이며 0으로 보정하지 않는다. 미커밋이 있으면 해당 첫 record의 Kafka timestamp로 대기 시간을 추정한다. 이는 처리 완료 시간이나 메시지 업무 만료가 아니다.

조회 consumer는 subscribe·commit하지 않고 자동 commit/offset 저장을 끈다. 별도 수동 assign consumer로 증거만 읽는다. [Confluent Python Consumer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#consumer).

부하 종료 후 두 번 연속 lag=0을 관찰하고 실행 전후 offset 범위의 접수·Dispatch·접수 DLT 레코드를 수집한다. 최대 5분이 지나도 적체가 남으면 실패다. peak lag는 샘플에서 본 최댓값이며 샘플 사이 순간 최댓값은 놓칠 수 있다. 해소 관찰 시간은 실제 마지막 처리 시각이 아니라 수집 간격을 포함한 상한이다.

DB는 해당 실행의 고유 ID에 대해서만 META와 최초 Attempt를 consistent BatchGet으로 읽는다. 미처리 key 재조회가 소진되면 대조 실패이며 누락으로 숨기지 않는다. 시뮬레이터 관리 조회에서 해당 attempt의 호출·처리 수를 읽는다. 두 조회는 부하 입력과 적체 관찰이 끝난 뒤 수행하며 발송 Worker의 판단에는 사용하지 않는다.

정상 통과 조건은 모든 입력의 202·반환 ID 일치, Kafka 양쪽 원본 존재, META 및 Attempt ACCEPTED, DLT 없음, 외부 calls=1·effects=1, 예상 밖 Kafka ID 없음, 미응답·dropped iteration 없음이다. 중복 요청의 Kafka record 중복은 허용하되 외부 추가 호출은 허용하지 않는다. DLT와 ACCEPTED가 공존하면 둘 다 기록하고 정상 성공으로 처리하지 않는다. REVIEW_REQUIRED·PROCESSING·Kafka에만 있음·어디에도 없음도 구분해 남긴다.

단계 지표의 신규 접수 counter는 별도 관측치다. 데이터 정합성의 근거를 counter 합계만으로 대체하지 않는다. `persistedAcceptancesPerWindowSecond`는 부하 전부터 적체 해소 관찰까지 포함한 지표 창의 평균이며 목표 입력 TPS·정상 구간 최대 처리량과 구분한다. DB 시각 기반 지연은 저장한 `updated_at - occurred_at`으로 계산한다. `updated_at`은 결과 Update 호출 직전 시각이라 DB ack 직후 Timer 지연과 약간 다르다.

## 결과 파일과 중단 조건

실행 폴더에 환경·commit·dirty 여부·앱 JAR 및 도구 SHA-256, 도구 버전, 자원 설정, 정책 전후, 앱 로그가 남는다. 실행별 하위 폴더에는 다음이 남는다.

| 파일 | 내용 |
|---|---|
| `requests.jsonl`, `k6-summary.json`, `k6.log` | 입력·응답, 입력률/지연/누락 iteration, 도구 로그 |
| `*.prom.gz`, `samples.jsonl`, `*-resources.json` | 구간 지표, partition별 끝/commit/lag·대기 시간, 자원 관측 |
| `kafka-records.jsonl`, `db-items.json` | body 없는 Kafka 식별 정보, 대조한 DB 상태·시각 |
| `reconciliation.jsonl`, `result.json` | 요청별 판정·업체 호출 수, 단계별 집계·통과 여부 |
| 상위 `results.json`, `failure.json` | 완료한 단계 결과, 실패 시 사유 |

지속 증가하는 적체 6회와 `max(100건, 입력률×5초)` 초과, DLT/운영 확인 증가, 앱 재시작/종료, 1GiB 미만 디스크 여유, 지표 수집 오류, k6 시간 초과 시 추가 부하를 중단한다. 가능한 범위에서 잔여 상태를 대조하고 원본을 보존한다. 프로세스가 SIGKILL되거나 호스트가 강제 종료된 실행은 완결된 보고서가 없을 수 있으며 원시 자료를 조사한다.

현재 도구는 정상 기준선과 동일 key 중복 검증용이다. 200 TPS 이상, 다수 고객·큰 본문, Kafka 직접 입력, 체계적인 장애 주입·실제 프로세스 kill·복구 성능은 후속이다. 코드·설정과 호스트 부하를 바꿔 재실행할 때에는 별도의 실행 ID로 비교한다.

첫 실행의 결과와 병목 후보는 [1차 측정 결과](16-1차-성능-PoC-측정-결과.md)에 남겼다. 100 TPS에서는 중단 조건이 실제 동작했으며 이를 정상 통과로 보정하지 않았다.
