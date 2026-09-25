# 고객 HTTP 묶음 통지와 재전송 복구

작성일: 2026-09-25. 대상: `delivery-result-worker`, 고객 수신 대역 `external-api-simulator`.

## 이번에 연결한 흐름

```text
최종 결과 Kafka → PostgreSQL 이력 + 고객 통지 PENDING
→ 같은 고객의 준비된 결과 최대 100건 선택
→ 묶음 ID·본문·목적지 URL 고정 + 호출 횟수 예약 COMMIT
→ 고객 HTTP POST
→ 204 확인: 묶음·항목 DELIVERED를 함께 COMMIT
  그 외: 다음 재전송 시각 저장 / 총 21회 소진 시 EXHAUSTED 보존
```

외부 업체의 1차 HTTP·2차 TCP 발송과 별개다. 고객 통지가 실패해도 발송 이력의 성공/실패/만료 결과를 바꾸거나 업체에 다시 발송하지 않는다. 이 단계의 DynamoDB·Redis 읽기/쓰기는 없다. 원본·Attempt·receipt marker 정리는 아직 실행하지 않는다.

## 사용자 요구와 이번 PoC 기본 계약

사용자가 확정한 조건은 같은 고객 최대 100건, 짧게 묶어 HTTP 통지, 최대 20회 재전송이다. 아래 HTTP 상태 코드·간격·인증·부분 응답 방식은 구현과 검증을 위한 **PoC 기본값**이다. 실제 고객 계약을 확정한 것으로 보지 않는다.

| 항목 | 구현한 PoC 계약 |
|---|---|
| 요청 | POST, `Content-Type: application/json`, 고객별 Bearer token |
| 멱등 식별자 | `Idempotency-Key: batchId`, 각 결과의 `eventId` |
| 본문 | `schemaVersion: 1`, `batchId`, `tenantId`, `results: [DeliveryFinalized, ...]` |
| 수신 확인 | **204만** 묶음 전체 수신 확인. 응답 본문은 읽지 않고 닫음 |
| 재전송 | 204 외 상태, 연결 실패, timeout, 응답 유실은 같은 묶음·본문으로 재전송 |
| 부분 응답 | 아직 지원하지 않음. 200·207 및 항목별 응답을 일부 성공으로 반영하지 않음 |
| 최대 횟수 | 최초 1회 + 재전송 20회 = 총 21회 예약 |
| 재전송 간격 | 기본 1·2·4·8·16·32·60초… 상한 60초. 설정 가능, 아직 jitter 없음 |
| 네트워크 | 기본 timeout 3초, redirect 미추적, HTTPS 사용. 로컬 시험만 loopback HTTP 허용 |

고객은 결과를 안전하게 수신·중복 처리할 수 있게 보존한 뒤 204를 반환해야 한다. 단순 수신 로그만 쓰고 204를 반환한 후 고객 측에서 잃어버리는 문제를 발송자가 검증할 수는 없다. HTTP 204의 일반 의미는 성공이며, 이를 **묶음 전체의 내구성 있는 수신 확인**으로 해석하는 것은 이 PoC의 별도 계약이다. [RFC 9110 §15.3.5](https://www.rfc-editor.org/rfc/rfc9110.html#name-204-no-content).

401·429·500 등도 현재는 같은 재전송 예산을 사용한다. `Retry-After`·부분 성공·특정 4xx 즉시 종료 정책은 구현하지 않았다. 운영 계약 적용 시 별도로 정해야 한다.

## 묶음과 고객 격리

기본 100ms 주기로 준비된 결과를 찾고, 1건이어도 바로 묶음을 만든다. 100건을 채우는 별도 대기 창은 없다. 본문 기본 상한은 256KiB이며, 건수가 100 미만이어도 바이트 상한에 맞춰 나눈다. 단일 결과조차 상한을 넘으면 예약을 rollback하고 PENDING으로 보존한다.

고객별 행을 짧게 잠근 뒤 준비된 outbox를 `FOR UPDATE ... SKIP LOCKED`로 선택한다. PostgreSQL 공식 문서는 SKIP LOCKED를 일반 일관 조회보다 여러 소비자의 큐 처리에 적합한 방식으로 설명한다. [PostgreSQL 17 잠금 절](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE). 고객별 선택과 예약에만 사용하고 HTTP 동안에는 SQL 트랜잭션·연결을 유지하지 않는다.

고객별 유효한 IN_FLIGHT 묶음은 하나이며 DB 부분 유일 색인으로 보호한다. 앱 안에서는 고객별 작업 하나와 전체 실행 슬롯 기본 4개를 두고 고객을 순환한다. 대기 중인 HTTP 하나가 슬롯을 모두 차지하지 않는다. 슬롯 수 이상의 고객이 동시에 느려지거나 DB 자체가 멈추면 다른 통지도 지연될 수 있다.

미래 시각의 재전송 대기는 같은 고객의 새 통지를 막지 않는다. 새 결과는 별도 묶음으로 나가며 이전 묶음에 끼워 넣지 않는다. 재전송 시각이 된 묶음부터 처리하므로 고객 내부에서 통지가 반드시 발송 인입 순으로 도착한다는 보장은 없다. 부분 응답을 구현하기 전에는 한 묶음의 구성 자체를 바꾸지 않는다.

100ms는 poll 기본값이며 부하·장애 상황의 절대 통지 지연 상한은 아니다. 만료를 묶음 때문에 연장하지 않지만, 고객 장애 중 실제 수신을 만료 시각 이내에 보장한다는 요구는 여전히 충족할 수 없다. [기존 기한·고객 장애 논의](10-고객-결과-묶음-전송과-발송-완료-데이터-정리.md)의 미해결 조건을 유지한다.

## 저장하는 정보와 횟수 보존

Flyway V2가 기존 V1의 이력·PENDING을 보존하면서 다음 정보를 추가한다.

| 테이블 | 역할 |
|---|---|
| `customer_notification_lane` | 고객별 예약 경합을 직렬화하는 작은 행 |
| `customer_notification_batch` | 고정 ID·본문·목적지, 항목 수, 상태, 총 예약 횟수, 다음 시각, lease/token, 마지막 실패 사유 |
| `customer_notification_outbox` | 기존 결과별 행에 batch_id 연결. 항목별 횟수·최종 수신 상태 유지 |

묶음 생성·항목 연결·횟수 증가·lease 선점을 하나의 SQL 트랜잭션에 넣는다. DB 저장에 실패하면 HTTP를 호출하지 않는다. 전송 결과를 기록할 때도 묶음과 항목을 함께 갱신한다. 기존 최종 Kafka 이벤트가 다시 와도 이력 소비자는 통지 상태·횟수를 초기화하지 않는다.

전송은 DB 예약 commit 이후다. **예약 후 HTTP 직전에 종료되어도 한 번 사용한 것으로 센다.** 실제로 고객에게 간 요청이 21회보다 적을 수 있지만 반복 재시작으로 전송 한도를 늘리지 않는다. 만료 lease는 다음 회차를 예약해 같은 묶음으로 재전송한다. 21번째 예약의 결과를 못 남겼다면 lease 만료 후 `EXHAUSTED / LAST_ATTEMPT_UNCONFIRMED`로 보존하고 22번째는 보내지 않는다.

기본 lease는 30초이고 HTTP timeout보다 최소 5초 길어야 한다. 기한 비교는 PostgreSQL 시각을 사용한다. 호출 직전 lease 소유권을 다시 확인하고, 결과 저장은 batch ID·token·회차·미만료 lease가 맞을 때만 수행한다. 이전 작업자의 늦은 응답은 새 회차 결과를 덮어쓰지 못한다.

다만 소유권 확인 직후 프로세스가 오래 멈췄다가 재개하면 실제 네트워크 호출을 DB에서 취소할 수는 없다. HTTP 성공 후 DB 저장 전 종료·응답 유실에도 고객이 이미 수신했을 수 있다. **고객의 eventId 중복 제거가 필요하며 외부 효과 exactly-once를 보장하지 않는다.** [멱등 재시도에 대한 AWS 공식 설명](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/). 대역은 결과 ID 중복 제거를 수행하지만 발송자는 대역의 관측 기록을 조회하지 않는다.

## 실패와 복구 절차

| 상황 | 처리·복구 |
|---|---|
| 예약 SQL 실패 | 전체 rollback. 횟수와 묶음 연결 불변, HTTP 0회. DB 복구 후 재선택 |
| HTTP 실패/무응답 | 재전송 시각을 저장. DB 갱신도 실패하면 IN_FLIGHT 유지 후 lease 복구 |
| HTTP 204 후 SQL 저장 실패 | 고객 수신은 불명. 같은 ID·본문 재전송, 고객 중복 제거 필요 |
| 소비자/통지 프로세스 재시작 | DB에 남은 PENDING·만료 lease를 원래 횟수로 이어 처리 |
| 총 21회 소진 | EXHAUSTED 보존. 발송 결과는 그대로, 자동 재전송 중단 |
| 고객 URL 미설정 | 원래 outbox PENDING/0 유지. 설정 후 앱 재시작 시 선택 |
| 기존 묶음 URL과 설정 URL 불일치 | 기존 묶음을 새 URL로 바꾸지 않음. 원래 URL 설정 복원 또는 감사 가능한 이관 도구 필요 |

인증키는 DB에 넣지 않고 외부 설정에서 읽는다. URL은 묶음에 고정한다. 같은 URL의 키 교체는 설정 변경 후 재시작으로 적용할 수 있다. URL 변경 시 기존 대기 묶음의 자동 이관은 없다. 만료된 IN_FLIGHT의 URL이 달라지면 해당 고객의 lane이 보류될 수 있으므로 활성 묶음을 확인한 뒤 변경한다.

수동 EXHAUSTED 재전송·추가 예산 부여 API는 아직 없다. 상태나 attempt_count를 임의로 초기화하는 것을 복구 절차로 사용하지 않는다. 이력·묶음·outbox·DynamoDB 완료 기록은 이번 단계에서 삭제하지 않는다.

## 로컬 실행

새 소비 기능은 기본 비활성화다. 실제 고객 endpoint가 없으므로 다음과 같이 대역과 고객별 설정을 명시한다. tenantId 42는 예시이며 접수에 사용하는 실제 tenantId로 바꾼다. 비밀값을 저장소에 커밋하지 않는다.

1. `.env`에 `SIMULATOR_CUSTOMER_RESULTS_ENABLED=true`와 충분한 임의 문자열 `CUSTOMER_RESULT_SIMULATOR_SECRET`을 설정한다. 토큰 길이는 32~256 ASCII 문자다.
2. `bash scripts/local.sh run external-api-simulator`로 고객 수신 경로도 활성화한다.
3. 저장소 밖의 설정 파일에 고객별 URL과 token을 둔다. 다음 token 환경변수는 `.env`의 대역 값과 같아야 한다.

```yaml
notification:
  enabled: true
  customers:
    42:
      url: http://127.0.0.1:18090/api/v1/customer-results/42/FAIL_ONCE
      token: ${CUSTOMER_RESULT_SIMULATOR_SECRET}
```

```bash
SPRING_CONFIG_ADDITIONAL_LOCATION=file:/absolute/path/customer-notification.yml \
  bash scripts/local.sh run delivery-result-worker
```

상세 우선순위는 Spring 설정 규칙을 따른다. 외부 파일의 `notification.enabled: true`를 쓰거나 `.env`의 `CUSTOMER_NOTIFICATIONS_ENABLED=true`로 활성화한다. Kafka finalized topic과 PostgreSQL이 준비돼 있어야 한다. [선행 단계](30-최종-이력과-고객-통지-대기-기록의-원자-저장.md).

대역 경로의 마지막 값은 다음 중 하나다. 이 대역의 기록은 메모리에만 있고 재시작 시 사라지므로 업무 내구성 저장소가 아니다.

| 모드 | 동작 |
|---|---|
| ACK | 결과 ID로 중복 제거하고 204 |
| FAIL_ONCE | 같은 묶음 첫 요청 503, 다음부터 204 |
| REJECT | 계속 503, 효과 없음 |
| ACK_THEN_503 | 수신 효과를 기록하고 503. 성공 확인 실패 후 중복 제거 관측 |
| DELAY_ACK | 효과 기록 후 기본 5초 지연하고 204 |

대역은 인증, tenant·묶음 ID 일치, 100건·1MiB 입력 상한, 같은 묶음 ID의 본문 충돌을 검사한다. 메모리 보관 상한에 도달하면 기존 기록을 축출하지 않고 503이다. 관리 포트 `http://127.0.0.1:19090/actuator/customerResults`에서 요청 수·묶음 수·고유 결과 효과 수를 확인한다. 실제 HTTP 연결 종료에 의한 응답 유실은 자동 테스트의 별도 로컬 HTTP 서버로 검증한다.

## 관측

새 워커의 `/actuator/prometheus`에는 다음 지표를 추가했다. ID·고객별 metric tag는 쓰지 않는다.

- `delivery_notification_events_total`: HTTP 확인/실패, 정상 수신 기록, 재전송 예약, 소진, stale 선점/갱신, 작업 실패.
- `delivery_notification_http_duration_seconds`: HTTP 대기 시간.
- `delivery_notification_batch_size`: 결과 저장까지 끝난 묶음의 크기.
- `delivery_notification_active`: 해당 앱의 실행 슬롯 사용 수.

카운터는 시도 횟수이며 DB의 현재 미처리 건수와 같지 않다. 마지막 미확인 예약을 lease 만료로 소진 처리하는 경우의 전체 건수는 DB에서 확인한다. 대기/소진·미설정 고객·목적지 변경 보류를 조사할 때는 다음처럼 읽는다.

```sql
SELECT status, count(*), min(created_at) AS oldest_created
FROM delivery_results.customer_notification_outbox GROUP BY status;

SELECT batch_id, tenant_id, status, attempt_count, next_attempt_at, lease_until, last_error
FROM delivery_results.customer_notification_batch
WHERE status IN ('PENDING', 'IN_FLIGHT', 'EXHAUSTED')
ORDER BY created_at;
```

Grafana의 신규 수집 대상 등록·통지 대기/소진 패널·알림은 후속이다. 고빈도 전체 COUNT를 poll 경로에 추가하지 않았다.

## 검증 결과와 남은 범위

실제 PostgreSQL 17.11·Kafka 4.0.2와 로컬 HTTP를 사용했다. 시험 자원은 UUID 전용 DB 스키마·토픽·그룹·임시 포트로 격리해 정리한다. [검증 기록](검증-결과/2026-09-25-고객-HTTP-묶음-통지와-재전송.json)에 전체 결과와 소스 해시를 남긴다.

검증 항목은 1/99/100/101건 묶음, 바이트 분할, 고객 혼합 차단, 21회 후 중단, 재구성한 워커의 동일 본문·횟수 유지, 미래 재전송 대기 중 신규 결과 진행, 실제 응답 유실, HTTP 204 후 SQL rollback, 호출 전 예약 rollback, 16개 동시 선점, 만료 token의 저장 차단, 미확인 예약 21회 소진, URL 변경 보류, 204 외 응답·redirect, 느린 고객 격리·timeout, 미설정 고객 보존, V1→V2 데이터 이관이다. 실제 앱 설정으로 Kafka→SQL→고객 HTTP→수신 상태 저장·Prometheus까지 연결했다. 대역 앱의 HTTP·관리 endpoint 기동도 검증했다.

```bash
DYNAMODB_TEST_ENDPOINT=http://localhost:18000 \
POSTGRES_TEST_URL=jdbc:postgresql://localhost:15432/delivery \
KAFKA_TEST_BOOTSTRAP_SERVERS=localhost:39092 \
JAVA_HOME=/path/to/jdk-21 ./mvnw -q package
```

JVM 강제 종료·다중 AZ 장애를 시험한 것은 아니다. 저장 직후 실패와 재시작은 SQL 오류 주입·lease 만료·저장소/서비스 재생성으로 재현했다. 기존 발송 시작부터 고객 수신까지 새 고 TPS/장시간 PoC는 아직 수행하지 않았다. 고객 수가 늘면 고객별 poll SQL·이력 join·건별 트랜잭션 비용을 측정하고 due 고객 조회/색인을 조정해야 한다. 다음 단계는 완료 원본 정리의 안전한 인계와 전체 흐름·관측 검증이다.

## 현재 v2의 완료 원본 정리

SQL 이력·통지·정리 예약을 함께 commit한 뒤 해당 실행의 ORIGIN·STEP을 조건부 삭제합니다. 고객 통지는 SQL 결과로 계속하며 고객 204를 받아야 DDB를 삭제하는 구조가 아닙니다. 완료 후 중복 차단은 Redis TTL이며, 표식 소실과 DDB 조건 통과 시 새 실행을 허용합니다. 작은 완료 META 영구 유지는 기존 v1 호환 경로입니다. [현재 저장·정리 계약](35-원본과-단계-최소-저장-및-Redis-완료-중복-차단.md).

기존 데이터 이관과 기능 활성화는 [테이블 이관 절차](36-원본과-단계-테이블-분리와-경로별-쓰기-내역.md)를 따릅니다. 위 통지 도입 당시의 시험 결과와 현재 전체 회귀 결과는 구분합니다. 최신 결과·남은 작업은 [01 진행 현황](01-현재-구현-상태와-남은-작업.md), 정책 변경 이유는 [ADR-022](adr/ADR-022-처리-중-DynamoDB-선점과-완료-후-Redis-중복-차단.md)에 있습니다.
