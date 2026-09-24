# 업체 결과 웹훅의 인증과 Kafka 접수

구현·검증일: 2026-09-24. **업체가 보내는 결과를 Kafka에 수신하는 단계**를 구현했다. 발송 Attempt의 성공·실패 확정, 웹훅에 따른 재시도·2차 전환, 늦은 웹훅 판정, 고객 통지는 다음 Result Processor의 작업이다. 이 문서의 202는 최종 결과 처리 완료를 뜻하지 않는다.

## 수신 경로와 저장 책임

```text
1차 HTTP 업체 / 2차 TCP 업체
→ HTTP POST receipt-api
→ 업체별 Bearer 인증·본문 검증
→ delivery.receipt-received.v1 발행 (key = deliveryId)
→ Kafka ack 확인
→ 202 / RECEIVED

후속: Result Processor → 단계·기한·중복 판정 → 조건부 상태 변경·후속 발송
```

`receipt-api`는 고객 JWT API와 별도 프로세스다. PostgreSQL 사용자 조회, Redis TPS·Quota, DynamoDB 읽기·쓰기를 하지 않는다. 업체가 결과를 보내는 일이 고객 발송 quota를 소비해서는 안 되고, 고객 인증 저장소 장애가 결과 수신을 막아서도 안 되기 때문이다.

**DynamoDB를 여기서 추가하지 않은 이유:** 수신 사실의 내구성 인계는 기존 설계대로 Kafka ack를 사용한다. 중복 수신 자체는 허용하고, 실제 발송 상태 변경과 외부 부수 효과를 보호하는 조건부 저장은 결과 처리 단계에 둔다. Redis에는 수신 멱등키를 저장하지 않는다. 영속 판정이 없는 Redis 중복 차단으로 Kafka에 저장되지 않은 결과까지 이미 처리했다고 응답하는 경로를 만들지 않는다. [기존 접수 ADR](adr/ADR-004-Kafka-저장-확인-후-접수-응답하는-이유.md), [저장소 특성](07-저장소별-특성과-데이터-유실-복구-조건.md).

Kafka Producer는 `acks=all`, `enable.idempotence=true`, zstd, linger 5ms다. 발행 호출 자체의 예외와 비동기 ack 실패 모두 503이며, HTTP 비동기 timeout도 503이다. **503은 미저장 증거가 아니다.** 응답을 잃거나 HTTP 대기 시간이 먼저 끝나도 Kafka에는 저장될 수 있으므로 같은 receiptId·내용으로 재전송한다. Producer 멱등 기능은 별도 HTTP 재요청을 한 개로 합치는 업무 멱등성을 대신하지 않는다. [Kafka Producer 공식 설정](https://kafka.apache.org/40/configuration/producer-configs/), 확인일 2026-09-24.

## HTTP 계약

`POST /api/v1/receipts/{provider}`. 개발 기본 provider는 1차 `mock-provider`, 2차 `tcp-provider`다. 인증 헤더는 `Authorization: Bearer <업체별 키>`이며 두 업체의 키를 서로 다르게 설정한다. 미설정 키의 업체는 수신을 허용하지 않는다. 다른 업체의 키·중복 Authorization 헤더도 거절한다.

```json
{
  "receiptId": "provider-result-0001",
  "deliveryId": "d8f89940-b10f-4b88-a1a8-30bf482cb68a",
  "attemptId": "c7a9f3a3-cf17-4e3a-99ce-c2153bb57180",
  "outcome": "DELIVERED",
  "code": "DELIVERED",
  "occurredAt": "2026-09-24T00:00:00Z"
}
```

위 UUID는 형식 예시다. 실제 결과에는 발송 요청에서 받은 deliveryId·attemptId를 돌려준다. receiptId는 **해당 업체 내에서 결과 사실마다 고유**하고, 동일 사실을 재전송할 때 바꾸지 않는다. 새 결과에 기존 receiptId를 재사용하지 않는다.

| 필드·검사 | 계약 |
|---|---|
| receiptId | 영숫자·점·밑줄·콜론·하이픈, 1~128자 |
| deliveryId / attemptId | 소문자 UUID 형식. 존재 여부·업체·단계 연결 검증은 후속 |
| outcome | DELIVERED 또는 FAILED |
| code | 대문자로 시작하는 영문 대문자·숫자·밑줄, 최대 64자 |
| 성공 조합 | DELIVERED + DELIVERED |
| 실패 조합 | FAILED + 실패 코드. ACCEPTED·RECEIVED·DELIVERED는 거절 |
| 미지의 실패 코드 | 형식이 유효하면 보존. 후속 정책에서 격리해야 하며 임의 성공·전환 금지 |
| occurredAt | 업체가 보고한 발생 시각. 우리 시스템의 만료 기준을 덮어쓰지 않음 |
| 본문 | 최대 16KiB. Content-Length 없는 chunked 요청에도 적용 |
| 미정의 필드 | 400. 원문 메시지 payload·고객 ID·인증정보를 받지 않음 |

provider와 routeOrder는 인증한 경로로부터 서버가 넣으며 본문으로 바꿀 수 없다. receivedAt은 인증·본문 검증 후 수신 서비스에 진입한 서버 시각이다. 소켓 접속 시작 시각이나 업체 시각이 아니다. 동시 수신·서로 다른 Kafka 파티션/토픽 사이의 전역 순서를 보장하지 않는다.

성공 응답: `202 {"eventId":"...","status":"RECEIVED"}`. 인증 실패는 401, 요청 형식 오류는 400, 본문 초과는 413이다. 발행 미확인은 `503 {"code":"RECEIPT_UNCONFIRMED","message":"저장 여부를 확인하지 못했습니다. 동일한 receiptId와 내용으로 재전송해 주세요."}`다. 업체 내부 오류·원문 요청·인증키를 HTTP 오류 본문에 노출하지 않는다.

비동기 응답 시 새 스레드에서 인증을 다시 적용한다. 내부 ERROR dispatch는 오류 응답을 전달하도록 허용한다. 최초 실제 요청의 업체 인증은 그대로 필요하다. [Spring Security dispatch 권한 공식 문서](https://docs.spring.io/spring-security/reference/7.0/servlet/authorization/authorize-http-requests.html), 확인일 2026-09-24.

## Kafka 계약과 중복의 의미

공통 `ReceiptEvent`는 schemaVersion=1, eventType=DeliveryReceiptReceived, 위 필드 및 provider·routeOrder·receivedAt을 담는다. Java 클래스명 type header는 발행하지 않는다.

eventId는 provider+receiptId로 안정적으로 만든다. 같은 HTTP 재전송은 **동일 eventId의 Kafka record 여러 건**으로 남을 수 있고 각 receivedAt은 다를 수 있다. API가 동일 ID·상이한 내용의 충돌을 숨기거나 덮어쓰지 않는다. Result Processor에서 원래 결과 내용과 비교하고 충돌을 격리해야 한다. 성공 receipt를 받았다는 이유만으로 여기서 Attempt를 DELIVERED로 변경하지 않는다.

늦은 웹훅 정책은 유지한다. 수신 API는 기한을 조회하지 않으므로 전송 계층인 Kafka에는 먼저 인계한다. 후속 처리기는 저장된 단계·기한을 확인해 만료 후 해당 단계의 웹훅을 로그 후 폐기하고 업무 상태·이력·추가 발송에 반영하지 않아야 한다. **현재는 그 처리기와 만료 판정이 없으므로 늦은 웹훅 처리까지 완성됐다고 볼 수 없다.** 만료 전 수신 후 큐 대기와 만료 작업의 경합은 [미정 경계](08-함께-결정할-설계-사항.md)에 남아 있다.

현재 재시도들은 동일 attemptId를 공유한다. 후속 실패 웹훅 처리 전에 재시도 회차 식별과 오래된 실패의 재적용 방지 계약도 완성해야 한다. 이번 수신만으로 중복 발송 방지·업무 결과 수렴을 보장하지 않는다.

## 실행과 관측

`.env.example`의 `RECEIPT_PRIMARY_SECRET`, `RECEIPT_SECONDARY_SECRET`에 서로 다른 32~256자 인쇄 가능 ASCII 키를 넣고 빌드 후 실행한다. 기본값은 비어 있어 수신이 비활성화된다. 테스트 키를 운영 키로 사용하지 않는다. 로컬 서버·관리 포트는 loopback에 바인딩하며 TLS·키 교체·외부 접근 경계는 운영 구성 후속이다. Bearer 키 인증은 요청 본문 HMAC 서명이 아니다.

```bash
bash scripts/local.sh run receipt-api
```

예시 환경의 수신 포트는 18094, 관리 포트는 19084다. 이 모듈만 시험하려면 Kafka만 필요하다. `scripts/local.sh`가 다른 저장소 주소도 환경에 넣지만 receipt-api는 해당 클라이언트를 생성하지 않는다. Kafka 없이 시작하면 topic 준비를 실패로 보고 시작을 중단한다.

| 설정 | 기본값 |
|---|---|
| receipt.topic | delivery.receipt-received.v1 |
| receipt.partitions / replicas / min-insync-replicas | 3 / 1 / 1 (단일 브로커 로컬용) |
| 새 토픽 cleanup / retention | delete / 7일 |
| KAFKA_PUBLISH_MAX_BLOCK_MS | 5000 |
| KAFKA_PUBLISH_REQUEST_TIMEOUT_MS / DELIVERY_TIMEOUT_MS | 5000 / 10000 |
| RECEIPT_HTTP_ASYNC_TIMEOUT | 20s |

새 토픽은 시작 시 준비한다. 기존 토픽의 복제 수·보관 정책을 이 설정 변경만으로 마이그레이션하지 않는다. 현재 Result Processor가 없으므로 운영 수신을 활성화하지 않는다. **소비하지 않은 결과도 보관 기간이 지나면 사라질 수 있다.** 처리기·lag 감시·보관 용량을 갖춘 뒤 활성화해야 한다. 다중 AZ 복제·ISR 운용과 장애 검증은 [무손실 설계](09-저장소-장애-시-중단-범위와-복구-절차.md)의 후속 작업이다. 로컬 replica=1의 202는 다중 AZ 무손실의 증거가 아니다. [Kafka 토픽 공식 설정](https://kafka.apache.org/40/configuration/topic-level-configs/), 확인일 2026-09-24.

관리 `/actuator/prometheus`에서 `delivery_stage_duration_seconds{stage="receipt_publish",result="success|failure"}`와 `delivery_stage_active{stage="receipt_publish"}`를 확인할 수 있다. duration은 발행 시작부터 ack까지이며 최종 결과 처리 지연이 아니다. audit는 stage=receipt·provider·eventId·접수 확인 여부만 기록하고 본문·인증키·deliveryId는 추가하지 않는다. 기존 Grafana Compose에는 새 모듈을 자동 배포하지 않았으며 수집 대상·결과 처리 패널 연결은 후속이다.

## 검증과 발견 사항

**전체 152건 통과, 실패·제외 0건, JAR package 통과.** API 9·공통 11·Ingress 19·Dispatch 82·시뮬레이터 13·Receipt 18건이다. 새 수신 HTTP 시험 16건, 실제 Kafka 시험 2건, 공통 receipt 식별자 시험 1건을 추가했다. 기존 DynamoDB·Kafka 통합 시험도 함께 실행했다.

수신 모듈의 실제 HTTP 서버 시험으로 ack 대기, 1차·2차 인증 격리, 잘못된 입력·미정의 필드, Content-Length 유무별 크기 제한, 동기/비동기 발행 실패, HTTP timeout 후 Kafka 완료 가능성과 동일 eventId 재전송을 확인했다. 실제 Kafka 시험은 UUID 전용 토픽을 쓰며 다른 토픽·offset·실행 서비스를 변경하지 않는다.

실제 HTTP→Kafka 경로에서 같은 receipt를 두 번 보내 동일 eventId의 record 두 건을 읽었다. 시험 토픽의 `max.message.bytes`를 100으로 내려 실제 broker 저장 거절을 유도해 HTTP 503을 확인하고, 제한 복구 후 같은 요청의 202·Kafka record를 확인했다. 이는 broker 저장 거절 시험이며 브로커 종료·복제본 상실 시험이 아니다.

**처음 시도한 장애 주입의 한계:** replica=1 토픽에서 min.insync.replicas만 2로 높여도 Kafka 4.0.2는 기록을 수락했다. 브로커 로그로 설정 적용을 확인했고, 공식 4.0.2 코드의 `effectiveMinIsr`가 `min(설정값, 실제 복제 수)`를 사용함을 확인했다. 따라서 이 방식의 202를 애플리케이션의 ack 누락으로 오판하지 않았으며, 해당 시도를 복제 장애 검증 통과로 집계하지 않는다. 실제 복제 수와 ISR을 함께 검증해야 한다. [Apache Kafka 4.0.2 Partition 소스](https://github.com/apache/kafka/blob/4.0.2/core/src/main/scala/kafka/cluster/Partition.scala), 확인일 2026-09-24.

재현 명령은 다음과 같다. 통합 환경 변수가 없으면 기존 방식대로 해당 통합 시험은 제외되므로 결과의 skipped 수를 함께 확인한다.

```bash
DYNAMODB_TEST_ENDPOINT=http://localhost:18000 \
KAFKA_TEST_BOOTSTRAP_SERVERS=localhost:39092 \
JAVA_HOME=/path/to/jdk-21 ./mvnw -q package
```

## 다음 커밋 범위

Result Processor에서 실제 Attempt·업체·단계를 연결하고, 만료 후 폐기·중복/충돌·응답과 웹훅 순서 역전을 조건부 상태 전이로 처리한다. 실패 코드의 재시도·허용된 2차 전환과 내구성 후속 발행을 연결한 다음, 시뮬레이터 자동 웹훅·주기 만료·고객 통지로 이어간다.
