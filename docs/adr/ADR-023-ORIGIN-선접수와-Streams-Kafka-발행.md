# ADR-023 ORIGIN 선접수와 Streams 기반 Kafka 발행

- Status: Accepted
- Date: 2026-09-30
- 적용 상태: 설계 결정, 구현·부하 검증 전.
- 대체 범위: ADR-004의 최초 접수 완료 기준과 ORIGIN 저장 위치, ADR-005·ADR-022의 신규 인입 식별자·고객 중복 정책, 최초 Kafka 이벤트의 별도 ingress·dispatch 인계. 기존 Kafka-first 구현과 검증 기록은 이관 전 사실로 보존한다. 업체 웹훅의 Kafka 접수와 발송 이후 결과 인계 정책은 변경하지 않는다.

## 문제와 결정

Kafka broker가 기록을 저장했지만 최초 producer가 ack를 받지 못할 수 있다. 기존 Kafka-first 경로에서는 API가 오류를 반환하는 동안 ingress가 그 기록을 소비해 업체 발송을 시작할 수 있다. 고객 접수의 내구성 기준과 업체 발송 허가 기준을 분리한다.

`EVENT-RECEIVE-API`는 Kafka에 직접 발행하지 않는다. 신규 실행의 ORIGIN을 `RECEIVED`로 조건부 저장한 사실을 확인한 뒤 `202 Accepted`를 반환한다. `202`는 원본의 영속 접수이며 Kafka 발행·업체 접수·최종 발송 성공을 뜻하지 않는다. ORIGIN 저장 실패 또는 결과 불명 시 성공으로 응답하지 않는다. 저장 결과가 불명확하면 같은 `executionId`로 원본을 조회·대조해 수렴해야 한다.

DynamoDB Streams는 신규 ORIGIN 기록의 빠른 트리거다. 별도의 Java `EVENT-PUBLISHER-APP`가 Streams를 읽고 동일 `executionId`로 최초 HTTP 요청 Kafka 토픽에 발행한다. Lambda는 필수가 아니다. 발행 앱은 Kafka ack가 확인되면 Streams 체크포인트를 진행하고, 실패하거나 결과가 불명확하면 같은 실행으로 재시도한다. ORIGIN에 별도의 발행 완료 상태를 쓰지 않는다. 목표 구조에서 `event-http-sender`가 최초 Kafka 레코드를 직접 소비하며, 실제 업체 호출은 ORIGIN 상태 확인과 기존 STEP 선점 조건을 통과해야 한다. 현재 `delivery-ingress-worker`가 맡는 ORIGIN 최초 저장은 API로 이동하므로 별도 ingress 워커는 두지 않는다. 1차·2차 발송과 `EVENT-RESULT-MANAGER`의 판단·토픽 분리는 [ADR-024](ADR-024-HTTP와-TCP-발송-워커-분리.md)를 따른다.

## 인입 계약과 중복

- 고객 접수 API의 목표 이름은 `EVENT-RECEIVE-API`, 경로는 `POST /api/v1/events`다. 현재 코드의 `POST /api/v1/deliveries`와 구분한다.
- `clientId`는 인증된 JWT에서 얻는다. 요청 본문에 `eventId`, `recipientNumber`, `eventType`, `payload`, `fallbackAllowed`를 둔다. `Idempotency-Key` 헤더는 신규 접수에 사용하지 않는다.
- `recipientNumber`는 `01012345678`처럼 하이픈 없는 `010` 시작 11자리만 허용한다. `+82`나 하이픈 입력을 변환하지 않는다.
- `eventType`은 `GENERAL`, `NOTI`, `ADV`, `ALERT`이며 유형별 우선순위는 없다.
- 고객 중복키는 `(clientId, eventId, recipientNumber)`다. Redis의 활성 키는 처리 중 새 실행을 차단하고 기존 `executionId`와 원문 지문을 보유한다. 같은 키에 다른 내용이 오면 충돌로 처리한다. Redis가 사용 불가하면 이 중복 검사를 우회하고 서로 다른 `executionId`의 실행을 허용한다.
- 신규 허용 요청마다 API가 UUID `executionId`를 만든다. Kafka 재발행·복구에는 이 ID를 재사용한다. ORIGIN은 실행별 원본이며 고객 중복키의 영속 소유권을 대신하지 않는다. 동일 실행의 Kafka record가 여러 개여도 STEP 조건부 선점으로 불필요한 업체 재발송을 막는다. 다른 실행 간 외부 중복 효과는 Redis 장애 시 허용 범위다.
- Redis 활성 키 확보 뒤 ORIGIN 저장이 확정 실패하면 해당 `executionId`의 키만 해제한다. ORIGIN 저장 결과가 불명확하면 동일 실행을 조회·대조하기 전에는 키를 해제하지 않는다.
- 인증 후 본문 파싱, 계약별 TPS·유형별 월 Quota 기록, 업무 필수값 검증, Redis 고객 중복 확인, ORIGIN 저장 순서로 이관한다. 10초 고정 구간 인입량과 유형별 월 사용량을 기록하며 고객 중복 요청도 차감한다. 본문 파싱 실패 또는 `eventType` 자체를 판별할 수 없는 요청은 유형별 Quota를 선택할 수 없으므로 별도 거절·계측한다. 현재 필터와 Spring `@Valid` 실행 순서는 이 목표와 다르므로 AOP라는 표현만으로 구현 완료로 간주하지 않는다.
- 최종 SQL 이력 정리 시 성공은 성공 시각부터 2시간 동안 Redis 차단을 유지한다. 성공 이외의 최종 상태는 Redis 중복키를 제거해 새 접수를 허용한다. 두 작업 모두 저장된 `executionId`가 최종화 대상과 같을 때만 적용한다. 첫 번째 발송 단계가 끝났다는 이유로 키를 조기 해제하지 않는다.

## 발행·발송 순서와 복구

1. API는 ORIGIN을 `RECEIVED`로 저장한다. Streams 기록이 먼저 도착하거나 API의 HTTP 응답이 유실되어도 원본을 기준으로 처리한다. `RECEIVED`는 최초 접수 사실이며 Kafka 발행 완료를 뜻하지 않는다.
2. 발행 앱은 신규 ORIGIN의 `INSERT` 기록만 발행 대상으로 사용한다. 후속 갱신·최종 정리의 `MODIFY`·`REMOVE` 기록은 재발행 트리거에서 제외한다. 원본이 이미 최종화·정리됐다면 재발행하지 않는다.
3. Kafka producer는 `acks=all`과 멱등성을 사용한다. ack 확인 전에는 Streams 체크포인트를 진행하지 않는다. 발행 실패·ack 불명확 시 동일 `executionId`와 원문으로 재시도한다. ack 유실 전에 저장된 레코드를 Kafka 소비자가 먼저 볼 수도 있으며, 그 자체가 발행 사실의 증거다.
4. `event-http-sender`가 최초 Kafka 레코드를 직접 소비하고 HTTP 1차 호출 전에 ORIGIN·STEP의 최신 상태와 조건부 선점을 확인한다. 즉시 응답·웹훅·기한을 `EVENT-RESULT-MANAGER`가 판단해 허용된 2차 전환을 Kafka로 인계하며 `event-tcp-sender`가 별도 2차 STEP을 선점한다. 현재 구현은 단일 `dispatch-worker`다. ORIGIN이 없거나 완료·취소·만료된 실행은 새 발송을 허용하지 않는다. 별도의 ingress 워커·`DispatchRequested` 인계·`READY` 게이트는 두지 않는다.
5. Streams 소비 실패·24시간 초과 중단에도 대비해 아직 후속 진행 증거가 없는 ORIGIN을 주기적으로 찾아 원본과 STEP·최종 이력을 대조한 뒤 재발행한다. 발행 재시도는 같은 `executionId`와 본문을 사용한다. 애플리케이션 수준 재발행은 Kafka record를 중복 생성할 수 있으므로 발송 선점·최종 이력은 실행 기준으로 멱등하게 처리한다.
6. Kafka를 발행할 수 없는 동안에도 API는 ORIGIN 저장에 성공한 요청에 `202`를 반환할 수 있다. 적체가 허용 용량·원래 업무 기한을 위협하면 접수 제한이나 최종 실패·만료 정책을 적용하며, `RECEIVED`를 성공 발송으로 해석하지 않는다.

## 경계와 성능 영향

ORIGIN과 Kafka는 하나의 트랜잭션이 아니다. Kafka 레코드 소비를 최초 발행의 확인으로 삼고, ack 불명확 시 중복 가능한 재시도와 실행별 선점으로 복구한다. 발행 앱의 ack가 유실돼도 Kafka 소비자가 레코드를 읽으면 후속 처리가 가능하다. HTTP `202` 응답만 유실된 경우에도 서버가 원본 저장을 완료했다면 이후 발송이 가능하다. 고객이 응답을 받지 못했다는 사실만으로 발송 금지를 보장할 수 없으며, 같은 고객 중복키로 접수 상태를 확인할 API·조회 인덱스는 이관 작업에서 구체화한다.

기존 정상 1차 성공 DDB 5회 호출·7항목 변경 수치는 새 경로에 적용되지 않는다. 최초 ORIGIN Put과 발송 전 기존 조건 확인·복구 조회·GSI·Streams/KCL 비용을 포함해 다시 계측한다. 최초 발행의 ack 후 `READY` Update는 없다. Java 발행 앱은 소비 지연 기반으로 확장할 수 있으나 병렬도는 활성 Streams 샤드 수의 영향을 받는다. 정상·장애 처리량과 복구 지연을 10,000 TPS 목표 시험에서 측정한다.

## 검증 방법

- ORIGIN 저장 실패·응답 유실, API 종료 후 Streams 발행, Kafka 발행 실패·ack 유실·중복 발행을 각각 주입한다.
- 발행 앱의 ack가 유실됐어도 Kafka 소비자가 레코드를 읽으면 정상 진행하는지, 동일 실행의 중복 레코드는 STEP 선점으로 업체를 재호출하지 않는지 확인한다.
- Streams 소비 중단·재시작·24시간 초과를 대신하는 원본·STEP·이력 대조 복구와 중복 레코드 처리를 검증한다.
- 성공 후 2시간, 실패·만료 후 키 제거, Redis 장애 중 별도 실행 허용, 이전 실행의 늦은 정리가 새 실행의 키를 지우지 못하는 조건을 검증한다.
- API `202` TPS와 최종 결과 TPS, 후속 진행 증거가 없는 ORIGIN의 체류 시간·건수, Streams 소비 지연, Kafka 발행 재시도·중복, DDB 호출량과 실제 외부 효과를 함께 계측한다.

## 공식 근거

- [AWS DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html): 변경 기록의 접근·순서와 24시간 보존.
- [AWS DynamoDB Streams Kinesis Adapter](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.KCLAdapter.html): Java 소비자의 샤드 배분·체크포인트.
- [Apache Kafka Producer](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer): producer 재시도와 애플리케이션 수준 재발행의 중복 경계.
- [AWS transactional outbox](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html): DB 원본과 메시지 인계의 분리·멱등 소비.
