# 원본과 단계 최소 저장 및 Redis 완료 중복 차단

갱신일: 2026-09-25. [ADR-022](adr/ADR-022-처리-중-DynamoDB-선점과-완료-후-Redis-중복-차단.md)의 코드 적용 기록이다. 새 API 접수는 v2 경로를 사용한다. 공용 실행 중인 애플리케이션을 재배포했다는 뜻은 아니다.

## 1. 바뀐 흐름

```text
API → Kafka 접수 확인 → ORIGIN 조건부 Put
    → STEP 조건부 선점 → HTTP 1차 발송 / TCP 2차 발송
    → 접수 응답이면 STEP에 ACCEPTED 저장
    → 재시도 응답이면 고정 회차 명령을 Kafka에 저장 확인
       → 재시도 소비자가 STEP의 version·회차를 조건부 변경하고 발송
    → 성공 웹훅이면 STEP에 결과·최종 이벤트 저장 + ORIGIN 완료 보호를 한 트랜잭션으로 처리
    → Kafka 최종 이벤트 → PostgreSQL 이력·고객 통지·정리 예약을 한 트랜잭션으로 저장
    → Redis 완료 표식 설정 시도 → ORIGIN·STEP 조건부 삭제
    → Redis TTL 동안 동일 고객+ClientMsgId 새 처리 차단
```

물리 테이블은 `ORIGIN`, `STEP`으로 분리했다. [테이블 배치·쓰기 합산·이관 절차](36-원본과-단계-테이블-분리와-경로별-쓰기-내역.md)를 참고한다. ORIGIN 테이블 안에서는 기존 정렬 키 값 `META`를 사용한다. 원문을 별도 항목으로 한 번 더 복사하지 않는다. STEP도 기존 `ATTEMPT#...` 키를 사용한다. v2는 별도 `RECEIPT#...` 및 `FINAL` 항목을 만들지 않는다.

## 2. 같은 ClientMsgId의 재발송을 구분하는 방법

- `requestKey`: 기존 고객 ID + Idempotency-Key(ClientMsgId)의 해시 UUID. API 응답의 기존 `deliveryId` 필드는 호환성을 위해 이 접수 식별자를 반환한다.
- 실행 `deliveryId`: ORIGIN을 새로 만들 때 생성하는 UUID. 활성 요청의 재접수는 먼저 저장된 실행 UUID·원문·최초 시각을 그대로 사용한다.
- ORIGIN 키는 `DELIVERY#requestKey / META`, STEP 키는 `DELIVERY#실행UUID / ATTEMPT#단계UUID`다. STEP은 ORIGIN의 requestKey를 포함한다.
- 외부 업체 요청·웹훅·최종 이력의 deliveryId는 **실행 UUID**다. 고객 최종 결과에는 `requestKey`도 포함하므로 최초 접수 응답과 연결할 수 있다. API 응답 ID와 실행 ID를 같은 값으로 비교하면 안 된다.
- 허용된 재접수는 새 실행 UUID를 가진다. SQL의 기존 delivery_id 고유키를 유지해도 이전 이력·통지와 합쳐지지 않는다.

최초 STEP 생성은 ORIGIN이 존재하고 실행 UUID가 일치하며 완료 보호가 없는지를 같은 DDB 트랜잭션에서 확인한다. 삭제된 실행의 Dispatch 입력은 새로운 STEP을 만들 수 없다. 새 처리를 만들 수 있는 진입점은 Ingress의 요청 접수 경로다. 이전 회차의 STEP 업데이트는 version 조건으로 차단한다.

## 3. Redis와 DynamoDB 각각의 역할

Redis `delivery:due` Sorted Set은 처리 식별자와 다음 확인 시각을 저장한다. STEP이 없는 인입 시점에는 ORIGIN을 찾을 수 있는 requestKey로 인계 후보를 등록한다. PRESEND에서는 이 후보를 제거하고 실행 UUID와 실제 단계 deadline을 등록한다. 접수 응답만으로 웹훅 대기를 해제하지 않는다. 단계가 끝나면 deadline 대신 즉시 후속 처리를 확인할 후보로 바꾸며, 2차 선점 시 기존 규칙의 2차 deadline으로 교체한다. 전체 결과가 Kafka에 인계되면 일정에서 제거한다.

Redis 조회는 기본 1초 주기다. Redis 일정 소실·등록 실패·worker 중단은 DDB `lifecycle_due_v1`을 기본 30초마다 조회해 복구한다. ORIGIN의 복구 인덱스를 STEP 생성 직후 따로 지우지 않아 DDB 쓰기 한 번을 절약한다. ORIGIN은 결과 확정 때 인덱스를 함께 제거한다. 그 전까지 주기 조회·조건 실패가 추가될 수 있으며 아래 직렬 계측에는 포함하지 않는다.

업무 만료는 DDB에 저장된 **1차 최초 인입+3시간, 2차 1차 결과 판단+4시간**이다. Redis를 재등록하거나 Kafka를 재소비해도 연장하지 않는다. Sorted Set은 후보 검색만 수행하고 실제 상태·회차·deadline은 DDB 조건으로 판정한다. 조회 주기와 장애 복구 때문에 처리/통지가 벽시계 deadline에 정확히 실행된다는 보장은 없다. 장기 장애 후에는 원래 기한으로 만료 처리한다.

완료 후 `delivery:completed:requestKey`에는 원문 없는 요청 fingerprint를 둔다. PoC 기본 보관은 **최종 결과 확정 시각(finalizedAt)+24시간**, `DELIVERY_COMPLETED_RETENTION`으로 조정한다. 사용자 확정 보관 기간이 아닌 구현 기본값이다. 정리 재시도로 TTL을 새로 24시간 연장하지 않으며, 오래된 작업의 정리 시 이미 보관 기한이 지났으면 새 키를 만들지 않는다.

Redis가 없거나 오류이면 완료 차단을 우회할 수 있다. 그러나 활성 ORIGIN과 STEP의 DDB 조건을 반드시 통과해야 한다. DDB 오류는 발송 허가가 아니다. SQL 저장 후 Redis 표식 설정이 실패해도 정리는 진행한다. 이는 사용자가 허용한 완료 후 중복 가능성이다. TPS/Quota 장애 우회와 같은 의미의 처리 중 DDB 우회는 아니다.

Redis 조회와 DDB 선점은 하나의 트랜잭션이 아니다. Redis 표식이 없을 때 조회한 요청이 이전 정리 직후 DDB 선점을 통과하면 새 실행이 허용될 수 있다. 이후 늦게 이전 완료 표식이 나타나더라도 이미 선점된 원본을 삭제하거나 입력을 버리지 않는다. 이 경계까지 엄격한 완료 중복 차단이 필요하면 영속 완료 기록을 유지해야 하므로 이번 허용 정책과 구분한다.

## 4. 재시도 예약 쓰기를 없앤 방법

`delivery.retry.v1` 명령은 실행 이벤트, 단계, source version, source retryCount, target retryCount, notBefore, deadline을 포함한다. target은 source+1이며 최대 3이다. 생산자는 `acks=all`, idempotence 설정과 제한 시간 내 저장 확인을 사용한다. listener는 발행 확인 이전에 정상 반환하지 않는다.

재시도 소비자는 notBefore 이전에는 입력을 유지한다. 이후 STEP의 source version·회차·상태·기한을 조건부 확인해 목표 회차를 한 번 선점한다. 같은 명령이 여러 번 오거나 소비자가 재시작돼도 target을 새로 계산해 증가시키지 않는다. 성공 웹훅/만료/다른 회차가 먼저 이겼다면 명령은 외부 호출 없이 종료한다. HTTP와 TCP 모두 이 경로를 사용한다.

발송 실패 응답 및 재시도 가능한 실패 웹훅은 예약용 DDB Update를 하지 않는다. Kafka 인계 전 종료 또는 저장 확인 실패로 STEP이 PROCESSING에 남는 구간은 운영 확인·원래 deadline 만료 경로다. timeout이 외부 미수신을 증명하지는 않으므로 무응답 재시도에는 외부 중복 가능성이 있다. 외부 호출까지 원자적 exactly-once라고 주장하지 않는다.

재시도 선점 직후 종료한 경우에도 명령 재소비로 추가 호출하지 않는다. 최대 기한 안에 운영 확인하거나 만료한다. PoC의 지연 재시도는 별도 Kafka consumer의 backoff를 사용하므로 같은 파티션 뒤의 명령이 대기할 수 있다.

## 5. 웹훅과 최종 결과 인계

v2 웹훅은 STEP의 실행·업체·단계·invocation·version·deadline으로 판정한다. 없는 STEP은 로그만 남기고 버린다. 별도의 전역 receipt ID 장부는 없으며 마지막 반영 웹훅의 fingerprint·invocation을 STEP에 둔다. 서로 다른 실행에서 같은 업체 receipt ID를 썼다는 사실을 전역 충돌로 검사하는 v1 기능은 v2에서 제거했다.

성공 웹훅은 STEP 상태 변경, 불변 최종 이벤트, 완료 보호, 복구 일정 정리를 하나의 DDB 트랜잭션으로 저장한다. 2차 성공이면 1차 STEP 닫기도 포함한다. 실패·운영 확인 뒤의 만료는 주기 worker가 최종 이벤트를 terminal STEP에 저장한다.

v2는 Kafka ack를 별도 DDB Update로 저장하지 않는다. SQL 저장을 기다리는 STEP의 `publish_state=PENDING`과 복구 인덱스를 유지한다. SQL 인계가 늦으면 같은 최종 이벤트가 주기적으로 Kafka에 다시 발행될 수 있다. PostgreSQL은 동일 이벤트를 한 번만 이력·통지 예약으로 저장하고, 이미 진행한 고객 통지를 초기화하지 않는다. **SQL commit 자체가 인계 증거**이므로 그 결과와 STEP의 저장 결과가 일치하면 삭제할 수 있다.

정리 worker는 SQL 이력+통지+정리 예약을 확인한 뒤 해당 실행의 ORIGIN·STEP만 트랜잭션 삭제한다. DDB 삭제 후 SQL 정리 완료 표시에 실패해도 재시도에서 이미 삭제된 실행을 확인하고 완료할 수 있다. 그 사이 같은 requestKey의 새 ORIGIN이 생겨도 이전 정리 작업은 건드리지 않는다.

## 6. DynamoDB 계측과 시험

직렬 정상 경로, 성공 쓰기 API / 변경 항목(삭제 포함) 기준이다. 실제 WCU 청구량·전체 운영 상한·TPS가 아니다.

| 경로 | v1 기준선 | v2 |
|---|---:|---:|
| 1차 접수→성공 웹훅→SQL 이후 정리 | 8회 / 14항목 | 5회 / 7항목 |
| 1·2차 각 최초+3회 모두 무응답 | 23회 / 29항목 | 14회 / 18항목 |
| 8회 접수 응답과 실패/성공 웹훅 | 31회 / 53항목 | 21회 / 25항목 |
| 8회 웹훅 뒤 양 단계 운영 확인·만료 | 33회 / 55항목 | 24회 / 28항목 |

정상 5회: ORIGIN Put → STEP 선점 → STEP 접수 응답 Update → 성공 웹훅/결과 확정 트랜잭션(ORIGIN+STEP) → SQL commit 뒤 삭제 트랜잭션(ORIGIN+STEP). 자세한 필요 이유는 [사용 횟수 문서](33-DynamoDB-사용-횟수와-최소화-검토.md)에 있다.

**검증 결과:** 전체 Maven package 278개 통과 후 마지막 복구 키·구형 결과 해시 호환 변경을 관련 통합 시험으로 재검증했다. 최종 누적 보고서는 Java 282개, 실패·오류·skip 0개다. Python 증거 대조 시험 10개도 통과했다. [재현 정보와 계측 원본](검증-결과/2026-09-25-원본-단계-최소화-검증.json)을 남겼다.

`OriginStepDynamoDbTest`는 실제 DDB 호출 인터셉터로 네 경로를 계측한다. 그 밖에 동시 재시도 16개 중 한 번만 선점, 실제 Kafka 중복 명령/소비자 재시작, Kafka 발행 실패 후 결과 불명, 성공 웹훅이 예약 재시도를 차단, 삭제 후 늦은 Dispatch·웹훅 격리, SQL 완료 표시 실패 후 새 세대 보존을 검증한다. Ingress 시험은 실제 Redis 표식 유실과 활성 DDB 보호, 동시 접수 24건의 동일 실행 선택을 검증한다. SQL 시험은 세대별 이력·통지 분리를 확인한다.

## 7. 적용 순서와 검증 한계

1. SQL migration은 유지한다. 물리 테이블 분리 버전은 36번 문서의 전체 writer 중지·데이터 복사·대조 후 배포 절차를 따른다. 양 테이블에 GSI가 필요하며 구형 delivery_state 사용 앱과 혼용하지 않는다.
2. 각 worker에 동일한 REDIS_HOST/PORT/DATABASE를 지정한다. 운영용 Kafka retry topic은 replicas와 min.insync.replicas를 장애 목표에 맞춰 설정한다. 기본 1/1은 로컬 PoC 값이다.
3. `DISPATCH_LIFECYCLE_ENABLED=true`, `DELIVERY_CLEANUP_ENABLED=true` 및 결과 worker/고객 통지 설정을 적용해야 만료·정리까지 동작한다. 완료 TTL은 `DELIVERY_COMPLETED_RETENTION`, GSI 복구 주기는 `dispatch.lifecycle.recovery-poll-ms`로 조정한다.
4. 기존 v1 완료 META를 일괄 삭제하지 않는다. 기존 요청은 v1 호환 경로와 완료 표식 정책을 유지한다. 기존 완료 요청까지 새로운 보관 정책으로 전환하는 일괄 이관은 별도 작업이다.

이번 결과는 저장소·Kafka 기능 통합 시험과 쓰기 수 재계측이다. 새 코드의 부하 TPS·전체 시스템 장애 복구 시간·AWS 다중 AZ 무손실을 새로 입증한 결과가 아니다. PoC 증거 수집기는 requestKey→실행 UUID 매핑을 지원하도록 갱신했다.

## 공식 근거

- [DynamoDB 트랜잭션](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html): ORIGIN 보호와 STEP 변경/삭제를 원자적으로 묶는 근거다. 외부 HTTP/TCP·Redis·Kafka·SQL은 이 트랜잭션 밖이다.
- [DynamoDB 읽기·쓰기 단위](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html): API 호출 수, 항목 변경 수, 과금 단위가 다른 이유다. 큰 ORIGIN 일부 속성 변경도 큰 항목 기준 비용이다.
- [Spring Kafka 발행 결과 확인](https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html): 비동기 발행 결과를 확인한 뒤 입력 처리를 넘기는 근거다.
- [Redis keyspace notification](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/): TTL 알림은 정확한 작업 실행 시각·유실 없는 큐를 보장하지 않는다. 이 구현은 알림 대신 Sorted Set 조회와 DDB 복구 인덱스를 쓴다.
- [Redis WAIT](https://redis.io/docs/latest/commands/wait/)와 [영속화](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/): 완료 표식을 100% 무손실로 간주하지 않는 근거다. 사용자는 그 소실 후 조건을 통과한 재발송을 허용했다.
