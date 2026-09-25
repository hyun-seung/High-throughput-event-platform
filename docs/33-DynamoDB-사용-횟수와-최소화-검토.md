# DynamoDB 사용 횟수와 최소화 검토

검토일: 2026-09-25. 구현 기준: `3b7f21769ea5089851f06692fea856c0006f9586`. 이 문서는 현재 사용량을 설명하고 개선 후보를 제시합니다. 업무 로직은 변경하지 않았습니다.

## 결론

정상 요청의 **1차 HTTP 접수 확인까지는 쓰기 API 3회·항목 변경 3회**입니다. **성공 웹훅 → 최종화 → Kafka 인계 → SQL 이력·고객 통지 예약 → 원본 정리까지는 성공 쓰기 API 8회·항목 변경 14회**입니다. 14회는 Put/Update 11회와 Delete 3회의 합이며, 14개의 서로 다른 레코드를 만든다는 뜻이 아닙니다.

현재 구현은 **DynamoDB 사용이 최소인 설계는 아닙니다.** 멱등 선점뿐 아니라 원본 복구, 재시도·만료 예약, 웹훅 충돌 검사, 최종 이벤트의 내구성 인계를 담당합니다. 각각 목적이 있지만, 같은 항목에 여러 단계로 쓰는 부분과 불필요한 선점 시도·반복 조회를 줄일 여지가 있습니다. 특히 작은 속성만 바꾸더라도 큰 원본 META 전체 크기를 기준으로 쓰기 비용이 계산되는 점이 중요합니다.

## 횟수를 구분하는 기준

| 단위 | 의미 |
|---|---|
| 쓰기 API 호출 | PutItem·UpdateItem·TransactWriteItems 등 SDK 호출 수. 트랜잭션도 API 1회 |
| 성공한 항목 변경 | 트랜잭션 내부 Put·Update·Delete를 각각 1회로 계산. 조건 확인은 변경에서 제외 |
| 실패한 쓰기 API | 조건 불일치·장애로 실패한 호출. 성공 변경은 0이어도 비용이 없다는 뜻은 아님 |
| 읽기 API | GetItem·Query 등. 트랜잭션 내부 ConditionCheck와 별도로 표시 |
| AWS 용량·비용 | 항목 크기, 일반/트랜잭션, GSI, 실패·SDK 재시도에 따라 달라짐. 위 횟수와 같지 않음 |

아래 대표값은 **현재 코드의 1차/2차 처리와 주기 최종화·정리를 활성화한 흐름을 직렬로 실행**한 값입니다. 같은 작업을 여러 워커가 경쟁하지 않고, 외부 접수 응답을 먼저 저장한 뒤 웹훅을 처리하며, 주기 복구의 색인 정리는 각 1회 수행합니다. 아직 기본 비활성화인 기능을 공용 실행 환경에서 켰다는 뜻은 아닙니다.

## 정상 경로: 어디에 왜 쓰는가

| 단계·메서드 | 성공 쓰기 API | 항목 변경 | 저장 목적·없을 때의 문제 |
|---|---:|---:|---|
| API의 Kafka-first 접수 | 0 | 0 | 원본 접수 보장은 Kafka ack. API에서 DDB를 중복 기록하지 않음 |
| Ingress `saveOrLoad` | 1 | META Put 1 | 동일 요청 내용·최초 인입 시각 고정, 웹훅 후속 발송·주기 복구의 원본. 현재 주기 복구가 Kafka 원본을 역조회하지 않으므로 삭제 불가 |
| 최초 발송 `claimRoute` | 1 | Attempt Update 1 + META ConditionCheck 1(변경 아님) | 외부 호출 전 단일 실행자 선점, 완료 후 재선점 차단. 검사와 생성 사이 경합 때문에 같은 트랜잭션 사용 |
| HTTP 접수 응답 `markAccepted` | 1 | Attempt Update 1 | 접수됨을 기록. 없으면 재전달 시 PROCESSING이 남아 정상 접수도 운영 확인으로 감 |
| 성공 웹훅 `ReceiptResultRepository.apply` | 1 | Attempt Update 1 + Receipt marker Put 1 | 결과 상태와 처리한 웹훅의 충돌/중복 판단을 함께 기록. marker 목록은 같은 Update에 포함 |
| 복구 색인 인계 `releaseMeta` | 1 | META Update 1 | 최초 미발송 복구 대상에서 제외하고 Attempt 색인에 맡김. 현재 구조에서 생략만 하면 due=0 원본이 반복 조회됨 |
| 전체 최종화 `finalizeDelivery` | 1 | META Update 1 + FINAL Put 1 + Attempt Update 1 | 완료 선점 차단, 고정된 최종 이벤트, 닫힌 실행 상태를 함께 확정. 중간 종료에도 Kafka 인계 재개 가능 |
| Kafka ack 확인 `published` | 1 | FINAL Update 1 | 발행 재시도 대상에서 제거. 현재 compactor의 삭제 조건이기도 함 |
| SQL 이력·고객 HTTP 통지/재전송 | 0 | 0 | 이력·통지·정리 예약 및 최초+20회 고객 통지 상태는 PostgreSQL에 저장 |
| 원본 정리 `DeliveryCompactor.compact` | 1 | META Put 1 + FINAL/Attempt/Receipt Delete 각 1 | SQL 인계 후 원문·활성 데이터 정리, 작은 완료 기록 유지. 전체 교체·삭제의 부분 성공 방지 |
| **합계** | **8** | **14 = Put/Update 11 + Delete 3** | 성공한 META ConditionCheck 1회 별도 |

1차 접수까지 3회, 웹훅 반영까지 4회/5항목, 최종 Kafka 발행 확인까지 7회/10항목, 정리까지 8회/14항목입니다. 고객 통지 완료 여부 때문에 DDB 쓰기가 추가되지는 않습니다.

계측한 정상 순서의 읽기는 11회입니다. Receipt 판정 2 Get, 최종화 조정 6 Get, 정리 2 Get+1 Query입니다. 성공한 쓰기 API 8회와 합치면 **SDK 호출 19회**이며, 최초 선점의 ConditionCheck는 그 트랜잭션 내부 동작입니다. 주기 GSI 검색·중복 실행·재전달을 포함한 실제 운영 총 호출 수로 고정해서는 안 됩니다.

## 최대 경로: 재시도 한도와 DB 호출 한도는 다름

1차 최대 4회, 허용된 2차 최대 4회로 외부 호출은 정상적인 상태 전이에서 총 8회입니다. 각 호출 회차의 웹훅은 상태·receipt_invocation 검사로 한 번만 새로운 상태에 반영합니다. 따라서 한 번의 업무 흐름에 새로 반영되는 Receipt marker는 최대 8개입니다. 늦은 결과와 닫힌 회차의 추가 웹훅은 새 marker를 만들지 않습니다.

| 검증 경로 | 성공 쓰기 API | Put/Update | Delete | 항목 변경 합계 | 조건 실패 쓰기 API 추가 |
|---|---:|---:|---:|---:|---:|
| 1차 접수 + 성공 웹훅 + 최종화·정리 | 8 | 11 | 3 | **14** | 0 |
| 1·2차 각 4회 무응답, 웹훅 없음, 실패 최종화·정리 | 23 | 26 | 3 | **29** | 6 |
| 1·2차 각 4회 접수 후 웹훅, 1차 마지막 전환·2차 마지막 성공 | 31 | 42 | 11 | **53** | 6 |
| 위 8회 웹훅 중 각 경로 마지막이 판단 불가 → 운영 확인 → 만료 | 33 | 44 | 11 | **55** | 6 |

마지막 두 경로에서 처음 세 웹훅은 RETRY_1S입니다. 53회 경로의 네 번째는 1차 FALLBACK, 2차 DELIVERED입니다. 55회 경로의 네 번째는 미정의 실패 코드로 REVIEW_REQUIRED에 들어가고, 각 단계가 만료됩니다. 1차 만료 시 허용된 2차로 전환합니다. 시간은 테스트 시계로 진행했으며 실제 7시간 기다린 부하 시험이 아닙니다.

55회의 산식은 다음과 같습니다.

| 항목 | 성공 쓰기 API | 항목 변경 |
|---|---:|---:|
| 원본 저장 | 1 | 1 |
| 최초 선점 2회 + 재시도 선점 6회 | 8 | 8 |
| 외부 접수 결과 기록 8회 | 8 | 8 |
| 회차별 Receipt 8회 | 8 | 16 |
| 1차/2차 만료 전환 | 2 | 2 |
| 2차 업체·deadline 고정 | 1 | 1 |
| META 색인 해제 + 1차의 2차 대기 시각 갱신 | 2 | 2 |
| 전체 최종화: META+FINAL+1차/2차 닫기 | 1 | 4 |
| Kafka 발행 확인 | 1 | 1 |
| META 교체+FINAL/Attempt 2개/Receipt 8개 삭제 | 1 | 12 |
| **합계** | **33** | **55** |

**55회는 각 업무 경계를 한 번씩 처리하는 위 대표 최대 상태 전이의 합계입니다. 운영 전체의 절대 상한은 없습니다.**

- 재시도할 때도 현재 `claimRoute`는 최초 생성 트랜잭션을 먼저 시도합니다. 기존 Attempt 때문에 실패한 뒤 META/Attempt를 읽고 예약 재시도를 선점합니다. 6회 재시도에는 최소 6번의 실패 트랜잭션이 붙습니다. 마지막 표의 쓰기 API 시도는 33+6=39회입니다.
- 재시도 시각 전 Kafka 재전달은 이 실패 경로와 읽기를 반복합니다. 10초 대기 중 여러 번 재전달될 수 있습니다.
- Dispatch 오류 처리는 유실 방지를 위해 무제한 재전달이며, 최종화/정리도 장애 동안 계속 재시도합니다. 고객이나 Kafka가 보내는 중복 입력 횟수도 외부 발송 재시도 3회에 포함되지 않습니다.
- `releaseMeta`는 과거 조회 결과를 가진 다른 워커가 다시 REMOVE할 수 있습니다. 따라서 상태가 실질적으로 바뀌지 않는 성공 쓰기도 추가될 수 있습니다.
- GSI의 지연 반영과 복수 워커 조회로 완료 후보를 반복 처리할 수 있습니다. 늦은 Receipt는 쓰기를 안 해도 조회 비용이 생깁니다.

## 각 추가 저장은 꼭 DynamoDB여야 하는가

| 부분 | 필요한 보장 | 현재 위치에 대한 판단 |
|---|---|---|
| 선점·version·횟수·deadline | 외부 호출 전에 실행 권한과 재시도 예산을 내구성 있게 확정 | 현재 DDB가 발송 판단의 기준이므로 유지해야 함. 메모리/Redis 캐시만으로 대체 불가 |
| 원본 META | 중복 요청 검증, 최초 시간 유지, 원본 Kafka offset과 분리된 후속 복구 | 내구성 원본은 필요하지만 DDB의 큰 META일 필요는 없음. 다른 저장소/별도 BODY 설계는 참조 생성·보관·장애 복구까지 함께 설계해야 함 |
| Receipt marker | 현재 구현의 업체 receiptId 충돌 검증·중복 후속 처리 | 별도 item 자체가 사용자 발송 멱등성의 필수 조건은 아님. Attempt 안에 처리 근거를 모을 수 있으나 현재 전역 receiptId 충돌 검증 범위가 달라짐 |
| 재시도·만료 GSI | Scan 없이 처리할 작업을 재발견 | 현재 일정 복구에 필요. Kafka 별도 지연 소비·내구성 스케줄러로 교체 가능하지만 비용·복잡도가 다른 곳으로 이동 |
| FINAL outbox | 최종 상태 commit과 Kafka 발행 사이 종료 복구 | 내구성 인계는 필요. 별도 FINAL item은 설계 선택이며 작은 제어 항목과 통합 가능성을 검토할 수 있음 |
| PUBLISHED 표시 | 반복 발행 중지 및 현재 정리 전제 | 현재 코드에서는 필요. SQL 수신 확인을 발행 완료 근거로 활용하면 개선 가능하지만 지연/반복 발행 중지와 구버전 이관까지 바꿔야 함 |
| 작은 완료 META | 정리 후 과거 요청·다른 업체 선점의 재발송 차단 | 현재 선점 트랜잭션과 같은 저장소에서 확인해야 경합이 없음. 보관 기간 합의 없이 제거 금지 |
| 이력·고객 통지·정리 예약 | 발송 후 내구성 결과 보관과 후속 재시도 | 이미 SQL 사용. 고객 통지 20회 재전송 때문에 DDB에 횟수를 쌓지 않음 |

DynamoDB 자체가 논리적으로 유일한 구현 수단은 아닙니다. 발송 원장과 선점을 SQL로 통합하는 대안도 가능하지만, 현재 구조에서 완료 기록만 SQL로 옮기고 별도 조회 후 DDB 선점을 허용하면 교차 저장소 경합을 해결하지 못합니다. 최소화 목표는 불필요한 경계·크기·조회 제거이며, 내구성 근거 삭제가 아닙니다.

## 최소화 개선 우선순위

아래는 검토 결과이며 이번에는 적용하지 않았습니다. 횟수 감소 예상은 위 직렬 기준에 한정합니다.

| 우선순위 | 개선 후보 | 기대 효과 | 함께 검증해야 할 조건 |
|---|---|---|---|
| 1 | 최종화 가능한 경로에서는 먼저 `releaseMeta`하지 않고 최종화 트랜잭션의 REMOVE 활용 | 정상 완료의 별도 Update 1회·항목 변경 1회 감소 후보: 8→7 API, 14→13항목. 큰 META 전체 갱신 1회 절약 | 아직 처리 중인 원본의 due=0 반복 조회는 계속 해제해야 함. 최종화 실패 시 원본 복구 근거 유지 |
| 1 | 2차가 이미 terminal이면 `waitForSecondary` 생략 | 2차 종료 조정에서 Update 1회 감소 후보 | 2차가 진행 중이면 부모의 즉시 재조회 방지를 위한 due 이동 유지 |
| 1 | 재시도/재전달 경로에서 처음부터 현재 상태를 확인해 불필요한 최초 선점 트랜잭션 회피 | 재시도 6회의 실패 트랜잭션 및 대기 중 반복 호출 감소 후보 | 최초 정상 경로에 Get을 무조건 추가할지 비교. 최종 생성은 반드시 완료 확인과 원자적으로 묶음 |
| 1 | 조정 함수의 중복 META 조회와 정리의 Get/Get/Query 겹침 축소 | 읽기 API와 큰 원본 전송 감소 후보 | 조건부 최종 쓰기 유지. Query가 원자 스냅샷이라는 가정 금지 |
| 2 | 큰 payload와 자주 갱신하는 작은 제어 항목 분리 | 완료 표시·색인 해제·선점 검사에서 큰 항목의 용량 비용 감소 | item/API 수는 늘 수 있음. 원본·제어 인계 원자성, 삭제 시 부분 실패, 이관 검증 필요 |
| 2 | Receipt 중복 근거를 Attempt에 묶거나 더 좁은 멱등 조건 활용 | 웹훅마다 별도 marker Put/삭제 및 트랜잭션 범위 축소 후보 | 이전 회차 replay, 내용 충돌, 업체 receiptId가 다른 Delivery에 재사용되는 경우의 현재 검증 범위 유지 여부 |
| 3 | FINAL·작은 제어 항목 통합, SQL 인계 확인 활용 | 최종화·발행 확인·정리의 항목 쓰기 감소 후보 | 이미 큰 META에 합치면 비용이 오히려 커짐. Kafka 확인 유실·SQL 장애·구버전과의 동시 실행 검증 |

단순히 API를 묶는 것이 비용 절약은 아닙니다. 예를 들어 최초 선점의 ConditionCheck를 META Update로 바꾸어 `releaseMeta`를 합치면 API는 줄어도 큰 META에 트랜잭션 쓰기가 생겨 용량 비용이 늘 수 있습니다. BatchWrite도 항목별 비용을 없애지 않으며 기존 조건부 원자 정리를 대체하지 못합니다.

## AWS 비용 해석과 공식 근거

검토일 2026-09-25, AWS 현행 개발자 가이드 기준입니다.

| 공식 사실 | 현재 코드에서의 의미 |
|---|---|
| [읽기·쓰기 용량](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html): 일반 쓰기는 1KB 단위, 트랜잭션 쓰기는 같은 크기에 두 배. Update/교체 Put은 전후 중 큰 item 크기, Delete는 삭제 item 크기 기준 | 속성 한두 개 변경이나 작은 완료 META로 교체해도 기존 큰 원본 비용이 반영됨. 원본 정리는 장기 저장량을 줄이지만 정리 자체가 무료가 아님 |
| 같은 문서: 조회 속성 제한은 읽기 용량 산정 크기를 줄이지 않음 | projection은 네트워크 양을 줄일 뿐. payload 분리·읽기 횟수 감소와 구별 |
| [트랜잭션 동작](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html): 원자 처리, 준비/commit 비용, 취소된 트랜잭션도 용량 소비 | API 1회를 항목 쓰기 1회나 비용 1단위로 해석하지 않음. 재시도 선점의 의도된 실패도 측정 대상 |
| [GSI](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html): 인덱스 갱신 비용은 별도이며 최종적 일관성으로 반영 | lifecycle_due 변경·색인 제거 비용과 반복 후보 조회를 별도로 산정 |
| [조건부 쓰기](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html): 조건 충족 시만 변경 | 실행 전 선점·version·완료 확인 조건은 횟수를 줄이려고 제거할 수 없음 |
| [TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html): 만료 이후 실제 삭제까지 지연 가능 | 정확한 업무 만료, SQL 인계 확인, 장기 완료 멱등성을 TTL만으로 대체하지 않음 |

정상 14항목 변경 중 일반 쓰기 4항목, 트랜잭션 쓰기 10항목입니다. **모든 관련 항목이 쓰기 전후 각각 1KB 이내라고 가정하면 성공한 기본 테이블 쓰기만 4+10×2=24 write units**입니다. ConditionCheck·Get/Query, GSI, 실패 호출, SDK 재시도는 별도입니다. 실제 항목 크기와 소비 용량을 수집하지 않았으므로 24를 실제 요청당 청구량으로 제시하지 않습니다.

주기 스케줄러는 16개 shard를 기본 fixed-delay 1초마다 조회합니다. 정상 tick 하나당 GSI Query 16회가 공유 비용으로 발생하며 실행 시간·실패에 따라 초당 값은 달라집니다. 요청 한 건의 쓰기 표에 넣을 수 없지만 저부하·다중 replica에서도 비용이 생깁니다.

현재 `delivery.dynamodb.duration`은 SDK 논리 호출 단위의 횟수/시간이며, 개별 wire 재시도나 소비 용량을 보여주지 않습니다. 다음 비교에서는 `ReturnConsumedCapacity=INDEXES`, 실제 item 크기, CloudWatch 기본 테이블/GSI 용량, 논리 호출/실패 호출을 구분해 수집해야 합니다. DynamoDB Local 수치로 운영 AWS 과금을 확정하지 않습니다.

## 검증과 코드 위치

신규 `DynamoDbWriteBudgetTest` 4건을 실제 DynamoDB Local에서 통과했습니다. SDK ExecutionInterceptor로 성공한 Put/Update/Delete 및 트랜잭션 내부 작업과 실패 호출을 셌습니다. 테스트용 테이블 준비·검증 후 삭제는 계측에서 제외했습니다. Ingress는 다른 모듈이므로 동일한 단일 conditional META Put fixture로 재현했고, 이후 실제 발송 저장소·Receipt 저장소·최종화 서비스·compactor를 실행했습니다. HTTP/TCP·Kafka·SQL 전체 경로 시험이나 AWS 과금 시험은 아닙니다. SQL 인계 전제는 기존 통합 시험에서 검증하며 이번 계측에서는 명시적으로 compactor를 호출합니다.

[계측 결과·명령·시험/소스 해시](검증-결과/2026-09-25-DynamoDB-쓰기-횟수-검증.json)를 함께 보관합니다. fixture의 읽기 수는 조정 실행 순서에 따라 바뀌므로 전역 최대값으로 사용하지 않습니다.

```bash
DYNAMODB_TEST_ENDPOINT=http://localhost:18000 \
JAVA_HOME=/path/to/jdk-21 ./mvnw -q -pl dispatch-worker -am \
-Dtest=DynamoDbWriteBudgetTest -Dsurefire.failIfNoSpecifiedTests=false test
```

- [Ingress 원본·중복 확인](../delivery-ingress-worker/src/main/java/event/delivery/ingress/repository/DeliveryRepository.java)
- [선점·응답·재시도·2차 계획](../dispatch-worker/src/main/java/event/delivery/dispatch/repository/DispatchAttemptRepository.java)
- [Receipt 원자 반영](../dispatch-worker/src/main/java/event/delivery/dispatch/receipt/ReceiptResultRepository.java)
- [색인 인계·만료·최종화·발행 확인](../dispatch-worker/src/main/java/event/delivery/dispatch/lifecycle/LifecycleRepository.java)
- [조정 호출 순서](../dispatch-worker/src/main/java/event/delivery/dispatch/lifecycle/LifecycleService.java)
- [원본 정리](../event-common/src/main/java/event/common/lifecycle/DeliveryCompactor.java)
- [횟수 계측 시험](../dispatch-worker/src/test/java/event/delivery/dispatch/lifecycle/DynamoDbWriteBudgetTest.java)
