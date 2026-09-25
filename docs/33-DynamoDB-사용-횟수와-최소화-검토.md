# DynamoDB 사용 횟수와 최소화 검토

갱신일: 2026-09-25. 새 API의 v2 ORIGIN/STEP 경로 기준이다. [구현·시험 기록](35-원본과-단계-최소-저장-및-Redis-완료-중복-차단.md)과 [최신 정책](adr/ADR-022-처리-중-DynamoDB-선점과-완료-후-Redis-중복-차단.md)을 함께 본다. 이전 8회/14항목과 33회/55항목은 v1 비교 기준선이다.

## 정상 1차 성공: 쓰기 API 5회, 항목 변경 7회

| 순서 | 실행 시점 | DDB 변경 | 필요한 이유 |
|---|---|---|---|
| 1 | Kafka 접수 후 Ingress | ORIGIN Put 1 | 같은 고객+ClientMsgId의 현재 실행을 하나로 선택. 최초 인입 시각·원문·대체 발송 허용을 보존한다. |
| 2 | HTTP 1차 발송 직전 | STEP 생성 Update 1 + ORIGIN 조건 검사 | ORIGIN의 실행 UUID·미완료 조건과 STEP 최초 선점을 한 트랜잭션으로 확인. provider·deadline·version·retryCount·lease 저장. |
| 3 | HTTP 접수 응답 | STEP Update 1 | ACCEPTED 및 업체 접수 시각 저장. 단순 접수를 최종 성공으로 처리하지 않는다. |
| 4 | 정상 성공 웹훅 | STEP Update 1 + ORIGIN Update 1 | STEP에 DELIVERED·웹훅 회차·최종 이벤트·SQL 인계 대기를 저장하고 ORIGIN에 완료 보호를 세운다. 만료/다른 결과와 조건부 경합. |
| 5 | SQL 이력·통지·정리 예약 commit 확인 후 | ORIGIN Delete 1 + STEP Delete 1 | 원문과 실행 정보를 원자 삭제. 완료 후 중복 차단은 Redis TTL에 맡긴다. |
| 합계 | | **Put 1 + Update 4 + Delete 2 = 7항목, 성공 쓰기 API 5회** | 조건 검사 자체는 항목 변경에 포함하지 않는다. |

DDB의 `UpdateItem`은 없는 STEP을 조건부 생성할 수도 있다. 위 ‘STEP 생성 Update’는 실행 상태의 최초 insert에 해당한다. 트랜잭션 API 한 번이 두 항목을 변경하면 ‘성공 쓰기 API 1회 / 변경 항목 2개’다. 삭제도 변경 항목에 포함한다.

이후 고객 HTTP 결과 묶음 전송·최대 20회 재전송은 PostgreSQL만 사용하므로 DDB 쓰기는 추가하지 않는다.

## 복잡한 대표 경로와 기존 비교

| 경로 | v1 API/항목 | v2 API/항목 | v2 Put/Update/Delete |
|---|---:|---:|---:|
| 정상 1차 성공 | 8 / 14 | **5 / 7** | 1 / 4 / 2 |
| 양 단계 총 8회 무응답 | 23 / 29 | **14 / 18** | 1 / 14 / 3 |
| 총 8회 접수 응답 및 웹훅 | 31 / 53 | **21 / 25** | 1 / 21 / 3 |
| 총 8회 웹훅, 양 단계 운영 확인 뒤 만료 | 33 / 55 | **24 / 28** | 1 / 24 / 3 |

‘24회’는 검증한 복잡한 대표 업무 경로다. 전체 운영에서의 절대 상한이 아니다. 재전달, 상태 경합, 조건 실패, 복구 worker, 저장 응답 유실, 운영 변경에 따라 호출은 늘어난다. 동일 이벤트가 반복 조회되는 비용도 별도다. v2 네 직렬 경로의 실패한 쓰기 호출은 0회지만 동시 경합 시험은 조건 실패를 의도적으로 발생시킨다.

실제 DDB Local SDK interceptor 계측은 `DynamoDbWriteBudgetTest`(v1), `OriginStepDynamoDbTest`(v2)로 재현한다. 설정·정리 fixture 호출은 계측 밖이다. Ingress의 ORIGIN Put은 이 시험 모듈 안에서 동일 관련 필드로 구성한다. 실제 Kafka/SQL/Redis 연계의 정확성 시험은 별도로 수행한다.

## 줄인 구간

- 재시도 실패 응답을 STEP의 RETRY_SCHEDULED로 별도 저장하지 않는다. Kafka 명령의 목표 회차를 고정하고 실제 실행 때 STEP만 조건부 갱신한다.
- 신규 receipt marker를 따로 생성·삭제하지 않는다. STEP의 실행·invocation·상태 및 마지막 fingerprint로 판정한다.
- 성공 웹훅 반영과 최종 결과 저장·완료 보호를 같은 트랜잭션으로 묶는다.
- 별도 FINAL Put/Delete를 없앴다. terminal STEP에 최종 이벤트를 둔다.
- META 복구 인덱스를 넘기는 단독 Update와 2차 대기용 단독 Update를 v2에서 제거했다.
- Kafka 최종 이벤트 ack용 Update를 없앴다. SQL commit을 삭제 근거로 사용하며 SQL 인계 전에는 같은 이벤트가 재발행될 수 있다.
- 완료 META 교체 Put 대신 ORIGIN을 삭제한다. 장기 DDB 완료 표식이 없으므로 Redis 소실 후 조건을 통과한 재발송은 허용된다.

## 왜 남은 DDB 사용을 유지하는가

ORIGIN 없이 Redis만으로 처리 중 중복을 막으면 Redis 소실 시 같은 ClientMsgId의 진행 중 요청도 새 실행으로 분리될 수 있다. 원문은 Kafka에서 선택 조회하는 별도 복구 설계를 두지 않았으므로 SQL 인계 전에는 ORIGIN에 유지한다.

STEP 선점 없이 외부 호출하면 동시 소비자가 모두 발송할 수 있다. 버전·회차·기한 조건은 Redis 유실과 무관하게 필요하다. 접수 응답 저장을 없애면 정상 접수도 결과 불명 PROCESSING으로 남아 운영 확인이 늘어난다.

최종 결과를 내구성 있게 확정하기 전에 SQL/Kafka/Redis에 흩어져 진행하면 웹훅·만료·2차 전환이 서로 다른 결론을 전달할 수 있다. STEP의 최종 이벤트와 ORIGIN 보호를 묶고, SQL 인계 후 실제 삭제하는 경계는 유지한다.

현재 사용자 정책과 저장 모델 안에서 쓰기를 줄인 구현이며 수학적인 최소라는 주장은 아니다. 큰 ORIGIN의 완료 보호 Update 비용을 줄이려면 원문과 제어 정보를 분리하는 등의 추가 모델 변경이 필요하다. 이는 쓰기 횟수 증가와 항목 크기 감소를 함께 비교해야 한다.

## 호출·변경·과금은 다르다

조건 검사·강한 읽기·GSI 조회는 읽기 비용이고, 실패한 조건도 무료가 아니다. 트랜잭션 쓰기는 일반 쓰기와 과금 단위가 다르다. 작은 속성 Update라도 항목 크기가 비용에 영향을 주며, 삭제도 기존 항목 크기를 기준으로 한다. GSI 유지 비용도 별도다. 따라서 정상 ‘5회’가 5 WCU라는 뜻은 아니다.

근거: [DynamoDB 트랜잭션](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html), [읽기·쓰기 단위](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html), [읽기 일관성](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html).
