# 주기 만료와 요청 최종화 및 Kafka 인계

2026-09-25. 1차·2차 미수신 만료, Attempt 이전 원본의 재개, 요청 전체 최종화와 `DeliveryFinalized` Kafka 인계를 구현했다. 작은 완료 기록과 미인계 상태를 같은 DynamoDB 항목에 남긴다. 저장소 선택·추가 비용·공식 문서 근거는 [ADR-013](adr/ADR-013-주기-복구와-최종-결과의-내구성-인계.md)에 분리했다.

기존 데이터 이관을 건너뛰지 않도록 `DISPATCH_LIFECYCLE_ENABLED=false`가 기본이다. 이번 작업에서 기존 장기 실행 서비스는 자동 배포·활성화하지 않았다.

## 상태 판정

| 현재 상태 | 주기 작업 |
|---|---|
| META만 존재 | 같은 원본 시각·flag로 기존 Dispatch 실행. 기한을 새로 시작하지 않음 |
| PROCESSING·ACCEPTED·REVIEW_REQUIRED | 기한 전에는 결과를 추측해 재발송하지 않음. `now >= deadline`이면 version 조건으로 만료 결정 |
| RETRY_SCHEDULED | 저장된 next_attempt_at 이후 기존 조건부 선점·최대 3회 재시도 규칙으로 재개. 기한 경과 시 만료 |
| 1차 만료 + fallbackAllowed=true | 1차 deadline을 판단 시각으로 고정해 2차 기한 계산, 기존 TCP 발송 경로로 연결 |
| 1차 실패 전환 + fallbackAllowed=true | 기존 전환 대상 사유에만 2차 연결. 영구 거절·명시적 재시도 코드 소진을 임의 전환하지 않음 |
| 2차 만료 | 3차 발송 없이 전체 EXPIRED |
| DELIVERED | 뒤늦은 복구 시각이 전체 기한 이후여도 이미 저장된 성공 유지 |
| 최종 가능한 DECISION_PENDING | 남은 대체 단계가 없으면 FAILED 또는 EXPIRED |

1차 기한은 인입 기준 3시간, 2차는 최초 1차 결과 판단 기준 4시간 설정을 유지한다. 자동 시험은 서로 다른 2초·3초로 줄였다. 기한은 기존 저장 형식인 epoch millisecond로 판정한다. 장기 중단 후 1차·2차 모두 지난 경우 2차 Attempt의 만료 기록은 만들지만 외부 TCP 호출은 하지 않는다. 원래 Worker의 늦은 저장은 만료 전이의 version 증가로 거절된다. 외부 호출 직전 멈춘 Worker가 뒤늦게 실제 호출하는 문제까지 외부 효과 exactly-once로 보장하는 것은 아니다.

웹훅 반영과 만료가 경합하면 기존 상태·version 조건이 승자를 정한다. 이미 반영된 성공은 유지하고, 만료로 닫힌 단계의 늦은 웹훅은 기존 폐기 경로를 따른다. 운영 확인 상태도 deadline을 넘겨 최종 결과를 미루지 않는다. 운영 조회·수동 조치 API는 아직 없다.

## 내구성 있는 탐색과 완료 기록

`delivery_state`에 `lifecycle_due_v1` GSI 하나를 추가했다. `lifecycle_bucket` 문자열과 `lifecycle_due` 숫자를 키로 사용하며 projection은 KEYS_ONLY다. 16개 bucket은 저장 스키마의 일부이므로 숫자를 임의로 바꾸면 기존 항목을 놓칠 수 있다.

- META: 최초 저장에 due=0을 포함한다. Dispatch 전 정체도 찾는다. Attempt가 존재하면 META 색인 속성만 제거하고 본문은 유지한다.
- Attempt: 최초 claim과 기존 재시도·웹훅 갱신에 기한 또는 재개 시각을 포함한다. DELIVERED·최종 판단은 due=0으로 최종화를 깨운다.
- 부모의 2차 대기: 자식 Attempt가 생긴 뒤 부모의 due를 고정된 자식 deadline으로 옮긴다. 자식의 독립 색인이 먼저 성공·재시도·만료를 깨울 수 있다.
- FINAL: 최종 결과와 PENDING을 원자적으로 생성하며 종료할 Attempt 색인을 함께 제거한다. Kafka ack 뒤 PUBLISHED로 바꾸고 FINAL 색인도 제거한다.

폴링은 Scan 없이 shard별 Query와 페이지 이동으로 동작한다. GSI는 후보 목록이며 업무 판정은 strong read·조건부 쓰기로 한다. 기본 poll은 1초, Worker 동시성 4, 페이지 크기 100이다. 작업량이 동시성을 넘으면 메모리 대기열을 늘리지 않고 다음 poll에서 이어간다. 오류 항목 뒤 페이지와 다른 shard도 진행하며 같은 프로세스에서는 같은 Delivery 작업을 중복 실행하지 않는다. 여러 프로세스의 경쟁은 DB 조건으로 보호한다.

최종 결과 JSON에는 eventId·deliveryId·tenantId·deliveryType·DELIVERED/FAILED/EXPIRED·사유·마지막 단계/업체·인입/결과/최종화/기한 시각을 담는다. 메시지 본문은 복제하지 않는다. `resultAt`은 성공 웹훅 수신 또는 실패 판단 시각이고, 만료는 원래 deadline이다. `finalizedAt`은 실제 최초 최종화 저장 시각이다.

## Kafka 인계와 실패 경계

기본 topic은 `delivery.finalized.v1`, key는 deliveryId다. Producer는 acks=all·idempotence를 켜며 broker ack를 기다린다. 개발 topic 기본값은 3개 partition·replica 1·minISR 1·7일 보관이다. 운영 다중 AZ 설정을 대신하지 않는다.

| 실패 위치 | 남는 근거·복구 |
|---|---|
| 만료 저장 실패 | 기존 활성 Attempt·복구 색인 유지 |
| 최종화 트랜잭션 실패 | 전체 미반영 또는 이미 커밋된 FINAL 재조회 |
| FINAL 저장 후 Kafka 발행 전 종료 | PENDING 색인으로 재발행 |
| Kafka 거절·통신 실패 | PENDING 유지, 외부 발송을 다시 하지 않음 |
| Kafka ack 이후 DB 확인 저장 실패 | 같은 eventId·같은 본문을 다시 발행할 수 있음 |
| PUBLISHED 이후 중복 요청 | FINAL과 기존 Attempt를 유지해 외부 재발송 차단 |

최종 이벤트의 물리적 Kafka record는 중복될 수 있다. 후속 이력·고객 통지는 eventId 기준으로 멱등 처리해야 한다. PUBLISHED는 고객 통지 완료가 아니다. 이력과 고객 통지의 내구성 인계가 아직 없으므로 원본·Attempt·receipt marker·FINAL을 삭제하지 않는다. Kafka retention을 넘긴 후 PUBLISHED의 자동 재인계도 후속이다.

## 이관과 활성화

신규 테이블 초기화는 GSI를 함께 생성한다. 기존 테이블 초기화는 자동으로 스키마를 바꾸지 않는다. 로컬용 이관 도구는 기본 read-only이며 `--apply`가 있을 때만 색인을 만들거나 전달받은 정확한 ID를 보강한다.

```bash
.poc-tools/venv/bin/python scripts/dynamodb/lifecycle_index.py --endpoint http://localhost:18000
.poc-tools/venv/bin/python scripts/dynamodb/lifecycle_index.py --endpoint http://localhost:18000 --apply
# 기존 미완료 원본 목록에서 확인한 ID만 지정한다. 여러 번 지정 가능.
.poc-tools/venv/bin/python scripts/dynamodb/lifecycle_index.py --endpoint http://localhost:18000 \
  --apply --delivery-id 00000000-0000-0000-0000-000000000001
```

AWS 운영 테이블은 인프라 이관 절차로 동일 GSI를 만들고 ACTIVE·스키마를 확인한다. 도구는 로컬 주소만 허용한다. 구형 미완료 데이터의 ID 목록을 확보하고 보강한 뒤 새 Ingress·Dispatch·Result 코드를 적용하고 `DISPATCH_LIFECYCLE_ENABLED=true`로 켠다. 기능을 꺼도 새 원본·Attempt에는 복구 속성을 기록한다. 기존 ID 누락을 자동 보완하려고 전체 테이블을 Scan하지 않는다.

활성화된 Worker는 GSI 부재·비정상 스키마를 시작 시 거절한다. 구버전 Worker와 혼합 실행하면 새 색인 일정이 갱신되지 않을 수 있으므로 무검증 혼합 운영·롤백은 하지 않는다. 만료 기준의 설정 변경도 기존 Attempt 기한을 다시 계산하지 않는다.

## 관측과 시험

`delivery_lifecycle_events_total`의 expired/finalized/published/work_failed/query_failed와 `delivery_lifecycle_active`를 제공한다. DynamoDB Query는 기존 지표의 `operation="query"`로 분리했다. ID·payload·token은 메트릭 label에 넣지 않는다. 업무 실패 로그는 해당 ID와 오류 종류를 남기며 본문은 기록하지 않는다. 새 Grafana 패널 배포는 이번 작업에 포함하지 않았다.

```bash
DYNAMODB_TEST_ENDPOINT=http://localhost:18000 \
KAFKA_TEST_BOOTSTRAP_SERVERS=localhost:39092 \
JAVA_HOME=/path/to/jdk-21 ./mvnw -q package

JAVA_HOME=/path/to/jdk-21 .poc-tools/venv/bin/python scripts/poc/lifecycle_flow.py \
  --kafka localhost:39092 --dynamo http://localhost:18000
```

실제 흐름 실행기는 **비어 있는 복구 GSI와 독점적인 시험 입력**을 요구한다. 기존 색인 항목이 있으면 기존 업무를 처리하지 않도록 중단한다. 기존 Docker 서비스는 재기동하지 않는다. 자신이 시작한 임시 포트 JVM 3개·UUID topic/group·시험 ID만 사용한다. 최초 JWT/Redis/API/Ingress는 이 시험의 시작점이 아니며 META 또는 Dispatch부터 실제 HTTP/TCP·Receipt·Kafka·DynamoDB를 통과한다.

Worker 중단 시험은 해당 자식 프로세스만 SIGSTOP/SIGCONT한다. Kafka 실패는 해당 최종 결과 topic의 크기 제한으로 발행 거절을 유발한 뒤 복구하며, Worker 자식 JVM을 종료하고 같은 설정의 새 JVM으로 시작한다. Kafka cluster 전체 장애·SIGKILL 시험은 아니다.

**전체 package 200건 통과, 실패·제외 0건.** 기존 180건에 만료·최종화 19건과 오류 항목 뒤의 페이지 진행 1건을 추가했다. 실제 DynamoDB의 GSI 페이지 이동, 기한 경계, 양 단계 장기 장애, 운영 확인 만료, 성공 보존, 늦은 저장 거절, 16개 동시 최종화, 최종화 커밋 응답 유실, Kafka ack 뒤 확인 저장 실패의 재인계를 검증했다.

**실제 흐름 7개 통과.** 최종 실행은 `receipt-flow-e8d1768b3f934cb989ccd69044c3ea60`이며 고유 최종 결과 7개와 Kafka record 7개를 본문까지 대조했다. 마지막 사례는 거절 중 PENDING JSON과 재기동 후 발행 JSON이 일치했고 외부 업체 호출은 1회였다. 시험 항목 정리 오류 0건. [검증 JSON·환경·JAR 해시·원본 증거 해시](검증-결과/2026-09-25-주기-만료와-최종-결과-인계.json).

| 실제 흐름 | 결과 |
|---|---|
| 정상 성공 웹훅 + Dispatch 재전달 | DELIVERED, 외부 1차 1회 유지 |
| 1차 미수신·대체 비허용 | 1차 EXPIRED, TCP 미호출 |
| 1차 미수신 만료·대체 허용 | TCP 1회, 2차 DELIVERED |
| 1차·2차 모두 미수신 | 인입 기준 약 5초의 고정 기한, 2차 EXPIRED |
| Worker를 양 단계 기한보다 길게 정지 후 재개 | 원래 기한으로 EXPIRED, TCP 미호출 |
| 오래된 META만 있고 Dispatch 입력 없음 | 정확한 ID의 색인 보강 후 양 단계 EXPIRED, 업체 미호출 |
| Kafka 크기 제한으로 거절 → Worker 종료 → 제한 복구 → 새 JVM | 같은 최종 결과 인계, 원래 발송 재실행 없음 |

검증 중 첫 GSI Query에 빈 ExclusiveStartKey를 보내는 오류를 발견해 첫 조회에서는 생략하도록 수정했다. 초기 통합 시험의 실패 감지 조건도 기록하지 않는 내부 예외 문자열 대신 해당 Delivery의 실패 관측과 PENDING 원장을 확인하도록 보강했다. 수정 전 실패 실행을 통과 수치에 포함하지 않았다.

고객 기한 내 실제 수신, GSI 반영 지연의 상한, 높은 TPS에서 만료 적체, 다중 AZ 무손실을 이 로컬 결과로 입증하지 않는다.

## 다음 단계

발송 최종화 다음은 PostgreSQL 이력 한 줄과 고객 HTTP 통지 PENDING의 원자 저장이다. 이어 같은 고객 최대 100건 묶음, 짧은 대기, 최초 이후 최대 20회 재전송을 구현한다. 이력이 안전하게 인계되기 전에는 발송 상세를 정리하지 않는다.
