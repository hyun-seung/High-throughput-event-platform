# 장애 시나리오와 검증표

각 테스트는 입력 수, accepted 수, Provider의 실제 부수 효과 수, terminal 수, known in-flight 수, quarantined 수를 함께 기록합니다. 프로세스 로그만으로 성공을 판정하지 않습니다.

| ID | 주입 지점 | 장애 | 기대 상태/동작 | 복구 | 핵심 검증 |
|---|---|---|---|---|---|
| F-01 | API | Kafka 발행 실패 | `202` 응답 금지 | Client retry | ack 없는 accepted 0 |
| F-02 | API | Kafka ack 후 응답 전 종료 | Client 재요청 가능 | 안정적인 deliveryId와 downstream dedupe | DynamoDB 원본 1 |
| F-03 | Producer | broker timeout/ack 유실 | 결과 불확실, 중복 발행 가능 | 같은 ID 재발행+Consumer dedupe | 내부 상태 1 |
| F-04 | Ingress Consumer | DynamoDB 저장 또는 Dispatch 발행 후 commit 전 종료 | record 재전달 | conditional insert와 idempotent publish | Dispatch 논리 효과 1 |
| F-05 | Sender | Provider 호출 전 종료 | 미호출 `PROCESSING` lease 만료 | 재처리 | 실제 호출 1 |
| F-06 | Sender | Provider 성공 후 commit 전 종료 | Kafka 재전달 | idempotency key/완료 기록 | Provider 부수 효과 1 |
| F-07 | Network | Provider 수신 후 response timeout | `UNKNOWN` | key 재조회/재호출 또는 운영 조정 | 성공/실패 임의 단정 금지 |
| F-08 | Provider | 500 연속 | `RETRY_SCHEDULED`, 한도 후 `DEAD` | delay retry | 간격/횟수/최종 사유 |
| F-09 | Provider | slow response | Provider별 concurrency 제한 | 회복 후 drain | API와 타 Provider 격리 |
| F-10 | Receipt API | 동일 Receipt 반복 | dedupe | no-op ack | finalized 효과 1 |
| F-11 | Receipt API | Sender 응답보다 먼저 도착 | 상위 상태 유지 | 후속 응답 병합 | terminal state 동일 |
| F-12 | Result Processor | DB 갱신 후 commit 전 종료 | event 재전달 | conditional update | 이력 중복 0 |
| F-13 | Kafka | broker 중단/재시작 | produce/consume 정지 또는 retry | broker 회복 후 drain | accepted 보존, lag 회복 |
| F-14 | Redis | 연결 실패/데이터 손실 | ADR의 fail 정책 수행 | cache/policy 재구성 | 원본 유실 0 |
| F-15 | PostgreSQL | timeout/connection 고갈 | 해당 group backlog 증가 | DB 회복 후 drain | 다른 group 정상 |
| F-16 | DynamoDB | throttling/timeout | Kafka backlog 증가, API 접수는 지속 가능 | capacity/회복 후 재개 | accepted record 보존 |
| F-17 | Consumer | poison event | 제한 retry 후 DLT | 수정/수동 replay | partition 진행 유지 |
| F-18 | Recovery | 두 Worker가 만료 Attempt 동시 선택 | lease 경합 | 한 Worker claim | 중복에도 결과 안전 |
| F-19 | Kubernetes | SIGTERM/pod kill | poll 중단, 처리/commit 정리 | rebalance 후 재개 | 유실 0, 중복 허용 범위 |
| F-20 | CDC | connector 장기 중단 | 기준정보 lag 증가 | 저장 offset부터 재개 | 최종 Redis 값 일치 |
| F-21 | Routing | primary Provider circuit open | fallback Provider로 전환 | 정책 순서대로 dispatch | route/attempt 이력과 최종 성공 |
| F-22 | Routing | primary의 확정 거절 | 허용된 오류일 때만 fallback | 보조 Provider dispatch | 실제 부수 효과 1 |
| F-23 | Routing | primary 수신 후 timeout | `UNKNOWN`, 자동 fallback 금지 | 조회/receipt/운영 조정 | 두 Provider 중복 전달 0 |
| F-24 | Routing | 모든 Provider 실패 | retry/deadline 후 `DEAD` | 운영 replay | 무한 순환 0, 전체 이력 보존 |

## Sender 필수 실험 4종

| Provider 멱등키 | 응답 | 재처리 정책 | 기대 결과 |
|---|---|---|---|
| 지원 | 정상 2xx | 같은 key로 재전달 | Provider 부수 효과 1회 |
| 지원 | 수신 후 timeout | 같은 key 조회/재호출 | 성공으로 수렴, 부수 효과 1회 |
| 미지원 | 정상 2xx 후 Sender 종료 | 내부 완료 기록 시점에 따라 중복 가능성 측정 | 실제 한계 수치화 |
| 미지원 | 수신 후 timeout | 자동 재호출 금지 또는 명시적 정책 | `UNKNOWN`과 운영 결정 기록 |

## Provider Fallback 필수 검증

- 확정 실패와 `UNKNOWN`을 서로 다른 routing 결과로 처리합니다.
- circuit breaker가 열린 Provider를 호출하지 않고 다음 route를 선택합니다.
- fallback 후 늦은 primary 응답이나 receipt가 도착해도 최종 상태가 되돌아가지 않습니다.
- 전체 Provider에 걸친 최대 attempt와 delivery deadline을 지킵니다.
- Provider별 성공률뿐 아니라 fallback 전환율과 fallback 이후 중복 부수 효과를 측정합니다.

## 테스트 결과 기록 형식

각 실행 결과에는 다음을 남깁니다.

- Git commit과 모든 서비스 image/version
- topic partition/replication/retention, Consumer group 설정
- 입력 수와 요청 속도, 실행 시간
- 장애 시작/종료 시각과 주입 설정
- accepted, rejected, terminal, in-flight, retry, recovery, unknown, dead 수
- Kafka lag/oldest age와 정상 회복까지 걸린 시간
- Provider가 기록한 unique idempotency key와 실제 부수 효과 수
- p50/p95/p99 API 및 end-to-end latency
- 판정, 관찰된 한계, 다음 변경
