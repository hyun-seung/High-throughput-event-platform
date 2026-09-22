# 현재 구현 분석과 Gap

분석 기준: 2026-09-22 현재 코드.

## 1. 현재 처리 흐름

```text
Client
→ JWT 인증
→ Redis TPS/Quota admission 확인
→ tenantId + Idempotency-Key로 안정적인 deliveryId 생성
→ DeliveryRequested를 Kafka에 발행
→ Kafka broker ack 완료
→ Client에 202 Accepted
→ Delivery Ingress Consumer가 consume
→ DynamoDB DELIVERY conditional insert
→ DispatchRequested를 Kafka에 발행
→ Dispatch Consumer가 consume
→ Mock Delivery Provider 호출
```

API는 DynamoDB를 호출하지 않습니다. Kafka ack가 최초 접수 보장의 근거이며 DynamoDB는 Consumer가 만드는 상태 projection입니다.

## 2. 현재 모듈

| 모듈 | 현재 역할 |
|---|---|
| `event-api` | JWT 인증, Redis admission control, Kafka-first Delivery 접수 |
| `delivery-ingress-worker` | Delivery 원본 conditional 저장, Dispatch event 발행 |
| `dispatch-worker` | Dispatch event 소비, 외부 Provider 호출 |
| `external-api-simulator` | Idempotency-Key 기반 중복 방지와 성공/강제 500 응답 |
| `event-common` | Delivery event 계약, Topic, DynamoDB 설정과 테이블 초기화 |
| `auth-module`, `common-security` | 사용자 인증과 JWT |
| `common-core` | 공통 API 응답과 오류 |

각 처리 단계는 독립 실행 모듈로 분리되어 별도 확장과 장애 격리가 가능합니다. 공통 계약 모듈에는 `event` 이름이 남아 있으며 업무 코드에는 `Delivery` 용어를 적용했습니다.

## 3. 구현된 신뢰성 경계

- 같은 tenant와 `Idempotency-Key`는 같은 `deliveryId`를 생성합니다.
- 최초 `DeliveryRequested` event ID도 deliveryId에서 안정적으로 생성됩니다.
- API는 Kafka send future가 성공한 뒤에만 `202`를 반환합니다.
- Producer는 `acks=all`, idempotence, zstd, 짧은 linger를 사용합니다.
- Ingress Consumer는 DynamoDB 단일 item을 conditional insert합니다.
- payload key 순서를 정규화하고 중복 insert에서는 기존 원본이 같은 요청인지 consistent read로 확인합니다.
- DynamoDB 저장 후 `DispatchRequested` 발행이 실패하면 listener가 실패하여 원본 record가 재전달될 수 있습니다.
- Dispatch Worker는 안정적인 `attemptId`로 DynamoDB Attempt를 conditional claim합니다.
- 유효한 `PROCESSING` lease가 있으면 Provider를 호출하지 않고, `ACCEPTED` Attempt는 즉시 완료 처리합니다.
- Attempt version이 일치하는 Worker만 Provider 성공 결과를 기록할 수 있습니다.
- Mock Provider는 attemptId를 Idempotency-Key로 받아 동일 key의 부수 효과를 한 번으로 수렴시킵니다.
- 외부 HTTP 연결/응답 timeout을 설정했습니다.

## 4. 요청당 저장과 발행

정상 흐름 기준:

| 구간 | Kafka | DynamoDB |
|---|---:|---:|
| API 접수 | `DeliveryRequested` 1건 | 0 |
| Ingress Consumer | `DispatchRequested` 1건 | `DELIVERY#id / META` Put 1건 |
| Dispatch Consumer | consume 1건 | Attempt claim 1건 + 성공 결과 Update 1건 |

기존 ingress의 원본 Put, Step Put, publish status Update 구조를 단일 conditional Put으로 줄였습니다.

## 5. 남은 Gap

| 영역 | 현재 상태 | 다음 작업 | 우선순위 |
|---|---|---|---|
| 로컬 실행 | DynamoDB만 Compose에 존재 | Kafka, Redis, PostgreSQL과 healthcheck 추가 | P0 |
| API 오류 계약 | Kafka future 기반 202 | publish timeout/실패를 명시적 503 응답으로 표준화 | P0 |
| Idempotency payload | canonical JSON 문자열 비교 | 대용량 payload에서 hash 저장 여부 검토 | P1 |
| Ingress consume-produce | at-least-once, downstream 중복 가능 | 중복 테스트와 DLT/error handler | P0 |
| Dispatch 멱등성 | Attempt claim/lease/version과 완료 skip | DynamoDB Local 중복/crash 통합 테스트 | P0 |
| 결과 불명 | 실패 시 lease가 만료될 때까지 `PROCESSING` | timeout/reset을 `UNKNOWN`으로 저장하고 조정 | P0 |
| Provider mode | key 지원만 구현 | key 미지원 mode와 중복 한계 실험 | P1 |
| Retry/Fallback | 없음 | 오류 분류, deadline, routing 정책 | P1 |
| Receipt/최종화 | 없음 | Kafka-first Receipt와 단조 상태 전이 | P1 |
| Redis | token bucket과 quota | TTL, 장애 정책, accepted usage 보정 | P1 |
| DLT | Topic만 생성 | ErrorHandler, replay 절차 | P1 |
| 상태 조회 | 없음 | eventual projection 조회 API | P1 |
| Observability | 로그 일부 | metrics, tracing, dashboard와 alert | P1 |
| 테스트 | ID/payload/Kafka ack 계약 테스트 5건 | Kafka/DynamoDB 통합 및 crash test | P0 |
| Kubernetes/CDC | 없음 | 핵심 보장 검증 후 진행 | P2 |

## 6. 현재 보장 범위

현재 설명할 수 있는 보장은 다음과 같습니다.

```text
202 Accepted
= Kafka가 DeliveryRequested를 ack함
```

```text
동일 Client 요청 재전송
→ 동일 deliveryId/eventId
→ Kafka record는 중복될 수 있음
→ DynamoDB 원본은 한 item으로 수렴
```

Provider 호출은 Mock Provider가 idempotency key를 지원하는 경우에만 실제 부수 효과가 한 번으로 수렴합니다. Worker는 완료된 Attempt 재호출을 막지만 Provider 성공 후 DynamoDB 기록 전 종료되면 lease 만료 후 같은 key로 재호출할 수 있습니다. 따라서 Provider가 key를 지원하지 않는 경우까지 exactly-once로 표현하지 않습니다.
