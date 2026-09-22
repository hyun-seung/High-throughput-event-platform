# 구간별 상세 처리 흐름

각 구간을 `입력 → 검사 → 저장/발행 → 완료 기준 → 실패 동작` 순서로 정리합니다.

구현 상태:

- **구현**: 현재 코드에 존재
- **일부**: 기본 흐름만 있고 신뢰성 처리가 남음
- **예정**: 설계 단계

## 1. API 인입

**구현**

```text
Client 요청
→ JWT 인증
→ Request validation
→ Redis TPS/Quota admission 확인
→ Idempotency-Key 검증
→ deliveryId와 DeliveryRequested eventId 생성
```

입력:

```http
POST /api/v1/deliveries
Authorization: Bearer ...
Idempotency-Key: client-request-001
```

```json
{
  "deliveryType": "ORDER_COMPLETED",
  "payload": {
    "orderId": "100"
  }
}
```

`deliveryId`는 `tenantId + Idempotency-Key`에서 안정적으로 생성합니다. 같은 tenant가 같은 key로 재요청하면 동일한 ID가 됩니다.

## 2. Kafka 최초 접수

**구현**

```text
DeliveryRequested 생성
→ delivery.requested.v1 발행
→ Kafka acks=all 확인
→ 202 Accepted + deliveryId
```

`202`의 의미:

```text
DeliveryRequested가 Kafka에 저장되어 비동기 처리를 시작할 수 있다.
```

Kafka send가 실패하면 future가 실패하므로 `202`가 완성되지 않습니다. 명시적 `503` 오류 계약은 다음 작업입니다.

API 요청 thread에서는 DynamoDB에 쓰지 않습니다.

## 3. Delivery 원본 Projection

**구현**

```text
Delivery Ingress Consumer
→ DeliveryRequested consume
→ event type 검증
→ DynamoDB DELIVERY conditional insert
→ 중복이면 기존 원본 동일성 확인
```

DynamoDB key:

```text
PK = DELIVERY#{deliveryId}
SK = META
```

정상 요청의 ingress DynamoDB write는 한 건입니다.

중복 record이면 Put 조건이 실패하고 consistent read로 `eventId`, `tenantId`, `deliveryType`, `payload`가 같은지 확인합니다. 내용이 다르면 idempotency 충돌로 실패시킵니다.

## 4. Dispatch 단계 발행

**구현**

```text
DynamoDB 원본 저장 또는 동일 원본 확인
→ DispatchRequested 생성
→ delivery.dispatch-requested.v1 발행
→ Kafka ack 확인
→ DeliveryRequested listener 완료
```

DynamoDB 저장 후 Dispatch 발행 전에 종료되면 최초 record가 다시 전달됩니다. 원본 conditional insert는 중복을 허용하고 Dispatch event를 다시 발행합니다.

Dispatch 발행 후 최초 record commit 전에 종료되면 Dispatch event가 중복될 수 있습니다. 따라서 Dispatch Consumer 멱등성은 필수입니다.

## 5. Dispatch Consumer와 Provider 호출

**일부**

현재:

```text
DispatchRequested consume
→ event type 검증
→ deliveryId를 Provider Idempotency-Key로 사용
→ Mock Provider HTTP 호출
```

HTTP connection timeout과 read timeout이 있습니다. Mock Provider는 동일 Idempotency-Key에 기존 응답을 반환합니다.

추가할 흐름:

```text
DispatchRequested consume
→ DISPATCH_ATTEMPT conditional claim
→ PROCESSING + leaseUntil
→ Provider 호출
→ ACCEPTED / FAILED / UNKNOWN 저장
→ offset commit
```

Provider가 key를 지원하지 않는 mode에서는 Attempt claim만으로 HTTP 응답 유실 뒤의 중복 부수 효과를 완전히 막을 수 없습니다.

## 6. Retry

**예정**

```text
확정된 일시 실패
→ attempt와 전체 deadline 확인
→ backoff + jitter 계산
→ nextAttemptAt 저장
→ 시각 도달 후 DispatchRequested 재발행
```

timeout/reset처럼 Provider 접수 여부가 불명확한 결과는 일반 Retry로 보내지 않고 `UNKNOWN`으로 처리합니다.

## 7. Provider Fallback

**예정**

```text
Primary Provider 확정 실패 또는 circuit open
→ Fallback 허용 정책 확인
→ 다음 Provider 선택
→ 새 attemptId와 routeOrder
→ Dispatch
```

`UNKNOWN`에서는 Primary가 이미 처리했을 가능성이 있으므로 자동 Fallback하지 않습니다.

## 8. Receipt 인입

**예정**

```text
Provider callback
→ 인증/서명 검증
→ DeliveryReceiptReceived Kafka 발행
→ Kafka ack
→ Provider에 2xx
```

Receipt API는 요청 thread에서 최종 상태를 계산하거나 PostgreSQL 이력을 저장하지 않습니다.

## 9. 최종 상태 확정

**예정**

```text
DeliveryReceiptReceived consume
→ providerEventId dedupe
→ 허용된 상태 전이 검사
→ DynamoDB terminal 상태 conditional update
→ DeliveryFinalized 발행
```

Receipt가 Provider HTTP 응답보다 먼저 도착해도 terminal 상태가 이전 상태로 돌아가지 않게 합니다.

## 10. 최종 결과 Fan-out

**예정**

```text
delivery.finalized.v1
├─ History Consumer Group
├─ Usage Consumer Group
├─ Client Callback Consumer Group
└─ Statistics Consumer Group
```

각 Consumer Group은 독립 offset과 멱등성 key를 사용합니다.

## 11. 구간별 DynamoDB write 기준

| 구간 | Write | 이유 |
|---|---|---|
| API 접수 | 없음 | Kafka ack가 접수 원장 |
| Delivery 원본 projection | 1회 | 조회와 idempotency 충돌 확인 |
| 단순 payload 변환 | 생략 | 원본에서 재계산 가능 |
| Provider Attempt claim | 필수 | 외부 호출 중복 방지 |
| Provider 응답/UNKNOWN | 필수 | Retry/Fallback 판단 |
| Retry 예약 | 필수 | 횟수, deadline, 다음 실행 시각 |
| Receipt 상태 전이 | 필수 | 중복과 순서 역전 방지 |
| 로그/메트릭 | 생략 | Loki/Prometheus 사용 |

## 12. 다음 구현 순서

```text
1. 로컬 전체 인프라와 통합 테스트
2. Dispatch Attempt claim/lease
3. Provider 응답 분류와 UNKNOWN
4. Retry
5. Fallback
6. Receipt와 최종화
7. 독립 Consumer Group
8. 장애 및 부하 테스트
```
