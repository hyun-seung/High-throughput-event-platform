# ADR-005 Delivery 식별자와 Event Envelope

- Status: Accepted
- Date: 2026-09-22

## Problem

하나의 ID가 Delivery 생명주기와 Kafka record를 동시에 나타내면 Retry, Fallback, replay와 단계별 dedupe를 구분할 수 없습니다. API 응답 유실 뒤 Client 재요청도 별도 Delivery를 만들 수 있습니다.

## Decision

- `deliveryId`: tenant와 Client `Idempotency-Key`에서 안정적으로 생성합니다.
- `eventId`: Delivery 단계별 불변 event를 식별합니다.
- `attemptId`: Provider 호출 한 번을 식별합니다.
- `correlationId`: 전체 Delivery 흐름에서 유지합니다.
- `causationId`: 직전 event ID를 가리킵니다.

현재 최초 `DeliveryRequested`와 `DispatchRequested` event ID는 deliveryId와 event type에서 안정적으로 생성합니다.

## Reason

같은 Client 요청과 Kafka 재전달을 동일 Delivery로 수렴시키면서 새로운 상태 전이와 Provider 호출은 별도로 추적하기 위해서입니다.

## Trade-offs

- Client는 모든 생성 요청에 Idempotency-Key를 제공해야 합니다.
- 같은 key에 다른 payload가 들어오면 충돌을 검출해야 합니다.
- deterministic ID가 있으므로 내부 원본 비교와 보존 기간 정책이 필요합니다.

## Failure scenarios

- Kafka ack 후 API 응답 유실: Client 재요청이 같은 deliveryId/eventId를 사용합니다.
- 같은 key와 다른 payload: idempotency 충돌로 격리합니다.
- Dispatch event 중복: 같은 단계 eventId와 Attempt claim으로 수렴합니다.

## Recovery strategy

Consumer는 conditional insert 후 기존 원본과 입력이 같은지 확인합니다. Provider 호출 단계는 별도의 Attempt claim과 lease를 사용합니다.

## Performance impact

일반 요청에는 ID 조회를 위한 별도 저장소 read가 없습니다. 중복 conditional insert가 발생한 경우에만 기존 원본을 consistent read합니다.

## Validation

- 동일 tenant/key가 동일 deliveryId와 최초 eventId를 만드는 단위 테스트를 수행합니다.
- 서로 다른 단계가 서로 다른 eventId와 올바른 causationId를 갖는지 검증합니다.
- 같은 key와 다른 payload의 충돌 테스트를 추가합니다.
