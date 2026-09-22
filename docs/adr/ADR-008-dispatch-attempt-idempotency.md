# ADR-008 Dispatch Attempt claim과 Provider 멱등성

- Status: Accepted
- Date: 2026-09-22

## Problem

Dispatch Consumer가 Provider 호출 뒤 Kafka offset commit 전에 종료되면 같은 record가 다시 전달됩니다. 내부 처리 기록만으로는 Provider가 만든 외부 부수 효과의 중복까지 제거할 수 없습니다.

## Options

1. Kafka record 중복을 허용하고 매번 Provider를 호출합니다.
2. Redis lock으로 Consumer의 동시 호출만 막습니다.
3. DynamoDB Attempt를 lease로 점유하고 안정적인 Provider idempotency key를 함께 사용합니다.

## Decision

3번을 사용합니다.

- 최초 호출은 `PK=DELIVERY#{deliveryId}`, `SK=ATTEMPT#{attemptId}`를 conditional update로 점유합니다.
- `attemptId`는 `deliveryId + provider + routeOrder + attemptNumber`에서 안정적으로 생성합니다.
- Attempt는 `PROCESSING`, `leaseUntil`, `version`을 저장합니다.
- 유효한 lease가 있는 중복 record는 Provider를 호출하지 않습니다.
- 만료된 lease는 version을 증가시켜 다시 점유합니다.
- Provider에는 attemptId를 Idempotency-Key로 전달합니다.
- 성공 결과는 점유한 version이 일치할 때만 `ACCEPTED`로 변경합니다.
- 이미 `ACCEPTED`인 Attempt는 Provider 호출 없이 완료합니다.

## Reason

DynamoDB 상태는 프로세스 종료 후에도 복구 근거로 남습니다. lease는 영구 점유를 막고 version은 만료된 Worker가 새 점유자의 결과를 덮는 것을 방지합니다. 안정적인 Provider key는 성공 응답을 받기 전에 연결이 끊기는 원자성 공백을 보완합니다.

## Trade-offs

- 정상 Provider 호출마다 DynamoDB write가 claim과 결과 기록으로 두 번 발생합니다.
- 유효한 lease 동안 같은 partition의 record 처리가 지연될 수 있어 별도 Retry/ErrorHandler가 필요합니다.
- Provider가 idempotency key를 지원하지 않으면 lease 만료 후 재호출의 외부 중복 가능성이 남습니다.

## Failure scenarios

- claim 전 종료: Kafka가 record를 다시 전달하고 새 Worker가 claim합니다.
- claim 후 호출 전 종료: lease 만료 뒤 같은 attemptId로 다시 점유합니다.
- Provider 성공 후 결과 저장 전 종료: key 지원 Provider에는 같은 attemptId로 재호출하여 기존 결과로 수렴합니다.
- 이전 Worker가 lease 만료 후 성공 결과 저장: version 조건이 실패해 새 점유 상태를 덮지 못합니다.
- 결과 저장 후 offset commit 전 종료: `ACCEPTED` 확인 후 Provider 호출 없이 완료합니다.

## Recovery strategy

현재는 Kafka 재전달이 만료된 lease를 다시 점유합니다. 이후 Recovery Worker가 만료 Attempt를 탐색하고, `UNKNOWN` Attempt는 Provider 조회나 Receipt를 이용해 조정합니다.

## Performance impact

Delivery 한 건의 정상 Dispatch에 DynamoDB write 2회가 추가됩니다. Attempt key는 deliveryId 아래 분산되며 hot delivery 한 건의 동시 갱신은 conditional write로 직렬화됩니다.

## Validation

- 완료된 Attempt 중복 처리에서 Provider client가 호출되지 않는 단위 테스트
- 유효한 lease 중복 처리에서 Provider client가 호출되지 않는 단위 테스트
- 안정적인 attemptId 생성 테스트
- DynamoDB Local 기반 claim 경쟁, lease 만료와 crash 통합 테스트를 Phase 2에서 추가
