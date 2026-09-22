# Architecture Decision Records

## 작성 형식

파일명은 `ADR-NNN-short-title.md`를 사용합니다. 상태는 `Proposed`, `Accepted`, `Superseded` 중 하나입니다.

```markdown
# ADR-NNN 제목

- Status: Proposed
- Date: YYYY-MM-DD

## Problem
## Options
## Decision
## Reason
## Trade-offs
## Failure scenarios
## Recovery strategy
## Performance impact
## Validation
```

`Validation`에는 결정이 맞는지 확인할 자동 테스트, 장애 테스트, 메트릭을 적습니다.

## 우선 작성 순서

| ADR | 결정 | 구현 전 필요한 Phase |
|---|---|---|
| ADR-001 | Kafka를 Delivery backbone으로 선택 | 0 |
| ADR-002 | PostgreSQL, DynamoDB, Redis의 책임 | 0 |
| ADR-003 | at-least-once 전달과 Consumer commit 원칙 | 0 |
| [ADR-004](ADR-004-kafka-first-ingress.md) | Kafka-first API 접수 | Accepted |
| [ADR-005](ADR-005-delivery-identifiers.md) | Delivery 식별자와 event envelope | Accepted |
| ADR-006 | topic, partition key, retention과 Consumer Group | 1 |
| ADR-007 | Delivery/Attempt 상태 머신과 순서 역전 규칙 | 2 |
| [ADR-008](ADR-008-dispatch-attempt-idempotency.md) | Sender claim, lease와 Provider idempotency | Accepted |
| ADR-009 | Provider 결과 불명 상태의 조정 정책 | 3 |
| ADR-010 | Retry와 Recovery 분리 및 스케줄 방식 | 4~5 |
| ADR-011 | Provider routing, fallback 조건과 전체 deadline | 5 |
| ADR-012 | Receipt 인증, dedupe와 결과 수렴 | 6 |
| ADR-013 | finalized event와 독립 Consumer Group | 7 |
| ADR-014 | TPS/Quota 알고리즘과 Redis 장애 정책 | 8 |
| ADR-015 | SLI/SLO와 metric cardinality | 9 |
| ADR-016 | Kubernetes lag scaling과 종료 정책 | 11 |
| ADR-017 | CDC 적용 범위와 source-of-truth | 13 |
