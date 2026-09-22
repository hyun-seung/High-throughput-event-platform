# Reliable Event Delivery Platform

외부 시스템으로 Event를 안정적으로 전달하는 Kafka 기반 비동기 플랫폼입니다. 유실, 중복, 외부 Provider 장애, 재시도와 복구를 코드와 테스트로 검증합니다.

업무 처리 단위는 `Delivery`, Kafka에서 전달되는 불변 사실은 `DeliveryEvent`, 외부 호출 한 번은 `DispatchAttempt`로 구분합니다.

## 문서

- [요구사항과 시스템 설계](docs/00-system-design.md)
- [현재 구현 분석과 Gap](docs/01-current-state.md)
- [구현 로드맵과 완료 조건](docs/02-roadmap.md)
- [장애 시나리오와 검증표](docs/03-failure-test-matrix.md)
- [전체 End-to-End 처리 흐름](docs/04-end-to-end-flow.md)
- [구간별 상세 처리 흐름](docs/05-stage-by-stage-flow.md)
- [ADR 목록](docs/adr/README.md)

## 가장 먼저 할 일

1. 로컬 Kafka, Redis, PostgreSQL, DynamoDB와 애플리케이션을 한 번에 실행할 수 있게 만듭니다.
2. Dispatch Attempt claim과 Provider 호출 결과를 DynamoDB에 기록합니다.
3. 각 단계에 중복 event를 넣고 상태 전이와 외부 호출 횟수를 검증합니다.
4. Sender timeout 후 결과 불명 상태를 Provider의 멱등키 지원 여부별로 검증합니다.
5. Retry, Fallback, Receipt 순서로 전달 흐름을 확장합니다.

단계별 순서와 통과 기준은 [로드맵](docs/02-roadmap.md)을 기준으로 합니다.
