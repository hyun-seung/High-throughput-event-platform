# ADR-004 Kafka 저장 확인 후 접수 응답하는 이유

- Status: Accepted
- Date: 2026-09-22

## 문제

API가 DynamoDB 원본과 발행 상태를 저장한 뒤 Kafka를 발행하면 정상 요청에도 여러 write가 필요하고 저장 후 미발행을 찾는 별도 Recovery가 필요합니다.

## 검토한 대안

1. DynamoDB 원본과 PENDING 상태를 먼저 저장하고 Kafka 발행 결과를 업데이트합니다.
2. PostgreSQL transactional outbox를 사용합니다.
3. Kafka ack를 접수 완료 기준으로 삼고 DynamoDB projection은 Consumer가 만듭니다.

## 결정

3번 Kafka-first 방식을 사용합니다. API는 `DeliveryRequested`의 broker ack가 성공한 뒤에만 `202 Accepted`를 반환합니다. Ingress Consumer가 DynamoDB에 Delivery 원본을 conditional insert하고 다음 event를 발행합니다.

## 선택 이유

- Kafka가 프로젝트의 핵심 전달 backbone입니다.
- API에서 DynamoDB latency와 장애 의존성을 제거합니다.
- ingress DynamoDB 변경을 세 건에서 한 건으로 줄입니다.
- Kafka 재전달을 원본 projection 복구에 사용할 수 있습니다.

## 장점과 제약

- `202` 직후에는 DynamoDB 상태 조회 projection이 없을 수 있습니다.
- Kafka retention과 lag가 복구 가능 시간을 결정합니다.
- Kafka ack 후 API 응답 전 종료 시 Client가 요청을 반복할 수 있습니다.
- Consumer의 DB write와 다음 Kafka publish는 하나의 transaction이 아닙니다.

## 장애 상황

- Kafka 발행 실패: 성공 응답을 반환하지 않습니다.
- Kafka ack 후 응답 유실: 같은 Client 요청이 다시 발행될 수 있습니다.
- DynamoDB 저장 후 Consumer 종료: record 재전달과 conditional insert로 수렴합니다.
- 다음 Topic 발행 후 commit 전 종료: downstream에서 중복 event를 처리합니다.

## 복구 방법

안정적인 Delivery ID, DynamoDB conditional write와 downstream 멱등성을 사용합니다. Consumer lag와 oldest record age가 retention 안전 범위를 넘기 전에 경보합니다.

## 성능 영향

API 경로에서 DynamoDB write를 제거하고 정상 Delivery의 ingress DynamoDB 변경을 한 건으로 줄입니다. API latency는 Kafka broker ack에 의해 결정됩니다.

## 검증 방법

- Kafka ack 없는 요청에 `202`가 반환되지 않는지 검증합니다.
- 같은 요청 N회가 하나의 Delivery projection으로 수렴하는지 검증합니다.
- DynamoDB 저장 전후와 다음 Topic 발행 전후에 Consumer를 종료합니다.
