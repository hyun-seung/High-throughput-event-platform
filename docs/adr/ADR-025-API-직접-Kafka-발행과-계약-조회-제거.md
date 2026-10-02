# ADR-025 API 직접 Kafka 발행과 계약 조회 제거

- Status: Accepted
- Date: 2026-10-02
- 대체 범위: ADR-023의 DynamoDB Streams·별도 최초 발행 앱과 접수 시 PostgreSQL 계약별 TPS·월 Quota 조회. ORIGIN 선접수·실행별 중복 보호와 ADR-024의 발송 역할 분리는 유지한다.

## 결정

`EVENT-RECEIVE-API`는 JWT 서명·만료와 subject의 `clientId`를 확인하고 본문을 검증한다. 접수 경로에서 고객 계약 테이블을 조회하거나 계약별 TPS·월 Quota를 차감하지 않는다. 현재 JWT 검증은 토큰 자체로 끝나므로 계약 정보를 얻기 위한 PostgreSQL·Redis 조회도 필요하지 않다. 이후 토큰에 없는 계약 속성을 인증 또는 인가에 사용해야 한다면 PostgreSQL 변경을 CDC로 Redis에 반영하고, 요청은 Redis를 먼저 읽으며 캐시 누락 시 PostgreSQL에서 조회한다. 이 조건부 조회 경로는 현재 접수 호출 흐름이 아니다.

새 실행은 Redis 고객 중복키를 확인한 뒤 DynamoDB ORIGIN을 `RECEIVED`로 조건부 저장한다. 저장이 확인되면 API가 곧바로 `event.http.requested.v1`에 같은 `executionId`를 key로 Kafka send를 시작한다. `202 Accepted`는 ORIGIN 영속 접수의 확인으로 반환하며 Kafka ack·업체 호출·최종 성공을 뜻하지 않는다. ORIGIN Put 결과가 불명확하면 동일 `executionId`로 강한 일관성 조회를 해서 접수 여부를 확인한다. 같은 고객 중복키의 재요청도 기존 ORIGIN과 대조한 뒤 같은 실행을 재발행할 수 있다.

ORIGIN 저장 후 API 종료, Kafka 호출 실패 또는 ack 불명확에 대비해 별도 `event-publication-recovery-app`이 ORIGIN의 발행 복구 인덱스를 조회한다. STEP 진행 증거와 완료 보호가 없는 실행을 같은 ID·본문으로 다시 발행한다. 이 앱은 DynamoDB Streams를 읽지 않는다. 최초 Kafka 기록은 중복될 수 있으며 HTTP sender가 ORIGIN·STEP의 조건부 선점을 확인한 뒤에만 업체를 호출한다.

기존 배포에서 ORIGIN Streams가 이미 켜져 있으면 즉시 비활성화하지 않아도 된다. 신규 테이블 초기화는 Streams를 요구하거나 자동으로 활성화하지 않는다. 기존 `delivery.*` Kafka-first 경로는 이관 전 호환 경로로 보존한다.

## 검증 경계

- ORIGIN 저장 실패 시 Kafka send가 호출되지 않고 `202`도 반환되지 않는다.
- ORIGIN 저장 성공 뒤 Kafka send가 실패·ack 불명확해도 원본은 남고 복구 앱이 같은 실행을 재발행한다.
- API 종료가 ORIGIN 저장 직후 발생하거나 HTTP 응답만 유실돼도 원본 기준으로 복구·중복 요청 대조가 가능하다.
- 같은 실행의 중복 Kafka 기록은 HTTP STEP 선점으로 업체 재호출을 막는다. Redis 장애 중 별도 실행 간 외부 중복 가능성은 남는다.
- 신규 인입의 실제 202 TPS·Kafka 발행 지연·복구 적체·DynamoDB 호출량을 다시 계측한다.
