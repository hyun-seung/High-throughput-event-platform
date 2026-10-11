# ADR-025 API 직접 Kafka 발행과 계약 조회 제거

- Status: Accepted
- Date: 2026-10-02
- Updated: 2026-10-11 — Kafka ack 확인 후 200 OK 반환·고객 중복 지문 순서 고정

## 결정

`MSG-RECEIVE-API`는 JWT와 요청 본문을 확인하고 고객 계약 테이블은 조회하지 않는다. Redis에서 [TPS·유형별 월 Quota](ADR-027-접수-API-Redis-TPS와-유형별-월-Quota.md)를 증가·판정한 뒤 고객 중복을 검사한다. 새 실행의 `clientMsgId`를 생성해 DynamoDB ORIGIN을 조건부 저장하고 `message.received.v1`에 같은 ID를 key로 직접 발행한다. `200 OK`는 ORIGIN 저장과 Kafka의 성공 ack를 모두 확인했다는 의미이며 업체 발송 완료나 최종 성공을 뜻하지 않는다. producer는 `acks=all`을 사용한다. API는 발행 future의 성공 완료를 최대 10초 기다리며, `KAFKA_PUBLISH_CONFIRM_TIMEOUT_MS`로 조정할 수 있다. 이 대기는 `KafkaTemplate.send`의 메타데이터·버퍼 대기(기본 최대 5초) 뒤에 시작한다.

ORIGIN 저장 뒤 Kafka 발행이 실패하거나 제한 시간 내 응답을 확인하지 못하면 API는 `503`·오류 코드 `50004`를 반환한다. 원본과 고객 중복키를 보존하고, `messaging-publication-recovery-app`이 발행 복구 인덱스를 조회해 같은 ID와 원문을 재발행한다. 중복 Kafka 레코드는 후속 단계가 저장된 결정과 시도를 대조해 처리한다. 계약과 발송 정보는 `PRE-SEND-MANAGER`가 CDC Redis 캐시 우선, 누락 시 PostgreSQL 조회로 확인한다. 전화번호 통신사 Redis 매핑이 없으면 SKT부터 시도한다.

고객 중복 마커는 `clientMsgId:v1:SHA-256`으로 저장하며 전체 JSON 객체의 키를 정렬해 JVM에 따른 지문 변화를 막는다. 구형 마커는 기존 실행 ID와 TTL을 유지하고 ORIGIN 전체 내용 대조로 확인한다. 내용이 다르면 409, 원본이 없거나 확인할 수 없으면 503이며 새 실행을 만들지 않는다. 상세 호환 경계는 [접수 거절·중복 흐름](../53-신규-메시지-케이스별-Call-Flow.md#2-접수-거절중복최초-발행-불명확)에 기록한다.

## 검증 경계

ORIGIN 저장 실패에는 Kafka 발행과 200 응답이 없어야 한다. 저장 직후 프로세스 종료나 Kafka ack 불명확 때는 같은 실행이 복구돼야 한다. Redis 중복키가 유실돼도 ORIGIN·완료 이력의 조건부 검사와 업체의 중복 제한 범위를 함께 확인해야 한다. 고객이 같은 본문·messageId·recipientNumber로 재요청하면 같은 실행을 재발행하고 ack 확인 후 200을 반환한다. 503은 원본이 저장되지 않았거나 메시지가 처리되지 않는다는 보장이 아니므로 새 messageId로 재접수하지 않는다. 상세 발송 경계는 [ADR-026](ADR-026-MESSAGE-RECEIVED와-통신사별-HTTP-발송-분리.md)을 따른다.
