# ADR-025 API 직접 Kafka 발행과 계약 조회 제거

- Status: Accepted
- Date: 2026-10-02

## 결정

`MSG-RECEIVE-API`는 JWT와 요청 본문을 확인하고 고객 계약 테이블은 조회하지 않는다. Redis에서 [TPS·유형별 월 Quota](ADR-027-접수-API-Redis-TPS와-유형별-월-Quota.md)를 증가·판정한 뒤 고객 중복을 검사한다. 새 실행의 `clientMsgId`를 생성해 DynamoDB ORIGIN을 조건부 저장하고 `message.received.v1`에 같은 ID를 key로 직접 발행한다. `202 Accepted`는 ORIGIN 저장을 확인했다는 의미이며 업체 발송 완료나 최종 성공을 뜻하지 않는다.

ORIGIN 저장 뒤 Kafka 발행이 실패하거나 응답을 확인하지 못하면 `messaging-publication-recovery-app`이 발행 복구 인덱스를 조회해 같은 ID와 원문을 재발행한다. 중복 Kafka 레코드는 후속 단계가 저장된 결정과 시도를 대조해 처리한다. 계약과 발송 정보는 `PRE-SEND-MANAGER`가 CDC Redis 캐시 우선, 누락 시 PostgreSQL 조회로 확인한다. 전화번호 통신사 Redis 매핑이 없으면 SKT부터 시도한다.

## 검증 경계

ORIGIN 저장 실패에는 Kafka 발행과 202 응답이 없어야 한다. 저장 직후 프로세스 종료나 Kafka ack 불명확 때는 같은 실행이 복구돼야 한다. Redis 중복키가 유실돼도 ORIGIN·완료 이력의 조건부 검사와 업체의 중복 제한 범위를 함께 확인해야 한다. 상세 발송 경계는 [ADR-026](ADR-026-MESSAGE-RECEIVED와-통신사별-HTTP-발송-분리.md)을 따른다.
