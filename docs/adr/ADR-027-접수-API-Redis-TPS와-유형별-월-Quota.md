# ADR-027 접수 API의 Redis TPS와 유형별 월 Quota

- Status: Accepted — 신규 API에 구현, 기본 비활성화 경로
- Date: 2026-10-03
- 변경 범위: [ADR-025](ADR-025-API-직접-Kafka-발행과-계약-조회-제거.md)의 접수 시 TPS·Quota 미차감 결정만 대체한다. API에서 PostgreSQL 계약을 조회하지 않는 결정은 유지한다.

## 결정과 호출 순서

`EVENT-RECEIVE-API`는 JWT에서 `clientId`를 확인하고 `eventType`이 있는 요청에 대해 Redis 운영 정책의 10초 고정 구간 TPS와 이벤트 유형별 월 Quota를 **원자적으로 증가·판정**한다. 이후 본문 상세 검증, Redis 고객 중복 확인, DynamoDB ORIGIN 조건부 저장, `event.received.v1` 발행, 202 응답 순서다. 고객 중복 요청과 유형을 알 수 있는 유효하지 않은 본문도 사용량에 포함한다. JWT 거절과 `eventType` 누락은 포함하지 않는다.

정책 키는 `event:usage:{client:<id>}:policy`이며 `tpsLimit`, `quotaGENERAL`, `quotaNOTI`, `quotaADV`, `quotaALERT`를 가진다. 10초 구간 한도는 `tpsLimit × 10`이다. 월별 사용량은 `event:usage:{client:<id>}:month:<yyyy-MM>:<eventType>`에 한국 시간 기준으로 저장한다. 한도 초과 요청도 증가된 사용량에 포함되고 429를 받는다. 로컬 `scripts/local.sh init-policy`는 예시 정책을 초기화한다. 운영 정책의 원천·변경 절차와 실제 한도는 별도 결정이 필요하다.

정책은 접수 제어용 Redis 데이터이며 고객 계약 테이블 조회가 아니다. 계약·발송 정보 확인은 `PRE-SEND-MANAGER`에서 CDC Redis 캐시 우선, 누락 시 PostgreSQL 조회로 수행한다. 정책이 없거나 형식이 잘못되면 API는 503으로 접수를 중단한다. Redis 연결 실패·타임아웃이면 접수를 계속하는 가용성 정책을 택한다. 따라서 Redis 장애 동안 제한 초과 접수가 가능하며 사용량도 누락될 수 있다.

이 결정은 신규 `/api/v1/events` 경로에 적용된다. 기존 `/api/v1/deliveries`의 `RequestControlFilter` 정책과 키는 별개다. 신규 API는 `EVENT_ADMISSION_ENABLED=false`가 기본값이다.
