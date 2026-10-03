# ADR-026 EVENT-RECEIVED와 통신사별 HTTP 발송 분리

- Status: Accepted — 단계적 구현 중
- Date: 2026-10-02
- 대체 범위: ADR-025의 API → HTTP sender 직접 발행과 ADR-024의 단일 HTTP sender·발송 전 DynamoDB STEP 선점. ORIGIN 선접수, API의 계약 조회 제거, 202의 의미, HTTP·TCP 결과 판단 분리는 유지한다.

## 확정한 호출 흐름

1. `EVENT-RECEIVE-API`는 JWT를 확인하고 Redis의 [TPS·유형별 월 Quota](ADR-027-접수-API-Redis-TPS와-유형별-월-Quota.md)를 증가·판정한 뒤 본문·고객 중복을 확인하고 ORIGIN을 조건부 저장한다. 저장 확인 뒤 `event.received.v1`에 불변 접수 원문을 `executionId` key로 발행하고 202를 반환한다. API 발행 실패·불명확 시 ORIGIN 기반 복구 앱도 같은 토픽으로 재발행한다.
2. `PRE-SEND-MANAGER`는 `event.received.v1`을 소비한다. 고객 계약은 PostgreSQL CDC로 갱신된 Redis를 먼저 조회하고 누락 시 PostgreSQL에서 조회한다. 전화번호별 통신사 정보의 원본도 PostgreSQL이며 CDC로 Redis에 반영된다. Manager가 Redis의 번호 매핑을 조회해 통신사를 정하고, 계약·발송 설정을 확인한 뒤 1차 업체 전문과 고정 `attemptId`를 만든다. 계약상 발송 불가이면 발송 토픽에 넣지 않고 실패 결과로 인계한다.
3. Manager는 동일한 `HttpSendCommand` 형식을 통신사별 토픽 하나에 발행한다. SKT는 `event.skt.http.send.v1`, KT는 `event.kt.http.send.v1`, LGU+는 `event.lgu.http.send.v1`이다. 같은 실행의 발행 재시도에서는 통신사·전문·`attemptId`를 고정한다.
4. 세 `EVENT-HTTP-SENDER` Deployment는 같은 코드와 전문 형식을 사용한다. 각 Pod는 자기 통신사 토픽만 소비하고 자기 통신사 주소로만 발송한다. Redis가 발송 시도 중복을 제어한다. Redis 유실 또는 발송 후 기록 전 종료에는 동일 `attemptId`를 업체에 전달하고 업체의 중복 처리에 의존하는 범위가 남는다. 발송 후 응답·발송 시각을 DynamoDB에 저장한다.

| 통신사 | Kafka 토픽 | Deployment |
|---|---|---|
| SKT | `event.skt.http.send.v1` | `event-skt-http-sender` |
| KT | `event.kt.http.send.v1` | `event-kt-http-sender` |
| LGU+ | `event.lgu.http.send.v1` | `event-lgu-http-sender` |

각 통신사의 토픽 적체·소비 속도·Pod 장애를 분리한다. Kafka·Redis·DynamoDB 같은 공용 인프라의 장애는 별도 장애 경계다. 업체 주소는 Pod 설정으로 관리하고 Kafka 전문에는 URL 대신 통신사 코드를 담는다.

## 아직 정할 정책과 현재 구현 경계

- 번호→통신사 Redis 키가 없거나 CDC 반영 전일 때 PostgreSQL을 직접 조회할지, 발송을 보류·재시도할지는 미정이다. 최초 발송에서 선택한 통신사는 해당 시도의 재처리 동안 바꾸지 않는다.
- Redis 선점의 TTL·진행 중 중복 처리·발송 후 DynamoDB 기록 실패 복구, 업체별 동일 `attemptId` 중복 처리 계약은 sender 구현 전에 구체화해야 한다.
- 현재 코드는 API의 Redis TPS·유형별 월 Quota, API·ORIGIN 복구 앱의 `event.received.v1` 발행, 통신사별 토픽·공통 전문 타입, 로컬 CDC 소비와 Redis 투영까지 반영했다. `PRE-SEND-MANAGER`의 계약·번호 조회, 통신사별 소비 Pod, Redis 발송 중복 제어, 발송 후 DynamoDB 기록은 아직 구현하지 않았다. 기존 `event-http-sender`는 이전 `event.http.requested.v1`과 발송 전 STEP 선점 모델의 코드이며 신규 경로의 소비자가 아니다. 신규 `/api/v1/events`는 기본 비활성화다.
- PostgreSQL 논리 복제·Debezium·Redis 투영의 로컬 준비는 [계약·번호 CDC 캐시](../52-계약과-번호-통신사-CDC-캐시.md)에 기록한다. `PRE-SEND-MANAGER`의 실제 캐시 조회와 발송 판단은 이 준비와 별도 구현이다.
