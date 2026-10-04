# ADR-024 HTTP·TCP 발송 워커와 결과 판단 분리

- Status: Partially superseded by [ADR-026](ADR-026-MESSAGE-RECEIVED와-통신사별-HTTP-발송-분리.md) — 1차 HTTP sender의 Pod·토픽과 발송 선점 방식
- Date: 2026-10-02
- 적용 상태: 목표 설계, 구현·부하 검증 전.
- 관련 결정: [ADR-025 API 직접 발행](ADR-025-API-직접-Kafka-발행과-계약-조회-제거.md). 현재 단일 `dispatch-worker` 구현과 검증 기록은 이관 전 사실로 보존한다.

## 결정

목표 구조에서 단일 `event-sender`를 두지 않는다. `messaging-http-sender`는 1차 HTTP, `messaging-tcp-sender`는 2차 TCP 업체 호출만 맡는다. 두 sender는 같은 실행의 ORIGIN·STEP을 사용하지만 각자 자기 단계의 조건부 선점 뒤에만 업체를 호출한다. 재시도·2차 전환·최종 성공/실패/만료 판단은 별도 `MESSAGE-RESULT-MANAGER` 한 곳에서 수행한다. 현재 `dispatch-worker`의 발송·결과·Lifecycle 로직을 세 역할로 이관해야 하며 단순 서비스 이름 변경으로 완료되지 않는다.

`MESSAGE-RECEIVE-API`는 ORIGIN 저장 직후 최초 실행을 `message.http.requested.v1`에 발행한다. 각 sender는 자기 요청/재시도 토픽을 소비하고, 호출 전 STEP을 선점하며, 업체의 즉시 접수·거절·무응답을 STEP에 기록한다. 업체 접수는 최종 성공이 아니다. 기록된 호출 결과를 단계별 `message.http.outcome.v1` 또는 `message.tcp.outcome.v1`에 발행한다. STEP 저장 후 Kafka 결과 발행이 실패·불명확하면 같은 결과를 재발행한다. 외부 호출 후 STEP 결과를 기록하기 전에 종료됐다면 업체 효과를 알 수 없으므로 자동 재호출하지 않고 기존 운영 확인·기한 정책을 따른다.

`receipt-api`는 인증·형식 검증 후 웹훅의 단계/업체 정보를 대조해 1차 결과를 `message.http.outcome.v1`, 2차 결과를 `message.tcp.outcome.v1`에 발행한다. `MESSAGE-RESULT-MANAGER`만 두 단계의 결과 토픽을 소비하고 STEP의 실행·단계 ID·회차·version·deadline을 조건부로 확인한다. 업체의 즉시 응답과 나중의 웹훅은 종류를 구분해 기록하며, 웹훅이 먼저 보이거나 중복·늦게 와도 이미 확정된 판단을 덮지 않는다. Manager가 Redis 일정과 DDB 복구 조회를 통해 1차·2차 만료도 판단한다.

모든 단계별 Kafka 기록은 `executionId`를 key로 사용한다. HTTP·TCP 토픽 사이의 도착 순서는 보장된다고 가정하지 않고 STEP의 조건부 상태 전이로 경합을 판정한다. sender도 즉시 응답을 STEP에 쓸 때 이미 확정된 상태를 덮지 않는다. Manager는 Pod 하나로 고정하지 않고 Kafka partition별 소비와 Redis·DDB 복구 작업을 분산해 10,000 TPS 목표에서 처리량·지연을 검증한다.

Manager는 1차 결과를 판단해 같은 단계 재시도가 필요하면 `message.http.retry.v1`, 대체가 허용되고 사유가 맞으면 `message.tcp.requested.v1`, 최종 확정이면 `message.finalized.v1`에 발행한다. 2차 결과는 `message.tcp.retry.v1` 또는 공통 최종 토픽으로 인계한다. 2차 전환 전에는 1차 판단 시각·사유·2차 deadline·단계 ID를 STEP에 고정한다. TCP sender는 이 바인딩을 확인한 뒤 2차 STEP을 선점한다. Manager가 STEP 판단 저장 후 Kafka 발행 전에 중단되면 STEP의 고정된 명령을 복구 작업이 같은 ID로 재발행한다. Kafka ack가 불명확해 중복 명령이 와도 sender의 STEP 선점이 동일 회차 업체 재호출을 제어한다.

어느 단계에서든 전체 결과가 확정되면 Manager가 STEP의 불변 최종 이벤트와 ORIGIN 완료 보호를 저장하고 공통 `message.finalized.v1`에 인계한다. HTTP·TCP 결과의 경합은 하나의 최종 결과로 수렴한다. 기존 `delivery-result-worker`는 이 Manager와 별개로 최종 Kafka 이벤트를 받아 SQL 이력·고객 통지·정리 예약을 멱등하게 저장한다. 단계별 deadline과 운영 확인 정책은 [전체 흐름](../04-요청-접수부터-최종-결과까지의-전체-흐름.md)을 따른다.

## 목표 토픽

| 토픽 | 생산 → 소비 | 용도 |
|---|---|---|
| `message.http.requested.v1` | MESSAGE-RECEIVE-API·발행 복구 앱 → messaging-http-sender | 최초 1차 발송 |
| `message.http.outcome.v1` | messaging-http-sender·receipt-api → MESSAGE-RESULT-MANAGER | 1차 즉시 응답·웹훅 |
| `message.http.retry.v1` | MESSAGE-RESULT-MANAGER → messaging-http-sender | 1차 지연 재시도 |
| `message.tcp.requested.v1` | MESSAGE-RESULT-MANAGER → messaging-tcp-sender | 조건을 통과한 2차 전환 |
| `message.tcp.outcome.v1` | messaging-tcp-sender·receipt-api → MESSAGE-RESULT-MANAGER | 2차 즉시 응답·웹훅 |
| `message.tcp.retry.v1` | MESSAGE-RESULT-MANAGER → messaging-tcp-sender | 2차 지연 재시도 |
| `message.finalized.v1` | MESSAGE-RESULT-MANAGER → result-worker | 실행별 최종 결과 |

각 입력 토픽의 재시도·DLT 경로와 보관 정책은 구현 시 단계별로 분리한다. 기존 `delivery.*` 토픽은 현재 구현의 이름이며 목표 토픽으로 자동 전환됐다고 간주하지 않는다.

## 검증 경계

- HTTP 최종 성공이면 TCP 요청은 0건이고, 대체 허용·사유가 모두 맞을 때만 Manager가 TCP 요청을 발행한다.
- sender의 STEP 결과 저장 후 outcome 발행 실패, Manager의 1차 판단 저장 후 중단, TCP 발행 ack 유실과 중복 TCP 이벤트 소비를 각각 주입한다.
- 1차 늦은 웹훅과 2차 결과가 경합해도 Manager가 전체 최종 결과를 하나로 수렴시킨다.
- HTTP·TCP 각 단계의 재시도 최대 3회, 원래 deadline, 운영 확인, Kafka·Redis 장애 복구를 별도 Pod 배치에서 다시 검증한다.
