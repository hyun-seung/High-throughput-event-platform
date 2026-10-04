# 신규 이벤트 접수부터 최종 결과까지: 케이스별 Call Flow

기준: 2026-10-04. **신규 `/api/v1/events` 경로의 목표 설계**를 한곳에서 읽기 위한 문서다. 정상 흐름과 실패·복구 분기를 함께 그린다. 현재 구현은 API 접수·ORIGIN 저장·`event.received.v1` 발행, 공통 전문·통신사별 토픽, CDC 캐시 준비까지다. `PRE-SEND-MANAGER`, 통신사별 sender, `MSG_RESULT` 생산·소비, 신규 결과 판단 경로는 아직 연결되지 않았다. 이관 전 `delivery.*` 및 `event.http.requested.v1` 구현·검증을 신규 경로의 완료로 읽지 않는다. 구현 상태는 [01 현재 상태](01-현재-구현-상태와-남은-작업.md), 결정 근거는 [ADR-026](adr/ADR-026-EVENT-RECEIVED와-통신사별-HTTP-발송-분리.md)·[ADR-027](adr/ADR-027-접수-API-Redis-TPS와-유형별-월-Quota.md)을 따른다.

## 읽는 순서와 경계

| 케이스 | 위치 | 최종 결과 |
|---|---|---|
| 1차 정상 성공 | [1](#1-1차-정상-성공) | 성공 웹훅 → 최종 성공 → 고객 통지·정리 |
| 접수 거절·중복·최초 발행 불명확 | [2](#2-접수-거절중복최초-발행-불명확) | API 응답 또는 ORIGIN 기준 복구 |
| 계약·통신사 조회 분기 | [3](#3-pre-send-manager의-계약통신사-조회) | 발송 명령 또는 보류·실패 |
| 업체 즉시 응답 실패 | [4](#4-업체의-즉시-응답이-실패) | 재시도·2차 전환·최종 실패·운영 확인 |
| 접수 성공 후 실패 웹훅 | [5](#5-접수-성공-후-실패-웹훅) | 웹훅 코드별 동일 판단 |
| 웹훅 미수신·1차 만료 | [6](#6-웹훅-미수신과-1차-만료) | 2차 전환 또는 만료 |
| 2차 TCP 발송 | [7](#7-2차-tcp-발송) | 성공·재시도·최종 실패·만료 |
| 고객 통지 실패·정리 | [8](#8-최종-결과-이후-고객-통지와-정리) | SQL 기준 독립 재개 |
| 중간 종료·저장소 장애 | [9](#9-중간-종료와-저장소-장애) | 저장된 원본·판단 기준 재개 |

현재 확정한 새 경로의 토픽은 `event.received.v1`(API → PRE-SEND-MANAGER), `event.skt.http.send.v1`·`event.kt.http.send.v1`·`event.lgu.http.send.v1`(Manager → 통신사별 sender), `MSG_RESULT`(1차 즉시 응답·웹훅 → EVENT-RESULT-MANAGER)다. 최종 결과 토픽은 기존 목표의 `event.finalized.v1`을 사용한다. **2차 발송은 아직 기존 목표명** `event.tcp.requested.v1`/`event-tcp-sender`로 표기한다. 제안된 `event.tcp.send.v1` 이름은 확정되지 않았다. 1차 재시도 토픽·명령 방식과 2차 결과를 `MSG_RESULT`에 합칠지도 미정이다.

업무 이벤트 Kafka 레코드의 key는 `executionId`를 사용한다. CDC 참조 데이터 토픽은 별개다. `attemptId`는 같은 단계의 업체 호출 식별자이고, 재시도 회차(`invocation`)·버전은 별도로 구분한다. 다른 토픽 사이의 도착 순서는 보장되지 않으므로 최종 판단은 DynamoDB의 조건부 상태 전이로 수렴시킨다.

## 1. 1차 정상 성공

전제: JWT·TPS·Quota 통과, 새 고객 이벤트, 계약과 전화번호→통신사 값이 Redis에 있음, 업체가 즉시 접수하고 나중에 `DELIVERED` 웹훅을 보냄, 고객이 최종 통지에 204로 응답함. PostgreSQL → CDC → Redis 투영은 요청 밖에서 먼저 진행된 참조 데이터 갱신이다.

```mermaid
sequenceDiagram
    autonumber
    actor C as 고객
    participant A as EVENT-RECEIVE-API
    participant R as Redis
    participant D as DynamoDB
    participant K as Kafka
    participant M as PRE-SEND-MANAGER
    participant S as 통신사별 HTTP-SENDER
    participant P as 통신사 업체
    C->>A: POST /api/v1/events + JWT
    A->>A: JWT에서 clientId 확인
    A->>R: 10초 TPS·유형별 월 Quota 원자 증가·판정
    A->>A: 본문 상세 검증
    A->>R: 고객 중복키 확인·선점
    A->>D: ORIGIN RECEIVED 조건부 저장
    D-->>A: 저장 확인
    A->>K: event.received.v1 발행 시작
    A-->>C: 202 + executionId
    K->>M: 불변 접수 원문
    M->>R: 계약·번호별 통신사 조회
    M->>M: 발송 조건 확인, 전문·attemptId 고정
    M->>K: 해당 통신사의 event.<carrier>.http.send.v1
    K->>S: HttpSendCommand
    S->>R: attemptId 발송 중복 확인
    S->>P: HTTP 발송 + attemptId
    P-->>S: 접수 성공(ACCEPTED)
    S->>D: 발송 시각·접수 응답 저장
    S->>K: MSG_RESULT / 즉시 접수 결과
```

`202`는 ORIGIN 영속 접수 확인이다. Kafka ack, 업체 접수, 최종 성공을 기다리지 않는다. 업체의 `ACCEPTED`도 최종 성공이 아니므로 결과 기한까지 웹훅을 기다린다.

```mermaid
sequenceDiagram
    autonumber
    actor C as 고객
    participant P as 통신사 업체
    participant W as receipt-api
    participant K as Kafka
    participant M as EVENT-RESULT-MANAGER
    participant D as DynamoDB
    participant R as Redis
    participant H as delivery-result-worker
    participant Q as PostgreSQL
    K->>M: MSG_RESULT / 즉시 접수 성공
    M->>R: 1차 deadline 후보 등록
    P->>W: DELIVERED 웹훅 + executionId·attemptId
    W->>W: 업체 인증·전문 검증
    W->>K: MSG_RESULT / 성공 웹훅
    K-->>W: 저장 확인
    W-->>P: 웹훅 접수 응답
    K->>M: 성공 웹훅
    M->>D: 실행·단계·회차 확인 후 최종 성공 조건부 저장
    M->>K: event.finalized.v1 발행
    M->>R: deadline 후보 제거
    K->>H: 최종 결과
    H->>Q: 이력·통지 예약·정리 예약 원자 commit
    par 고객 결과 통지
        H->>C: 성공 결과 HTTP 통지
        C-->>H: 204
        H->>Q: 통지 완료
    and 발송 데이터 정리
        H->>R: 성공 시각부터 중복키 2시간 차단
        H->>D: ORIGIN·STEP 조건부 삭제
        H->>Q: 정리 완료
    end
```

고객 통지와 발송 데이터 정리는 SQL commit 후 독립 작업이다. 고객 204를 기다려야만 ORIGIN·STEP을 지우는 순서가 아니다.

## 2. 접수 거절·중복·최초 발행 불명확

정상 접수 순서는 `JWT → Redis TPS·유형별 월 Quota 증가·판정 → 본문 상세 검증 → 고객 중복 확인 → ORIGIN 저장 → Kafka send 시작 → 202`다. API는 계약 PostgreSQL을 조회하지 않는다. `eventType`을 확인할 수 있는 잘못된 본문과 고객 중복 요청도 사용량에 포함된다.

| 발생 지점 | 흐름 | 고객 응답·후속 |
|---|---|---|
| JWT 거절 또는 `eventType` 누락 | 사용량 증가 전 중단 | 접수하지 않음 |
| TPS·월 Quota 초과 | Redis에서 증가와 판정이 한 번에 이뤄짐 | 429, ORIGIN·Kafka 없음. 초과분도 사용량에 포함 |
| 정책 키 누락·형식 오류 | 접수 제어 정책을 판정할 수 없음 | 503, ORIGIN·Kafka 없음 |
| Redis 사용량 검사 연결 실패·타임아웃 | 제한 검사를 우회하는 가용성 정책 | 이후 검증·접수 계속. 이 기간 사용량 누락 가능 |
| 본문 상세 검증 실패 | 사용량 증가 후 중단 | 400, 중복 선점·ORIGIN·Kafka 없음 |
| 동일 고객 중복키 재요청 | Redis 실행 ID로 ORIGIN을 대조 | 같은 실행을 202로 반환·재발행할 수 있음. 원본과 충돌하면 409, 저장 확인 전이면 503 |
| 새 ORIGIN Put의 결과 불명확 | 동일 `executionId`로 강한 일관성 조회 | 같은 원본이 확인되면 202, 확인 불가하면 503. Redis 중복키를 성급히 해제하지 않음 |
| ORIGIN 저장 뒤 Kafka 발행 실패·ack 불명확 | API는 ORIGIN 접수 기준으로 202. 발행 복구 앱이 같은 원문·ID로 재발행 | Kafka 기록 중복 가능, 새 실행을 만들지 않음 |

```mermaid
flowchart LR
    A[ORIGIN RECEIVED 저장 확인] --> P[API가 event.received.v1 발행 시도]
    P --> U{발행 확인 여부}
    U -->|확인| M[PRE-SEND-MANAGER 소비]
    U -->|실패·불명확| G[ORIGIN 발행 복구 조회]
    G --> K[같은 executionId로 event.received.v1 재발행]
    K --> M
```

## 3. PRE-SEND-MANAGER의 계약·통신사 조회

```mermaid
flowchart TD
    E[event.received.v1 소비] --> C[Redis 계약 조회]
    C -->|누락| P[PostgreSQL 계약 조회]
    C -->|있음| V[계약·발송 조건 확인]
    P --> V
    V -->|발송 불가| F[실패 결과로 인계: 전문·토픽 미정]
    V -->|발송 가능| N[Redis 전화번호→통신사 조회]
    N -->|SKT·KT·LGU| B[전문·통신사·attemptId 고정]
    B --> T[해당 통신사 HTTP 발송 토픽]
    N -->|누락| U[PostgreSQL 조회 또는 보류 정책 미정]
```

계약 원본과 번호 매핑 원본은 PostgreSQL이며 CDC가 Redis를 갱신한다. **계약 캐시 누락 시 PostgreSQL 조회는 합의됐고, 번호→통신사 캐시 누락 시 행동은 미정**이다. 한 번 선택한 통신사와 전문·`attemptId`는 같은 실행의 재처리 동안 바꾸지 않는다. 현재 계약 테이블에 발송 전문 생성에 필요한 모든 정보가 있는 것도 아니므로 계약·설정 스키마는 추가 설계가 필요하다.

## 4. 업체의 즉시 응답이 실패

HTTP-SENDER는 업체 호출의 즉시 거절·재시도 코드·무응답을 관찰하고 발송 시각과 결과를 DynamoDB에 기록한 뒤 `MSG_RESULT`에 인계한다. **sender가 재시도·2차 전환·최종 실패를 독자적으로 결정하지 않는다.**

```mermaid
sequenceDiagram
    autonumber
    participant S as 통신사별 HTTP-SENDER
    participant P as 통신사 업체
    participant D as DynamoDB
    participant K as MSG_RESULT
    participant T as 후속 Kafka Topic
    participant M as EVENT-RESULT-MANAGER
    S->>P: HTTP 발송
    P-->>S: 즉시 실패 코드 또는 무응답
    S->>D: 호출 시각·응답·관찰 결과 기록
    S->>K: 즉시 실패 결과
    K->>M: 실행·attemptId·회차·코드 전달
    M->>D: 기존 상태·기한 확인 후 판단 조건부 저장
    alt 재시도 가능한 코드·횟수·기한
        M->>M: 다음 회차와 예정 시각 고정
        M->>T: 예정 시각에 같은 통신사 경로로 재인계
        Note over M,S: 신규 재시도 토픽·스케줄 방식 미정
    else 대체 대상 사유이고 fallbackAllowed
        M->>D: 1차 판단·2차 바인딩 고정
        M->>T: event.tcp.requested.v1 발행
    else 확정 가능한 영구 실패
        M->>D: 최종 실패 저장
        M->>T: event.finalized.v1 발행
    else 결과가 불명확한 상태
        M->>D: 운영 확인·원래 기한 유지
    end
```

이전 정책의 코드 예시는 `RETRY_1S`·`RETRY_10S`(최초 1회 + 최대 3회), `FALLBACK`(대체 대상), `REJECTED`(영구 거절)이다. 무응답은 실제 업체 처리 여부가 불명확할 수 있다. `HTTP_ERROR`·형식 오류 같은 결과를 자동 재시도로 단정하지 않는다. 새 `MSG_RESULT` 결과 전문과 코드별 분류는 구현 전에 확정해야 한다. 대체 대상이어도 `fallbackAllowed=false`이면 TCP를 호출하지 않는다.

## 5. 접수 성공 후 실패 웹훅

즉시 응답이 `ACCEPTED`였더라도 업체가 나중에 실패 웹훅을 보낼 수 있다. 이 경우 실패의 출처는 sender가 아니라 인증된 `receipt-api`다.

```mermaid
sequenceDiagram
    autonumber
    participant P as 통신사 업체
    participant W as receipt-api
    participant K as MSG_RESULT
    participant M as EVENT-RESULT-MANAGER
    participant D as DynamoDB
    P->>W: FAILED 웹훅 + executionId·attemptId·코드
    W->>W: 업체 인증·형식·업체·시도 정보 검증
    W->>K: 실패 웹훅 결과 발행
    K-->>W: 저장 확인
    W-->>P: 웹훅 접수 응답
    K->>M: 실패 결과
    M->>D: 실행·단계·회차·version·기한 확인
    alt 현재 유효한 실패
        M->>D: 코드별 재시도·2차 전환·최종 실패 중 하나 저장
    else 중복·지난 회차·이미 최종화
        M->>M: 기존 판단 유지, 추가 발송 없음
    end
```

유효한 웹훅 실패의 이후 분기는 [즉시 실패와 같은 정책](#4-업체의-즉시-응답이-실패)을 따른다. 웹훅이 먼저 도착하거나 중복·늦게 와도 확정 상태를 덮지 않는다. 업체가 재시도 회차를 웹훅에 어떻게 실어 돌려주는지는 신규 전문 계약에서 정해야 한다.

## 6. 웹훅 미수신과 1차 만료

즉시 `ACCEPTED` 뒤 웹훅이 오지 않으면 접수 성공만으로 최종 성공을 만들지 않는다. 이전 목표의 1차 deadline은 **최초 인입 +3시간**이며, Redis는 만료 후보 일정이고 DynamoDB는 최종 판단·복구의 기준이다.

```mermaid
flowchart TD
    A[ACCEPTED 후 웹훅 대기] --> R[Redis deadline 후보]
    A --> G[DynamoDB 복구 인덱스]
    R --> M[EVENT-RESULT-MANAGER]
    G --> M
    M --> D{원래 1차 deadline 경과·미완료?}
    D -->|아니오| W[남은 기한까지 대기]
    D -->|예| C[조건부 1차 만료 판단]
    C --> F{fallbackAllowed·대체 사유 충족?}
    F -->|예| T[2차 TCP 발송]
    F -->|아니오| E[EXPIRED 최종화]
```

Redis 일정이 유실돼도 DynamoDB 조회로 누락을 찾는다. 늦게 온 웹훅은 관측만 남기고 이미 확정한 만료·2차 전환·최종 결과를 바꾸지 않는다. 2차 전환을 늦게 처리해도 2차 deadline을 임의로 연장하지 않는다.

## 7. 2차 TCP 발송

2차는 모든 1차 실패의 기본 경로가 아니다. 기존 목표의 대체 사유는 `FALLBACK_REQUIRED`, `PRIMARY_EXPIRED`, `RETRY_EXHAUSTED_NO_RESPONSE`이며 최초 요청의 `fallbackAllowed`가 참이어야 한다. 현재 목표 이름은 `event.tcp.requested.v1` → `event-tcp-sender`다. 2차 결과 토픽은 기존 목표의 `event.tcp.outcome.v1`을 표기하며, `MSG_RESULT`로 통합할지는 아직 결정되지 않았다.

```mermaid
sequenceDiagram
    autonumber
    participant M as EVENT-RESULT-MANAGER
    participant D as DynamoDB
    participant K as Kafka
    participant T as event-tcp-sender
    participant P as TCP 2차 업체
    participant W as receipt-api
    M->>D: 1차 판단 시각·사유·2차 attemptId·deadline 고정
    M->>K: event.tcp.requested.v1 발행
    K->>T: 2차 명령
    T->>D: 1차 판단·2차 바인딩과 미완료 상태 확인
    T->>P: TCP 발송
    P-->>T: 즉시 접수 응답
    T->>D: 2차 호출 결과 기록
    T->>K: event.tcp.outcome.v1 / 즉시 결과
    K->>M: 2차 즉시 결과
    P->>W: 2차 최종 결과 웹훅
    W->>K: event.tcp.outcome.v1 / 웹훅
    K->>M: 2차 최종 결과
    M->>D: 유효 회차·기한 확인 후 최종 결과 고정
    M->>K: event.finalized.v1 발행
```

2차도 코드별 최대 3회 재시도와 웹훅 대기를 적용한다. 기존 목표의 2차 deadline은 **1차 결과 판단 시각 +4시간**이고, 실패·만료 뒤 세 번째 업체로 넘어가지 않는다. 2차 결과가 성공이든 실패든 최종 이벤트 이후에는 [8번](#8-최종-결과-이후-고객-통지와-정리)으로 합류한다. 이 다이어그램은 이전 DDB 선점 모델을 포함한 목표 기록이며, 신규 1차의 Redis 중복 제어와 2차 발송 보호를 어떻게 맞출지는 구현 전에 확정해야 한다.

## 8. 최종 결과 이후 고객 통지와 정리

`EVENT-RESULT-MANAGER`가 최종 결과를 DynamoDB에 고정한 뒤 `event.finalized.v1`로 인계한다. `delivery-result-worker`는 PostgreSQL에 **최종 이력·고객 통지 예약·정리 예약을 단일 트랜잭션**으로 저장한다. SQL commit 전에는 ORIGIN·STEP을 삭제하지 않는다.

```mermaid
flowchart TD
    F[event.finalized.v1] --> Q[PostgreSQL 이력·통지·정리 예약 commit]
    Q --> N[고객 HTTP 결과 통지]
    Q --> C[발송 데이터 정리]
    N --> A{204 수신?}
    A -->|예| D[SQL 통지 완료]
    A -->|아니오| R[같은 묶음으로 재전송 예약]
    R --> L{총 21회 소진?}
    L -->|아니오| N
    L -->|예| H[EXHAUSTED·운영 확인]
    C --> X[Redis 중복키 처리·DynamoDB 조건부 삭제]
    X --> Y[SQL 정리 완료]
```

| 케이스 | 후속 흐름 |
|---|---|
| 고객이 결과 HTTP 통지에 204 응답 | 동일 고객 최대 100건 묶음의 통지 완료를 SQL에 기록 |
| 고객 응답이 204가 아니거나 무응답 | 같은 묶음 ID·결과 ID로 SQL 예약에 따라 재전송. 최초 + 최대 20회, 총 21회. 소진하면 운영 확인 대상으로 보존 |
| 고객에게 이미 처리됐지만 204만 유실 | 같은 묶음이 다시 도착할 수 있으므로 고객도 묶음 ID로 중복 제거 |
| 고객 통지와 무관한 정리 | SQL 이력·예약 저장을 확인하고 ORIGIN·STEP을 조건부 삭제. 성공은 Redis 고객 중복키를 성공 시각부터 2시간 차단, 실패·만료는 해당 실행의 키를 제거 |
| SQL 저장 실패 | 최종 이벤트·DynamoDB 결과를 유지하고 SQL 인계를 재시도. 정리 작업은 시작하지 않음 |

정리와 고객 통지는 SQL commit 이후 병렬로 진행한다. 고객 통지 21회 한도와 업체 발송 단계별 최대 3회 재시도는 서로 다른 정책이다.

## 9. 중간 종료와 저장소 장애

| 중단 위치 | 재개 근거와 지켜야 할 경계 |
|---|---|
| ORIGIN 저장 직후 API 종료·최초 Kafka ack 불명확 | 발행 복구 앱이 ORIGIN의 동일 원문·`executionId`로 `event.received.v1` 재발행. Kafka 중복 가능 |
| PRE-SEND-MANAGER의 통신사 토픽 발행 ack 불명확 | 같은 통신사·전문·`attemptId`로 재인계. 업체 호출 중복은 sender의 Redis 제어와 업체의 `attemptId` 처리 범위에 의존 |
| Redis 발송 중복 정보 유실 | 내부 중복 제어가 약해진다. 동일 `attemptId`를 업체에 전달하지만 업체 중복 처리 계약과 실제 보장은 별도 확인 필요 |
| 업체 호출 뒤 sender의 DynamoDB 결과 기록 전 종료 | 업체 효과가 불명확하다. 무조건 새 호출하지 않고 저장 조회·운영 확인·원래 deadline 정책이 필요. 신규 복구 절차 미정 |
| DynamoDB 결과 기록 뒤 `MSG_RESULT` 발행 실패 | 저장된 관찰 결과를 같은 ID로 재발행해야 한다. 신규 sender의 복구 구현·인덱스는 미정 |
| `MSG_RESULT` 소비 후 Manager 판단 저장·후속 Kafka 발행 사이 종료 | DynamoDB에 고정한 판단·명령을 같은 ID로 재발행. 먼저 저장되지 않았다면 원본 결과를 재처리 |
| 웹훅의 Kafka 저장 실패·ack 불명확 | `receipt-api`가 성공 접수로 확정하지 않고 동일 웹훅 재전달을 수용. 중복 결과는 Manager가 조건부 상태로 수렴 |
| Redis deadline 일정 유실 | DynamoDB 복구 인덱스로 만료 후보 재발견. Redis 비어 있음 여부에만 의존하지 않음 |
| 최종 DDB 저장 뒤 `event.finalized.v1` 발행 실패 | DDB의 불변 최종 결과로 같은 최종 이벤트 재인계 |
| PostgreSQL commit 뒤 고객 통지·DDB 정리 전 종료 | SQL의 통지·정리 예약으로 각각 재개. 고객 통지 완료와 정리는 독립 |

이 표의 *목표 복구 경계*와 *현재 코드에서 검증된 복구*는 다르다. 신규 sender·Manager·`MSG_RESULT` 경로의 장애 주입 시험은 아직 수행하지 않았다.

## 구현 전에 확정할 인터페이스

- `MSG_RESULT`의 통합 전문: 즉시 응답과 웹훅의 구별, 통신사·단계·`attemptId`·`invocation`·업체 코드·발생 시각·중복 식별자.
- 신규 1차 재시도 명령의 통신사별 라우팅·지연 예약 방식. 기존 단일 sender용 `event.http.retry.v1`을 그대로 신규 경로로 읽지 않는다.
- 번호→통신사 Redis 캐시 누락 정책, 계약상 발송 불가 결과의 인계 전문·토픽, 계약·발송 설정 스키마.
- sender Redis 선점 TTL·진행 중 중복 처리·DynamoDB 기록 실패 복구, 업체별 동일 `attemptId` 보장 범위.
- 2차 토픽·Pod 최종 이름과 2차 즉시 응답·웹훅의 `MSG_RESULT` 통합 여부.

이전 경로의 상세 시험·장애 근거는 [기존 전체 흐름](04-요청-접수부터-최종-결과까지의-전체-흐름.md), [저장소 장애](09-저장소-장애-시-중단-범위와-복구-절차.md), [계약·번호 CDC](52-계약과-번호-통신사-CDC-캐시.md)에 보존한다. 신규 경로 구현이 진행되면 이 문서의 목표·미정·완료 표시를 갱신한다.
