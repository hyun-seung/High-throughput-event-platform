# ADR-026 MESSAGE-RECEIVED와 통신사별 HTTP 발송 분리

- Status: Accepted — 단계적 구현 중
- Date: 2026-10-02
- 대체 범위: ADR-025의 API → HTTP sender 직접 발행과 ADR-024의 단일 HTTP sender·발송 전 DynamoDB STEP 선점. ORIGIN 선접수, API의 계약 조회 제거, 202의 의미, HTTP·TCP 결과 판단 분리는 유지한다.

## 확정한 호출 흐름

1. `MESSAGE-RECEIVE-API`는 JWT를 확인하고 Redis의 [TPS·유형별 월 Quota](ADR-027-접수-API-Redis-TPS와-유형별-월-Quota.md)를 증가·판정한 뒤 본문·고객 중복을 확인하고 ORIGIN을 조건부 저장한다. 저장 확인 뒤 `message.received.v1`에 불변 접수 원문을 `executionId` key로 발행하고 202를 반환한다. API 발행 실패·불명확 시 ORIGIN 기반 복구 앱도 같은 토픽으로 재발행한다.
2. `PRE-SEND-MANAGER`는 `message.received.v1`을 소비한다. 고객 계약은 PostgreSQL CDC로 갱신된 Redis를 먼저 조회하고 누락 시 PostgreSQL에서 조회한다. 전화번호별 통신사 정보의 원본도 PostgreSQL이며 CDC로 Redis에 반영된다. Manager가 Redis의 번호 매핑을 조회해 통신사를 정한다. **번호 매핑이 없으면 첫 1차 HTTP 통신사를 SKT로 정한다.** 계약·발송 설정을 확인한 뒤 1차 업체 전문과 해당 통신사 시도의 `attemptId`를 만든다. 계약상 발송 불가이면 발송 토픽에 넣지 않고 실패 결과로 인계한다.
3. Manager는 동일한 `HttpSendCommand` 형식을 통신사별 토픽 하나에 발행한다. SKT는 `message.skt.http.send.v1`, KT는 `message.kt.http.send.v1`, LGU+는 `message.lgu.http.send.v1`이다. 같은 명령의 발행 재시도에서는 통신사·전문·`attemptId`·`sendRequestId`를 고정하고, 새 발송 회차나 다음 통신사에는 새 `sendRequestId`를 부여한다.
4. 세 `MESSAGE-HTTP-SENDER` Deployment는 같은 코드와 전문 형식을 사용한다. 각 Pod는 자기 통신사 토픽만 소비하고 자기 통신사 주소로만 발송한다. Redis가 발송 시도 중복을 제어한다. Redis 유실 또는 발송 후 기록 전 종료에는 동일 `attemptId`를 업체에 전달하고 업체의 중복 처리에 의존하는 범위가 남는다. **업체가 1차 HTTP 발송에 `200 OK`를 반환하면 sender는 발송 시각·접수 응답을 DynamoDB에 갱신하고 해당 호출을 끝낸다. 이 성공 경로에서 Kafka 결과를 발행하지 않는다.** 명시적 비-200이면 응답에 실패 코드가 있고 이 발송의 웹훅은 오지 않는다. 응답 코드의 Manager 인계 방식은 미정이다. 응답 자체가 없는 타임아웃은 별도 결과 불명이다.
5. HTTP 200을 받은 발송의 **웹훅이 해당 1차 시도의 최종 결과**다. 업체는 한 번의 웹훅에 **1~100건**의 메시지 결과를 보낼 수 있다. `WEBHOOK-RECEIVE-API`는 웹훅을 받아 검증한 뒤 `MSG_RESULT`에 발행하고 Kafka 저장을 확인한 후 업체에 접수 응답을 한다. API는 업무 결과를 판단하지 않는다. `MSG-RESULT-MANAGER`가 각 결과의 실행·시도·회차와 현재 상태를 확인한다. 성공 결과면 최종 판단을 저장하고 `MSG-RESULT-FINALIZED`를 발행한다. 실패 결과면 먼저 합의된 1차 통신사 불일치 이동(`SKT` → `KT` → `LGU+`) 여부를 확인하고, 해당하지 않을 때 2차 발송 조건을 확인한다. 2차 대상이면 2차 발송 명령을 발행하고 해당 결과 처리를 끝낸다. 이동·2차 대상이 아닌 실패의 최종 처리 정책은 별도 확정이 필요하다. `MSG_RESULT`와 `MSG-RESULT-FINALIZED`는 신규 경로의 물리 토픽명이다.

| 통신사 | Kafka 토픽 | Deployment |
|---|---|---|
| SKT | `message.skt.http.send.v1` | `messaging-skt-http-sender` |
| KT | `message.kt.http.send.v1` | `messaging-kt-http-sender` |
| LGU+ | `message.lgu.http.send.v1` | `messaging-lgu-http-sender` |

각 통신사의 토픽 적체·소비 속도·Pod 장애를 분리한다. Kafka·Redis·DynamoDB 같은 공용 인프라의 장애는 별도 장애 경계다. 업체 주소는 Pod 설정으로 관리하고 Kafka 전문에는 URL 대신 통신사 코드를 담는다.

## 아직 정할 정책과 현재 구현 경계

- 한 웹훅 요청의 1~100개 결과를 Kafka에 묶음 1레코드로 발행할지 결과별 레코드로 분리할지 정해야 한다. 결과별 `executionId` key·멱등 ID를 권장하지만, 일부 발행 성공 후 API 응답 실패·업체 재전달을 처리해야 한다. 한 건이 잘못된 묶음을 전체 거절할지 부분 수용할지도 업체 응답 계약과 맞춰야 한다. API는 필요한 Kafka 저장 확인 전 웹훅 성공 응답을 하지 않는다.
- 명시적 비-200에는 실패 코드가 있고 웹훅이 오지 않으므로 Manager 인계 방식이 필요하다. [케이스별 Flow의 세 선택지](../53-신규-메시지-케이스별-Call-Flow.md#즉시-실패-인계-선택지) 중 비-200만 `MSG_RESULT`에 `source=HTTP_RESPONSE`로 발행하는 방안을 권장한다. HTTP 200 성공 경로에서는 sender가 Kafka에 발행하지 않는다. 타임아웃은 명시적 비-200과 구분하고, HTTP 200 뒤 웹훅 미수신 만료 트리거도 별도로 둔다.
- 웹훅이 sender의 DynamoDB HTTP 200 기록보다 먼저 도착해도 해당 시도의 최종 결과는 웹훅이다. 늦은 sender 갱신이 웹훅 판단을 덮지 않도록 조건부 상태 전이를 구현해야 한다. 2차 대상이 아닌 실패의 최종화·동일 통신사 재시도·운영 확인 기준도 필요하다.
- 번호→통신사 Redis 키가 없으면 PostgreSQL을 인라인 조회하지 않고 SKT부터 1차 HTTP 발송한다. "우리 통신사 아님" 결과를 받을 때마다 다음 통신사로 이동하되, 이미 발행한 시도의 통신사·전문·`attemptId`는 재처리 동안 바꾸지 않는다. 매핑이 있는데도 해당 통신사가 불일치라고 응답할 때의 탐색 순서, LGU+까지 모두 불일치일 때의 최종 결과, 발견한 통신사 정보를 PostgreSQL 원본에 갱신할지는 미정이다.
- 통신사 불일치의 업체별 응답·웹훅 코드를 공통 분류로 매핑하고, 이동 판단을 중복·늦은 웹훅과 경합 없이 한 번만 저장해야 한다. 다음 통신사 `HttpSendCommand`의 내구성 있는 원본과 발행 주체는 신규 Manager 구현 전에 정한다. 통신사 이동은 같은 통신사의 호출 재시도 회차나 TCP 2차 전환 횟수에 섞지 않는다.
- Redis 선점의 TTL·진행 중 중복 처리·발송 후 DynamoDB 기록 실패 복구, 업체별 동일 `attemptId` 중복 처리 계약은 sender 구현 전에 구체화해야 한다.
- 기존 `messaging-http-sender`의 `message.http.outcome.v1` 발행은 이전 경로다. 신규 1차 sender는 `200 OK`에서 Kafka 결과를 발행하지 않으며, `WEBHOOK-RECEIVE-API`와 `MSG-RESULT-MANAGER`의 신규 경로는 아직 구현하지 않았다. 2차 발송은 기존 목표에서 `message.tcp.requested.v1`/`messaging-tcp-sender`로 불렸으며 신규 1차와의 네이밍 통일은 아직 결정되지 않았다.
- 현재 코드는 API의 Redis TPS·분류별 월 Quota, API·ORIGIN 복구 앱의 `message.received.v1` 발행, 통신사별 토픽·`sendRequestId`를 포함한 공통 명령 타입, 로컬 CDC 소비와 Redis 투영까지 반영했다. `messaging-pre-send-manager` 모듈에는 계약 Redis 우선·누락 시 PostgreSQL 조회, 번호 Redis 조회·누락 시 SKT 선택, 첫 통신사를 ORIGIN에 조건부로 고정하는 코드가 있다. 이 ORIGIN 기록은 재전달 시 경로를 유지하기 위한 것이며 Sender의 Redis 중복 선점과 별개다. Kafka 소비·전문 생성과 통신사별 소비 Pod, Redis 발송 중복 제어, 발송 후 DynamoDB 기록은 아직 구현하지 않았다. 기존 `messaging-http-sender`는 이전 `message.http.requested.v1`과 발송 전 STEP 선점 모델의 코드이며 신규 경로의 소비자가 아니다. 신규 `/api/v1/messages`는 기본 비활성화다.
- PostgreSQL 논리 복제·Debezium·Redis 투영의 로컬 준비는 [계약·번호 CDC 캐시](../52-계약과-번호-통신사-CDC-캐시.md)에 기록한다. `PRE-SEND-MANAGER`의 참조 조회 코드는 추가됐고, 발송 판단·명령 발행은 아직 연결되지 않았다.
