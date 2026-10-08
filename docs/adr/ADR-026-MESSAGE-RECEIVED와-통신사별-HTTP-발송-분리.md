# ADR-026 MESSAGE-RECEIVED와 통신사별 HTTP 발송 분리

- Status: Accepted
- Date: 2026-10-02

## 확정한 호출 흐름

1. `MSG-RECEIVE-API`는 요청별 `traceId`로 인입을 추적하고 JWT를 확인한 뒤 Redis의 [TPS·유형별 월 Quota](ADR-027-접수-API-Redis-TPS와-유형별-월-Quota.md)를 증가·판정한다. 고객 `messageId`의 UTF-8 최대 40바이트·본문·고객 중복을 확인한 뒤 하이픈 없는 UUID 32자리의 고정 `clientMsgId`로 ORIGIN을 조건부 저장한다. 저장 확인 뒤 `message.received.v1`에 불변 접수 원문을 `clientMsgId` key로 발행하고 202에 같은 값을 반환한다. API 발행 실패·불명확 시 ORIGIN 기반 복구 앱도 같은 토픽으로 재발행한다.
2. `PRE-SEND-MANAGER`는 `message.received.v1`을 소비한다. 고객 계약은 PostgreSQL CDC로 갱신된 Redis를 먼저 조회하고 누락 시 PostgreSQL에서 조회한다. 전화번호별 통신사 정보의 원본도 PostgreSQL이며 CDC로 Redis에 반영된다. Manager가 Redis의 번호 매핑을 조회해 통신사를 정한다. **번호 매핑이 없으면 첫 1차 HTTP 통신사를 SKT로 정한다.** 계약·발송 설정을 확인한 뒤 1차 업체 전문과 해당 통신사 시도의 `attemptId`를 만든다. 계약상 발송 불가이면 발송 토픽에 넣지 않고 실패 결과로 인계한다.
3. Manager는 동일한 `HttpSendCommand` 형식을 통신사별 토픽 하나에 발행한다. SKT는 `message.skt.http.send.v1`, KT는 `message.kt.http.send.v1`, LGU+는 `message.lgu.http.send.v1`이다. 명령의 업체 전문 `clientMsgId`에 접수 ID를 그대로 담으며 최대 40바이트를 검사한다. 같은 명령의 발행 재시도에서는 통신사·전문·`attemptId`를 고정하고, 새 회차나 다음 통신사에도 같은 `clientMsgId`를 사용한다. 통신사·회차는 내부 시도 상태로 관리한다.
4. `MSG-SKT-SENDER`·`MSG-KT-SENDER`·`MSG-LGU-SENDER` Deployment는 같은 코드와 전문 형식을 사용한다. 각 Pod는 자기 통신사 토픽만 소비하고 자기 통신사 주소로만 발송한다. Redis가 발송 시도 중복을 제어한다. Redis 유실 또는 발송 후 기록 전 종료에도 업체에는 같은 `clientMsgId`를 전달한다. 업체는 발송 중·성공한 같은 ID의 중복 발송을 약 2시간 차단하며 실패한 호출에는 ID를 재사용할 수 있다고 파악했다. 정확한 보장 시간·기산 시점은 확인이 필요하다. **업체가 1차 HTTP 발송에 `200 OK`를 반환하면 sender는 발송 시각·접수 응답을 DynamoDB에 갱신하고 해당 호출을 끝낸다. 이 성공 경로에서 Kafka 결과를 발행하지 않는다.** 명시적 비-200이면 응답에 실패 코드가 있고 이 발송의 웹훅은 오지 않는다. Sender가 실패 코드·시도 정보를 DynamoDB에 기록하고 `MSG_RESULT`에 직접 발행한다. HTTP 응답이 5초 동안 없으면 sender가 `HTTP_TIMEOUT` 관찰을 저장하고 `MSG_RESULT`에 직접 발행한다. Manager가 타임아웃 1분 뒤 같은 통신사 재발송을 예약한다.
5. HTTP 200을 받은 발송의 **웹훅이 해당 1차 호출의 최종 결과**다. 업체는 한 번의 웹훅에 **1~100건**의 메시지 결과를 보낼 수 있고 각 결과의 `clientMsgId`는 발송 요청과 같다. `status=success`에는 `error`가 없고 `status=fail`에는 `error: {code, message}`가 반드시 있다. 별도 업체 결과 ID·회차 값은 주지 않는다. `error.code`는 5자리 JSON 숫자이며 1차 통신사 실패는 6만 대역이다. 한 요청 안에는 중복 ID가 없다. `WEBHOOK-RECEIVE-API`는 인입별 `traceId`를 발급하고 업체 인증·형식·건수·수신 크기만 검증한 뒤 1~100건 전체를 `MSG_RESULT` 1레코드(`traceId` key)로 발행한다. Kafka 저장을 확인한 후 업체에 접수 응답을 한다. API는 개별 메시지의 DynamoDB 조회·업무 판단을 하지 않는다. **API가 실패 응답을 반환하면 업체는 같은 묶음을 재전송**한다. `MSG-RESULT-MANAGER`는 배치 항목의 `clientMsgId`로 현재 저장 시도를 찾고 결과 중복을 조건부 상태 전이로 제어한다. 한 AP 안에서 항목들을 제한된 병렬성으로 처리하고 모두 내구성 있게 처리한 뒤 배치 offset을 완료한다. 성공 결과면 최종 판단을 저장하고 `MSG-RESULT-FINALIZED`를 발행한다. 실패 코드 `66001`이면 SKT → KT → LGU+에서 이미 시도한 통신사를 제외한 순서로 이동하고, `66002`이면 **실패 판단 1분 후 동일 통신사**에 같은 ID로 새 회차를 발송한다. 다른 6만 대역 실패는 1차를 닫고 최초 요청의 `secondarySendPayload`가 있으면 TCP 2차로 인계하며, 없으면 최종 실패·고객 웹훅으로 인계한다. 5초 무응답도 1분 후 최대 3회 재발송하고 소진하면 `40003`으로 같은 기준을 적용한다. 이전 실패 웹훅이 다음 발송 뒤 늦게 재전송되면 시도별 구분을 보장할 수 없다. `MSG_RESULT`와 `MSG-RESULT-FINALIZED`는 신규 경로의 물리 토픽명이다.

| 통신사 | Kafka 토픽 | Deployment |
|---|---|---|
| SKT | `message.skt.http.send.v1` | `MSG-SKT-SENDER` |
| KT | `message.kt.http.send.v1` | `MSG-KT-SENDER` |
| LGU+ | `message.lgu.http.send.v1` | `MSG-LGU-SENDER` |

각 통신사의 토픽 적체·소비 속도·Pod 장애를 분리한다. Kafka·Redis·DynamoDB 같은 공용 인프라의 장애는 별도 장애 경계다. 업체 주소는 Pod 설정으로 관리하고 Kafka 전문에는 URL 대신 통신사 코드를 담는다.

## 남은 검증 경계

업체가 같은 `clientMsgId`의 발송 중·성공 중복을 약 2시간 차단하는 범위와 실제 실패 코드 매핑을 확인해야 한다. 웹훅에는 회차 식별자가 없으므로 이전 실패 웹훅의 늦은 재전송을 현재 회차와 구분할 수 없다. 결과 Manager는 이미 확정한 최종 결과를 늦은 웹훅으로 바꾸지 않는다. 2차 TCP는 현재 임시 전문으로 동작하며 실제 업체 규격을 확인한 뒤 어댑터를 교체한다. 고객별 URL이 없는 웹훅 명령은 SQL에 보류한다. 상세 상태와 재시도는 [Call Flow](../53-신규-메시지-케이스별-Call-Flow.md)를 따른다.
