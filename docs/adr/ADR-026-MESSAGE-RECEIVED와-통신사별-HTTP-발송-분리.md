# ADR-026 MESSAGE-RECEIVED와 통신사별 HTTP 발송 분리

- Status: Accepted — 단계적 구현 중
- Date: 2026-10-02
- 대체 범위: ADR-025의 API → HTTP sender 직접 발행과 ADR-024의 단일 HTTP sender·발송 전 DynamoDB STEP 선점. ORIGIN 선접수, API의 계약 조회 제거, 202의 의미, HTTP·TCP 결과 판단 분리는 유지한다.

## 확정한 호출 흐름

1. `MSG-RECEIVE-API`는 요청별 `traceId`로 인입을 추적하고 JWT를 확인한 뒤 Redis의 [TPS·유형별 월 Quota](ADR-027-접수-API-Redis-TPS와-유형별-월-Quota.md)를 증가·판정한다. 고객 `messageId`의 UTF-8 최대 40바이트·본문·고객 중복을 확인한 뒤 하이픈 없는 UUID 32자리의 고정 `sendRequestId`(`R`)로 ORIGIN을 조건부 저장한다. 저장 확인 뒤 `message.received.v1`에 불변 접수 원문을 `R` key로 발행하고 202에 `R`을 반환한다. 현행 이벤트의 `executionId`에는 같은 값이 담겨 있으며 새 계약으로 이관 중이다. API 발행 실패·불명확 시 ORIGIN 기반 복구 앱도 같은 토픽으로 재발행한다.
2. `PRE-SEND-MANAGER`는 `message.received.v1`을 소비한다. 고객 계약은 PostgreSQL CDC로 갱신된 Redis를 먼저 조회하고 누락 시 PostgreSQL에서 조회한다. 전화번호별 통신사 정보의 원본도 PostgreSQL이며 CDC로 Redis에 반영된다. Manager가 Redis의 번호 매핑을 조회해 통신사를 정한다. **번호 매핑이 없으면 첫 1차 HTTP 통신사를 SKT로 정한다.** 계약·발송 설정을 확인한 뒤 1차 업체 전문과 해당 통신사 시도의 `attemptId`를 만든다. 계약상 발송 불가이면 발송 토픽에 넣지 않고 실패 결과로 인계한다.
3. Manager는 동일한 `HttpSendCommand` 형식을 통신사별 토픽 하나에 발행한다. SKT는 `message.skt.http.send.v1`, KT는 `message.kt.http.send.v1`, LGU+는 `message.lgu.http.send.v1`이다. 명령의 고정 `R`과 최대 40바이트의 업체 전송 ID(`R:SKT:1`은 38바이트)를 함께 담고, 발송 요청에서는 `clientMsgId` 필드로 전달한다. 같은 명령의 발행 재시도에서는 통신사·전문·`attemptId`·파생 ID를 고정하고, 새 회차나 다음 통신사에는 같은 `R`에서 새 파생 ID를 만든다.
4. `MSG-SKT-SENDER`·`MSG-KT-SENDER`·`MSG-LGU-SENDER` Deployment는 같은 코드와 전문 형식을 사용한다. 각 Pod는 자기 통신사 토픽만 소비하고 자기 통신사 주소로만 발송한다. Redis가 발송 시도 중복을 제어한다. Redis 유실 또는 발송 후 기록 전 종료에는 같은 명령의 파생 `sendRequestId`를 업체에 전달한다. 업체는 발송 중·성공한 같은 ID의 중복 발송을 약 2시간 차단하며 실패한 ID는 재사용 가능하다고 파악했다. 정확한 보장 시간·기산 시점은 확인이 필요하다. **업체가 1차 HTTP 발송에 `200 OK`를 반환하면 sender는 발송 시각·접수 응답을 DynamoDB에 갱신하고 해당 호출을 끝낸다. 이 성공 경로에서 Kafka 결과를 발행하지 않는다.** 명시적 비-200이면 응답에 실패 코드가 있고 이 발송의 웹훅은 오지 않는다. Sender가 실패 코드·시도 정보를 DynamoDB에 기록하고 `MSG_RESULT`에 직접 발행한다. 응답 자체가 없는 타임아웃은 별도 결과 불명이다.
5. HTTP 200을 받은 발송의 **웹훅이 해당 1차 호출의 최종 결과**다. 업체는 한 번의 웹훅에 **1~100건**의 메시지 결과를 보낼 수 있고, 각 결과는 `clientMsgId`에 그 호출의 파생 `sendRequestId`를 그대로 담는다. `status=success`에는 `error`가 없고 `status=fail`에는 `error: {code, message}`가 반드시 있다. 별도 업체 결과 ID·회차 값은 주지 않는다. `error.code`는 5자리 JSON 숫자이며 1차 통신사 실패는 6만 대역이다. 한 요청 안에는 중복 ID가 없다. `WEBHOOK-RECEIVE-API`는 인입별 `traceId`를 발급하고 업체 인증·형식·건수·수신 크기만 검증한 뒤 1~100건 전체를 `MSG_RESULT` 1레코드(`traceId` key)로 발행한다. Kafka 저장을 확인한 후 업체에 접수 응답을 한다. API는 개별 메시지의 DynamoDB 조회·업무 판단을 하지 않는다. **API가 실패 응답을 반환하면 업체는 같은 묶음을 재전송**한다. API는 업무 결과를 판단하지 않는다. `MSG-RESULT-MANAGER`가 배치 항목마다 `clientMsgId`에서 고정 `R`·통신사·회차를 얻어 저장 시도와 대조하고 재전송된 결과의 중복을 제거한다. 한 AP 안에서 항목들을 제한된 병렬성으로 처리하고 모두 내구성 있게 처리한 뒤 배치 offset을 완료한다. 추가 배치 분리 AP나 항목별 Kafka 재발행 토픽은 두지 않으며, 한 항목의 일시 장애에서는 배치 전체를 재처리한다. 성공 결과면 최종 판단을 저장하고 `MSG-RESULT-FINALIZED`를 발행한다. 실패 코드 `66001`(자사 통신사 아님)이면 SKT → KT → LGU+에서 이미 시도한 통신사를 제외한 순서로 이동하고, `66002`(TPS 초과)이면 **실패 판단 1분 후 동일 통신사**에 새 회차로 재발송한다. 다른 실패는 2차 발송 조건을 확인한다. 2차 대상이면 2차 발송 명령을 발행하고 해당 결과 처리를 끝낸다. 이동·재발송·2차 대상이 아닌 실패의 최종 처리 정책은 별도 확정이 필요하다. `MSG_RESULT`와 `MSG-RESULT-FINALIZED`는 신규 경로의 물리 토픽명이다.

| 통신사 | Kafka 토픽 | Deployment |
|---|---|---|
| SKT | `message.skt.http.send.v1` | `MSG-SKT-SENDER` |
| KT | `message.kt.http.send.v1` | `MSG-KT-SENDER` |
| LGU+ | `message.lgu.http.send.v1` | `MSG-LGU-SENDER` |

각 통신사의 토픽 적체·소비 속도·Pod 장애를 분리한다. Kafka·Redis·DynamoDB 같은 공용 인프라의 장애는 별도 장애 경계다. 업체 주소는 Pod 설정으로 관리하고 Kafka 전문에는 URL 대신 통신사 코드를 담는다.

## 아직 정할 정책과 현재 구현 경계

- 웹훅 API 경량화를 위해 한 웹훅 요청의 1~100개 결과를 Kafka 묶음 1레코드로 발행한다. 한 요청 안의 파생 `sendRequestId`는 유일하지만 API 실패 응답 뒤 재전송 요청에는 같은 결과가 다시 포함된다. 업체의 재전송 최대 기간은 없으며 간격은 미확인이다. 서로 다른 배치는 서로 다른 `traceId` key로 다른 파티션에서 처리될 수 있으므로 결과별 고정 `R`·회차의 DynamoDB 조건부 상태로 중복과 순서 역전을 해결한다. API는 배치 1레코드의 Kafka 저장 확인 전 웹훅 성공 응답을 하지 않는다. 영구적으로 잘못된 한 항목의 격리 방식과 HTTP 요청·Kafka 레코드 최대 바이트는 구현 전에 정한다. 1차 최초 인입+3시간, 2차 1차 판단+4시간은 업무 결과 판단 기한이며 API의 웹훅 수신 기한이 아니다.
- 명시적 비-200 실패는 sender가 `source=HTTP_RESPONSE` 결과를 `MSG_RESULT`에 직접 발행한다. 웹훅은 `source=WEBHOOK`으로 구분한다. 실패 관찰·발행 대기 상태를 DynamoDB에 기록하고 ack 불명 시 같은 `resultId`로 복구한다. HTTP 200 성공 경로에서는 sender가 Kafka에 발행하지 않는다. 타임아웃은 명시적 비-200과 구분하고, HTTP 200 뒤 웹훅 미수신 만료 트리거도 별도로 둔다. 오류 코드 대역과 원본 코드 보존 방식은 [54 오류 코드 계약](../54-메시징-오류-코드와-결과-인계-계약.md)을 따른다.
- 웹훅이 sender의 DynamoDB HTTP 200 기록보다 먼저 도착해도 해당 시도의 최종 결과는 웹훅이다. 늦은 sender 갱신이 웹훅 판단을 덮지 않도록 조건부 상태 전이를 구현해야 한다. 2차 대상이 아닌 실패의 최종화·동일 통신사 재시도·운영 확인 기준도 필요하다.
- 번호→통신사 Redis 키가 없으면 PostgreSQL을 인라인 조회하지 않고 SKT부터 1차 HTTP 발송한다. `66001`(자사 통신사 아님) 결과에서만 다음 통신사로 이동하며 SKT → KT → LGU+에서 이미 시도한 통신사를 제외한다. 이미 발행한 시도의 통신사·전문·`attemptId`는 재처리 동안 바꾸지 않는다. 세 통신사가 모두 불일치할 때의 최종 결과와 발견한 통신사 정보를 PostgreSQL 원본에 갱신할지는 미정이다.
- 통신사 불일치의 업체별 응답·웹훅 원본 코드를 `66001`로 매핑하고, TPS 초과 원본 코드는 `66002`로 매핑한다. 이동·1분 후 재발송 판단을 중복·늦은 웹훅과 경합 없이 한 번만 저장해야 한다. `66002`는 최초 발송 뒤 같은 통신사에 최대 3회 재시도(총 최대 4회)하며, 소진 시 1차 실패 `40001`로 고정한다. 세 통신사의 `66001` 소진은 1차 실패 `40002`로 고정하고 2차 발송 대상 여부를 검사한다. 다음 통신사 및 동일 통신사 새 회차 `HttpSendCommand`의 내구성 있는 원본과 발행 주체는 신규 Manager 구현 전에 정한다. 통신사별 공유 속도 제한은 아직 미정이다. 통신사 이동은 같은 통신사의 호출 재시도 회차나 TCP 2차 전환 횟수에 섞지 않는다.
- Redis 선점의 TTL·진행 중 중복 처리·발송 후 DynamoDB 기록 실패 복구, 업체의 약 2시간 중복 차단 확인과 만료 뒤 내부 재전달 방지 범위는 sender 구현 전에 구체화해야 한다. 업체의 재전송 최대 기간이 없는 늦은 웹훅은 인증·형식 검증 뒤 수신하되 이미 확정한 업무 결과를 바꾸지 않는다. 정리된 실행·알 수 없는 `sendRequestId`의 관측 보존 위치와 API 응답 코드는 후속 결정이다.
- 기존 `messaging-http-sender`의 `message.http.outcome.v1` 발행은 이전 경로다. 신규 1차 sender는 `200 OK`에서 Kafka 결과를 발행하지 않으며, `WEBHOOK-RECEIVE-API`와 `MSG-RESULT-MANAGER`의 신규 경로는 아직 구현하지 않았다. 2차 발송 AP명은 `MSG-TCP-SENDER`로 확정했다. 현재 목표 토픽은 `message.tcp.requested.v1`이며 물리 토픽 최종 이름은 별도로 정한다.
- 현재 코드는 API의 Redis TPS·분류별 월 Quota, API·ORIGIN 복구 앱의 `message.received.v1` 발행, 통신사별 토픽·`sendRequestId`를 포함한 공통 명령 타입, 로컬 CDC 소비와 Redis 투영까지 반영했다. `messaging-pre-send-manager` 모듈에는 계약 Redis 우선·누락 시 PostgreSQL 조회, 번호 Redis 조회·누락 시 SKT 선택, 첫 통신사를 ORIGIN에 조건부로 고정하는 코드가 있다. 이 ORIGIN 기록은 재전달 시 경로를 유지하기 위한 것이며 Sender의 Redis 중복 선점과 별개다. Kafka 소비·전문 생성과 통신사별 소비 Pod, Redis 발송 중복 제어, 발송 후 DynamoDB 기록은 아직 구현하지 않았다. 기존 `messaging-http-sender`는 이전 `message.http.requested.v1`과 발송 전 STEP 선점 모델의 코드이며 신규 경로의 소비자가 아니다. 신규 `/api/v1/messages`는 기본 비활성화다.
- PostgreSQL 논리 복제·Debezium·Redis 투영의 로컬 준비는 [계약·번호 CDC 캐시](../52-계약과-번호-통신사-CDC-캐시.md)에 기록한다. `PRE-SEND-MANAGER`의 참조 조회 코드는 추가됐고, 발송 판단·명령 발행은 아직 연결되지 않았다.
