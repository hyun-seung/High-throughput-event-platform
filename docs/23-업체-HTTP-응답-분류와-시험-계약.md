# 업체 HTTP 응답 분류와 시험 계약

최초 분류 구현·검증일: 2026-09-24. 이 문서의 최초 구현 범위는 `1b6b789` 기준이다. 이후 실패 저장·최대 3회 예약 재시도를 연결했으며 현재 동작은 [후속 구현 문서](24-실패-저장과-1차-재시도-예약.md)를 따른다. 아래의 미구현 설명과 검증 수치는 최초 분류 단계의 기록이다. 실제 업체 계약이 없으므로 1차 HTTP 테스트 서버와 Worker 사이의 개발용 계약을 정의했다. 코드는 업체별 표준이 아니며 실제 연동 때 adapter에서 매핑해야 한다.

## 이번에 구현한 범위

Worker가 정상 접수·재시도 대상·대체 발송 대상·영구 거절·무응답·판단 불가를 구분한다. HTTP 4xx/5xx에서도 본문의 업무 코드를 읽는다. 응답의 `deliveryId` 일치, `accepted` 존재, `processedAt` 존재와 성공 코드의 일관성을 확인한다. 정상 접수만 기존 DynamoDB ACCEPTED 저장으로 이어진다.

**분류는 자동 재시도·대체 발송 구현 완료를 뜻하지 않는다.** 비정상 응답은 분류된 예외로 listener에 전달한다. 현재는 기존 PROCESSING lease 및 REVIEW_REQUIRED 경로를 유지하며, 분류 결과 자체는 아직 DB에 저장하지 않는다. lease 만료 후 재선점하거나 메모리에서 바로 재호출하지 않는다. Kafka 재전달에도 유효 lease가 있는 Attempt는 업체를 추가 호출하지 않는다.

ACCEPTED는 업체의 접수 확인이며 최종 수신 성공이 아니다. 업체 결과 웹훅과 고객 결과 전달은 후속 구현이다.

## 요청과 응답

기존 `POST /api/v1/deliveries`, `Idempotency-Key: attemptId`를 유지한다. 테스트 요청의 payload에서만 `simulatorResultCode`를 지정한다. 생략하면 `ACCEPTED`다.

```json
{
  "deliveryId": "delivery-1",
  "accepted": false,
  "processedAt": "2026-09-24T00:00:01Z",
  "code": "RETRY_10S"
}
```

| 테스트 입력 코드 | 시뮬레이터 HTTP | accepted | Worker 분류 | 후속 업무 처리 목표 |
|---|---:|---|---|---|
| ACCEPTED / 생략 | 200 | true | 정상 응답 반환 | 업체 결과 대기 |
| RETRY_1S | 429 | false | RETRY_1S | 1초 뒤 1차 재시도 |
| RETRY_10S | 429 | false | RETRY_10S | 10초 뒤 1차 재시도 |
| FALLBACK | 503 | false | FALLBACK_REQUIRED | 최초 요청이 허용한 경우에만 2차 전환 |
| REJECTED | 400 | false | PERMANENT_REJECTION | 재시도하지 않는 거절 |

REJECTED는 이 테스트 계약에서 해당 요청을 영구 거절한다는 뜻이다. 업체별 오류 코드 정책의 일반 규칙으로 확장하지 않는다. 재시도 소진·만료 시 최종 상태와 대체 경로 판단은 요청 설정·deadline과 함께 후속 구현한다.

시뮬레이터의 명시적 실패는 호출 횟수만 올리고 처리 효과를 만들지 않는다. 같은 입력을 계속 보내면 같은 실패를 반환한다. ‘몇 회 실패 후 성공’ 동적 시나리오는 아직 없다. 알 수 없는 시나리오 값은 400으로 거부하며 tracking key를 만들지 않는다.

기존 `forceFail=true`는 유효한 시나리오 선택값보다 우선하고, 이전처럼 업무 코드 없는 500을 반환한다. Worker는 이를 HTTP_ERROR로 분류하며 HTTP 500만으로 재시도·대체 발송을 결정하지 않는다. 기존 3필드 성공 응답은 전환 호환성을 위해 code가 없어도 유효한 2xx·메시지 ID·처리 시각이면 허용한다.

## 무응답과 판단 불가

| 관측 | 분류 | 의미 |
|---|---|---|
| 연결 오류, 헤더 대기 또는 본문 수신 중 socket timeout/reset | NO_RESPONSE | 완전한 응답을 받지 못함. 외부 미처리를 의미하지 않음 |
| 빈 본문, 잘못된 JSON·필수 필드, 다른 deliveryId, 모순된 성공 표시, 알 수 없는 업무 코드 | INVALID_RESPONSE | 자동 성공·실패·대체 발송 판단에 사용할 수 없음 |
| 형식은 기존 계약과 같지만 코드 없는 accepted=false HTTP 오류 | HTTP_ERROR | HTTP 상태만으로 업무 정책을 추정하지 않음 |

관찰한 무응답은 합의대로 재시도할 대상이다. 이를 자동화하려면 관찰 결과와 예약을 내구성 있게 남겨야 한다. 발송 선점 후 프로세스가 종료되어 결과 자체가 저장되지 않은 경우는 계속 운영 확인 대상이다. 그 두 경우를 현재의 예외 객체만으로 재시작 후 구별할 수는 없다.

구조화 로그는 기존 Grafana 필터가 사용하는 `http_error`·`transport_error` outcome을 유지하고 `code`에 위의 제한된 분류값을 남긴다. `http_error`는 여기서 2xx 본문 업무 거절과 응답 계약 오류도 포함한다. 원시 응답 본문·파서 예외 체인은 Kafka 오류 로그로 전달하지 않는다. 요청 ID와 임의 업체 코드를 새 metric label로 추가하지 않았다.

## 검증과 수정 근거

```bash
JAVA_HOME=/path/to/jdk-21 ./mvnw -q -pl dispatch-worker,external-api-simulator -am test
```

변경 모듈과 공통 모듈에서 **50건 통과, DynamoDB 통합 8건 제외**. 이번에 HTTP wire 시험 20건·Dispatch 인계 시험 1건·시뮬레이터 시험 5건을 추가했다. 기존 저장소 통합 시험의 재실행이나 Kafka부터 고객 통지까지의 전체 시험 결과로 해석하지 않는다.

- 실제 loopback HTTP 서버와 운영 RestClient 설정으로 정상·기존 응답 호환, 오류 상태의 업무 코드, 다른 메시지 응답, 누락·모순·미등록 코드, 잘못된 본문을 검증했다.
- 헤더 대기 timeout은 처음에 INVALID_RESPONSE로 잘못 분류됐다. 로컬 Spring Web 7.0.8 소스의 `DefaultRestClient.readWithMessageConverters`에서 응답 추출 중 I/O가 일반 RestClientException으로 감싸질 수 있음을 확인했다. 원인 체인의 socket 오류도 판별하도록 수정했고 헤더 대기와 본문 수신 timeout 모두 NO_RESPONSE로 통과했다. JSON 파싱 오류는 계속 INVALID_RESPONSE다.
- 분류 예외가 Dispatch 밖으로 전달되고 ACCEPTED 저장·추가 HTTP 호출이 생기지 않는지 검증했다.
- 시뮬레이터 실패 효과 0, 이후 정상 호출의 효과 1, 잘못된 시나리오의 용량 미소비를 확인했다.

실행 중인 모니터링 환경은 기존 JAR 기준선을 유지했다. 이번 변경은 Maven 테스트로 검증했으며 새로운 전체 성능 시험이나 실행 중 앱 교체는 수행하지 않았다.

## 다음 구현 단위

1. 관찰한 실패·무응답과 `nextAttemptAt`, 재시도 횟수를 Attempt의 조건부 결과 저장에 연결한다. 정상 요청의 DynamoDB write는 늘리지 않는다.
2. 재시작해도 예약을 복구하고, 같은 예약을 중복 처리해도 외부 호출을 하나만 선점하도록 한다. 외부 결과 저장과 Kafka 인계 사이의 종료도 검증한다.
3. 최초 시도와 별도로 최대 3회 재시도, 최초 인입 기준 1차 deadline, 대체 발송 허용 조건을 적용한다.
4. TCP 2차 접수·업체 결과 웹훅·만료와 최종화·고객 묶음 결과 전달을 연결한다.
