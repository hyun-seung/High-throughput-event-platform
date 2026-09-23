# 접수 실패 격리와 DLT 재처리 절차

작성일: 2026-09-23. 대상은 `delivery-ingress-worker`의 접수 Consumer다. 외부 업체 호출 재시도 및 `dispatch-worker` 오류 처리와 구분한다.

## 1. 처리 규칙

```text
접수 원본 소비
→ DynamoDB 저장 또는 기존 동일 요청 확인
→ DispatchRequested 발행 ack 확인
→ 원본 offset commit

처리 실패
→ 오류 분류에 따라 재시도
→ DLT 발행 및 ack 확인
→ 원본 offset commit

DLT 발행 실패·응답 미확인
→ 원본 offset 유지
→ 원본 재전달 및 복구 재시도
```

DLT에 저장됐다는 것은 요청이 성공하거나 최종 실패로 확정됐다는 뜻이 아니다. 처리하지 못한 원본을 내구성 있게 격리한 상태다. 자동 만료·운영 조회·고객 최종 결과와의 연결은 후속 구현이며, 현재 DLT 보존만으로 전체 업무 기한 보장을 달성했다고 표현하지 않는다.

## 2. 오류 분류와 개발 설정

| 오류 | 처리 |
|---|---|
| 같은 멱등키의 다른 입력 | `IdempotencyConflictException`으로 구분하고 즉시 DLT 격리. 기존 원본은 변경하지 않음 |
| JSON 역직렬화 오류 등 Spring Kafka가 복구 불가능으로 분류한 오류 | 재시도 없이 DLT에 원본 바이트 보존 |
| DB 저장·읽기 오류, 후속 Kafka 발행 오류 등 나머지 처리 실패 | 개발 기본값 1초 간격으로 2회 재시도한 뒤 DLT 격리 |
| DLT 발행 오류·timeout | 복구 완료로 간주하지 않음. 실패 원본과 뒤쪽 미처리 record의 offset을 건너뛰지 않음 |

`INGRESS_RETRY_INTERVAL`과 `INGRESS_MAX_RETRIES`는 접수 Consumer 재전달 설정이다. 최초 처리 외 2회 재시도가 기본값이며, 사용자가 정한 외부 업체 발송 재시도 3회나 고객 웹훅 재전송 20회와 관련 없다. 동일 Consumer 스레드가 담당한 다른 파티션도 재시도 대기의 영향을 받을 수 있으며, 완전한 파티션별 격리를 주장하지 않는다.

`AckMode.RECORD`를 명시하고 `DeadLetterPublishingRecoverer.setFailIfSendResultIsError(true)`로 DLT 발행 결과를 확인한다. Producer는 `acks=all`, idempotence를 유지한다. 소스 파티션 번호를 그대로 사용하므로 DLT 파티션 수는 원본 이상이어야 한다. 파티션 확인 후 임의의 다른 파티션으로 보내는 동작은 사용하지 않으며, 없는 파티션에 대한 전송은 실패 처리한다.

Kafka 발행 제한은 metadata/buffer 대기 5초, 개별 요청 5초, 발행 결과 10초의 개발 기본값을 사용한다. recoverer의 ack 대기는 producer의 발행 제한과 Spring Kafka의 여유 시간을 반영하므로 10초가 전체 Consumer 처리의 절대 상한이라는 뜻은 아니다. 긴 대기 설정은 `max.poll.interval.ms`와 함께 검토한다.

**공식 근거:** Spring Kafka의 기본 오류 복구는 재시도 소진 후 기록만 남길 수 있다. 명시적인 DLT recoverer를 연결하고, 복구 실패 시 다시 원본을 처리하도록 설정해야 프로젝트의 인계 조건을 충족한다. **프로젝트 판단:** 단순 로그 후 건너뛰기를 피하고, Kafka 저장 확인을 기준으로 처리 책임을 넘긴다. [Spring Kafka 오류 처리](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html), [Consumer commit 동작](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html). 구현 API는 프로젝트가 사용하는 Spring Kafka 4.1.0 소스와 대조했다.

## 3. 보존하는 정보와 보장 범위

- 정상 역직렬화된 요청은 기존 이벤트 식별자·인입 시각·payload를 유지한다.
- 역직렬화 실패의 key/value는 원본 바이트로 발행할 수 있도록 String/JSON과 byte 배열 직렬화를 함께 지원한다.
- 원래 topic·partition·offset·예외 정보는 Spring Kafka DLT header로 남긴다. 업무 결과와 달리 운영 분석을 위한 정보다.
- DLT ack 직후 원본 offset commit 전에 종료하면 같은 원본이 DLT에 여러 번 남을 수 있다. 원본 topic·partition·offset으로 같은 격리 건을 식별한다. DLT와 offset을 원자적으로 묶는 Kafka transaction은 이번 범위가 아니다.
- DynamoDB에 별도 실패 복사본이나 DLT 전용 항목을 추가하지 않는다. Kafka 원본·DLT 보관 기간과 적체 용량은 운영 준비 시 함께 정한다.
- DLT retention도 시간에 따라 진행하므로 DLT에 보냈다는 이유로 무기한 유실 방지가 되는 것은 아니다. 격리 건은 해결될 때까지 보관 여유를 감시하고 필요 시 보관 연장·별도 내구성 인계를 수행한다.

## 4. 운영 재처리 절차

현재 자동 DLT replay Consumer나 수동 발행 CLI는 제공하지 않는다. 다음은 도구 구현 시 적용할 절차이며 실제 외부 발송을 재개하기 전에 만료·멱등 상태 판단이 필요하다.

1. 원본 topic·partition·offset으로 대상 목록을 만들고 중복 DLT 기록을 묶는다. 원본·예외·격리 시각·조치자를 남긴다.
2. DB·후속 Kafka 장애이면 원인을 복구한다. 멱등 충돌이면 기존 요청과 충돌 입력을 비교해 분리하고, 충돌 원본을 그대로 자동 재발행하지 않는다. malformed payload도 정상 발송 이벤트로 임의 변환하지 않는다.
3. 원래 인입 시각과 실행·완료 상태를 확인한다. 완료 기록이 없는 것을 미발송 증거로 단정하지 않는다. 업무 기한이 지난 요청은 새 기한으로 재발송하지 않고 만료·운영 확인 경로로 보낸다. 해당 판단 기능 구현 전에는 오래된 DLT의 운영 재발송을 보류한다.
4. 재처리 가능한 요청은 동일 key·eventId·deliveryId·원래 인입 시각·payload로 원본 접수 토픽에 발행한다. 고객 API를 다시 호출해 인입 시각이나 식별자를 새로 만들지 않는다.
5. 재발행 ack를 확인한 뒤 재처리 인계 사실을 기록한다. ack 미확인 시 성공으로 표시하거나 격리 원본을 삭제하지 않는다. 뒤쪽 record만 성공했다고 앞선 미인계 건의 관리 위치를 넘어가지 않는다.
6. 업무 상태 수렴과 추가 격리 여부를 확인한다. DLT 저장 성공·재발행 성공·최종 업무 완료를 별도 상태로 관리한다.

## 5. 검증 범위

단위 시험은 DLT ack 대기, 발행 실패, ack timeout, 재시도 소진, 멱등 충돌의 즉시 격리, 원본 key/value 직렬화를 확인한다.

Kafka 통합 시험은 테스트 전용 UUID 토픽·그룹에서 다음을 확인한다. 실행 방법은 [로컬 개발 환경](06-로컬-개발-환경-실행-방법.md)을 따른다.

1. 충돌 입력을 DLT에 저장하고 뒤의 정상 요청을 처리하며 원본 offset이 이동한다.
2. 잘못된 JSON을 원본 바이트 그대로 DLT에 보존하고 업무 listener에는 넘기지 않는다.
3. DLT 파티션이 없으면 이미 처리한 지점에서 원본 offset이 멈춘다. 테스트가 DLT 파티션을 복구하면 DLT 저장과 뒤쪽 요청 처리가 진행된다.

실제 broker 전체 장애·프로세스 kill·다중 AZ 장애 및 자동 운영 재처리의 검증을 대신하는 시험은 아니다.
