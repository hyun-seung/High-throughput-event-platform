# DLT 조회와 재처리 대상 분류

2026-09-25. DLT 후속 작업의 첫 단계로 **Java 읽기 전용 조회·분류 CLI**를 추가했다. 후속 [원장 대조와 기존 실행 복구](50-DLT-원장-대조와-기존-실행-복구.md)에서 ORIGIN이 남은 요청의 재처리·만료 인계와 SQL 조치 기록을 추가했다. 조회 성공이나 REQUIRES_STATE_CHECK 분류를 재발송 승인으로 해석하지 않는다.

## 1. 실행

```sh
# JDK 21에서 전체 package 또는 Java 통합 검증을 완료한 뒤
bash scripts/inspect-dlt.sh \
  localhost:9092 delivery.requested.dlt.v1 0 0 20 delivery.requested.v1 PT3H
```

인수는 broker, DLT 토픽, partition, 시작 offset, 최대 건수(1~100), 예상 원본 토픽, 1차 TTL이다. 원본 토픽·TTL을 생략해 임의 기본값을 적용하지 않는다. 이번 CLI는 로컬 단일 broker 주소만 허용한다. 운영 SASL/TLS·권한·감사 연결은 아직 제공하지 않는다.

외부 도구는 **Bash 실행 래퍼**이며, 조회·JSON 검증·분류는 `delivery-ingress-worker` JAR의 Java `DltInspector`/`DltInspection`이 수행한다. Spring 앱 전체나 발송 worker를 띄우지 않는다. Kafka 진단 로그는 stderr, 조회 결과 JSON은 stdout이다. 필요한 경우 출력 파일 접근 권한을 제한한다.

## 2. 조회가 바꾸지 않는 것

KafkaConsumer의 assign/seek로 지정 파티션만 읽는다. group.id·subscribe·commit·producer가 없고, enable.auto.commit=false·allow.auto.create.topics=false다. 기존 업무 group의 할당·commit 위치를 바꾸지 않는다. [KafkaConsumer 공식 문서](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)는 수동 할당과 consumer group 조정, 현재 읽기 위치와 committed 위치를 구분한다.

조회 시작 시 beginning/end offset을 고정한다. 보존 범위 밖의 시작 offset은 자동 보정하지 않고 실패한다. 결과의 nextOffset으로 다음 페이지를 조회한다. 최대 건수로 잘린 페이지에는 reachedSnapshotEnd=false가 표시된다. offset은 연속 숫자일 필요가 없으며 조회 도중 새로 들어온 레코드는 다음 조회의 snapshot에 포함한다. 각 Kafka API 대기와 페이지 polling에 제한이 있고, timeout 시 완성된 페이지인 것처럼 성공 출력하지 않는다.

조회는 DLT 삭제·재발행·원본 변경·DynamoDB 쓰기를 수행하지 않는다. 따라서 DLT의 보관 기간을 늘려주거나 유실 위험을 해소한 작업도 아니다.

## 3. 분류

[Spring Kafka DLT header](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)의 원본 topic/partition/offset을 검증해 source로 출력한다. 같은 source를 가진 여러 DLT 레코드는 동일 격리 원본의 중복 후보로 식별할 수 있다. 자동 병합·삭제는 하지 않는다.

| 분류 | 의미·다음 처리 |
|---|---|
| INVALID_SOURCE | 원본 header 누락·중복·길이 오류·다른 source topic. 임의의 원본 좌표로 재발행하지 않음 |
| MALFORMED_EVENT | 파싱 오류, null/과대 본문, key/ID 불일치, 필수 필드 누락, 미래 인입 시각 등. 정상 업무 이벤트로 보정하지 않음 |
| UNSUPPORTED_EVENT | 현행 v2 신규 접수 외 schema/type/eventId. 별도 확인 |
| IDEMPOTENCY_CONFLICT | 직접/원인 예외 header에 기존 멱등 충돌 예외가 있음. 기한이 지나도 충돌 원본을 일반 만료로 처리하지 않음 |
| EXPIRED_REQUIRES_FINALIZATION | 원래 occurredAt + 지정 1차 TTL에 도달. 기존 상태·허용 대체 발송·원래 2차 기한을 확인할 후속 경로 필요 |
| REQUIRES_STATE_CHECK | 형식과 계산상 1차 기한 확인을 통과했음. 실제 ORIGIN/STEP·SQL 이력·완료 캐시 확인이 아직 필요 |

현재 기한은 **명시한 설정으로 계산한 1차 기한**이다. 과거 설정이 달랐거나 이미 단계가 진행된 경우의 저장된 실제 기한을 대체하지 않는다. SQL 이력·완료 표식 부재만으로 미발송을 추론하지 않는다. 만료 또는 충돌 건을 새 시각/ID로 바꿔 고객 API에 다시 넣는 경로는 제공하지 않는다.

응답에는 DLT 좌표, 검증된 원본 좌표, 요청 ID·고객 ID·인입 시각·계산 기한·분류, 원본 value SHA-256·크기를 담는다. payload·원본 key·예외 메시지/stacktrace·원문 파서 오류는 출력하지 않는다. 본문 1 MiB 초과는 분류에서 거부하지만 Kafka가 큰 첫 record batch를 가져올 수 있으므로 이를 절대 메모리 상한으로 표현하지 않는다.

## 4. 검증과 남은 범위

Java 단위 테스트는 기한 경계, 원래 인입 시각 유지, 충돌 우선 보류, malformed/null/뒤에 붙은 JSON, 출처 header 검증, v1/미래 시각/키 불일치, 비로컬 broker·과대 페이지 거부를 확인한다. 실제 Kafka 테스트는 독립 UUID 토픽에 3건을 넣고 두 페이지로 읽어 원문 보존·같은 조회 결과·기존 group commit 유지·끝 offset 유지·범위 밖 조회 거부를 확인한다.

다음 구현은 저장소 상태·이력 대조, 원래 ID/시각을 보존하는 재처리 또는 만료 인계, 인계 ack와 내구성 조치 기록이다. 현재 CLI에는 쓰기 명령이 없으며 그 기능들이 완료됐다고 표시하지 않는다.


## 5. 실행 결과

격리 실행 `java-test-20260925141232-17311`에서 **Java/JUnit 310개 실행, 미실행 0, 실패/오류 0**을 통과했다. 분류 단위 8개·실제 Kafka 1개가 새로 포함됐다. [집계·도구·소스 해시·패키지 실행 근거](검증-결과/2026-09-25-Java-DLT-조회와-310개-통합-검증.json).

패키징된 JAR를 Bash 래퍼로 실행해 잘못된 인수의 exit 2와 실제 Kafka 빈 페이지 JSON도 확인했다. 조회용 전용 토픽은 삭제하고 소유한 테스트 broker는 다시 정지했다. 업무 Kafka group/토픽을 변경하거나 실제 메시지를 재발송하지 않았다.

표준 unit 실행도 별도로 확인했다. Java 156개 실행·외부 저장소 조건 142개 미실행·실패/오류 0이다. 이를 통합 310개에 더해 고유 테스트 수로 세지 않는다.
