# 구현 로드맵과 완료 조건

## 진행 원칙

- Kafka ack를 최초 접수의 내구성 근거로 사용합니다.
- 내부 계산에는 상태 write를 추가하지 않고 외부 부수 효과와 복구 경계에 저장합니다.
- 모든 단계는 정상 경로, 중복 event, 프로세스 종료와 의존성 장애를 검증합니다.
- 다음 Phase는 이전 Phase의 자동 검증과 장애 검증이 통과한 뒤 시작합니다.

## 현재 기준선

완료된 항목:

- Delivery 도메인 용어와 `deliveryId`, `eventId` 분리
- Client `Idempotency-Key` 기반 안정적인 Delivery ID
- Kafka-first API 접수와 ack 후 `202 Accepted`
- `delivery.requested.v1`과 `delivery.dispatch-requested.v1` 분리
- DynamoDB `DELIVERY#id / META` 단일 conditional insert
- Producer `acks=all`, idempotence, zstd 설정
- Provider HTTP timeout과 Idempotency-Key 전달
- Idempotency-Key를 지원하는 Mock Provider의 중복 응답 수렴
- Dispatch Attempt conditional claim, lease, version과 완료 skip
- DynamoDB Local named volume 영속화

## Phase 0. 재현 가능한 로컬 환경

**목표:** clean checkout에서 한 명령으로 전체 최소 flow를 실행합니다.

작업:

1. Maven wrapper와 Java 21 기준을 추가합니다.
2. Compose에 Kafka, Redis, PostgreSQL, DynamoDB와 healthcheck를 추가합니다.
3. 환경변수 예시와 실행/종료/초기화 절차를 작성합니다.
4. Kafka topic 설정을 환경별로 분리합니다.

완료 조건:

- 전체 Maven test가 통과합니다.
- API 요청 한 건이 두 Kafka topic, DynamoDB와 Mock Provider까지 도달합니다.
- 서비스 readiness를 자동 확인할 수 있습니다.

## Phase 1. Ingress 신뢰성 검증

**목표:** Kafka-first 접수와 DynamoDB projection의 중복/종료 경계를 검증합니다.

작업:

1. Kafka publish 실패와 timeout을 명시적 API 오류로 변환합니다.
2. DeliveryRequested 중복 전달 통합 테스트를 작성합니다.
3. DynamoDB 저장 직후와 Dispatch 발행 직후 종료를 주입합니다.
4. Consumer error handler, DLT와 replay 절차를 추가합니다.

완료 조건:

- Kafka ack 없는 요청에는 `202`를 반환하지 않습니다.
- 같은 Client 요청 N회가 하나의 DynamoDB 원본으로 수렴합니다.
- 같은 Idempotency-Key와 다른 payload는 충돌로 격리됩니다.
- crash 후 Dispatch event가 누락되지 않습니다.

## Phase 2. Dispatch Attempt와 Sender 멱등성

**목표:** Kafka 재전달이 외부 호출을 불필요하게 중복시키지 않습니다.

작업:

1. `DISPATCH_ATTEMPT` item과 상태 모델을 구현합니다.
2. conditional claim, lease와 version을 구현합니다.
3. Provider idempotency key 지원/미지원 mode를 구현합니다.
4. 호출 성공 직후 강제 종료 테스트를 자동화합니다.
5. HTTP 결과를 accepted, retryable, permanent, unknown으로 분류합니다.

완료 조건:

- 동일 DispatchRequested N회에도 key 지원 Provider의 실제 부수 효과가 한 번입니다.
- 완료된 Attempt는 Provider를 재호출하지 않습니다.
- 만료된 PROCESSING lease는 안전한 조정 경로로 이동합니다.
- timeout/reset은 성공이나 실패로 임의 단정하지 않고 `UNKNOWN`으로 남습니다.

## Phase 3. Retry와 UNKNOWN 조정

**목표:** 확정된 일시 실패와 처리 여부 불명을 서로 다르게 다룹니다.

작업:

1. backoff, jitter, 최대 attempt와 전체 deadline을 구현합니다.
2. `nextAttemptAt` 스케줄 방식과 조회 인덱스를 확정합니다.
3. Provider 상태 조회, Receipt 대기와 운영 검토 절차를 구현합니다.
4. DLT/DEAD 조회와 수동 replay 감사 이력을 추가합니다.

완료 조건:

- 일시 실패가 지정 시각 이후 재처리됩니다.
- 영구 실패는 불필요하게 재시도하지 않습니다.
- 무한 Retry가 없습니다.
- `UNKNOWN`의 사유와 최종 조정 결과를 조회할 수 있습니다.

## Phase 4. Provider Fallback과 장애 격리

**목표:** Primary 장애를 다른 Delivery와 Provider로 확산시키지 않고 대체 경로를 사용합니다.

작업:

1. tenant, delivery type과 destination별 routing 정책을 구현합니다.
2. Provider별 timeout, connection pool, concurrency limit와 circuit breaker를 적용합니다.
3. routeOrder와 Provider별 attempt를 기록합니다.
4. fallback 허용 오류와 전체 deadline을 적용합니다.
5. `UNKNOWN`에서는 자동 fallback을 금지합니다.

완료 조건:

- Primary 확정 실패나 circuit open에서 Secondary로 전환됩니다.
- 느린 Provider가 API와 다른 Provider 처리량을 소진하지 않습니다.
- Primary가 `UNKNOWN`이면 두 Provider 중복 전달이 발생하지 않습니다.
- 모든 경로 소진 시 전체 이력과 `DEAD` 사유가 남습니다.

## Phase 5. Receipt와 결과 수렴

**목표:** 비동기 결과와 순서 역전에도 최종 상태가 일관됩니다.

작업:

1. Receipt API의 인증/서명 검증과 Kafka-first 응답을 구현합니다.
2. `delivery.receipt-received.v1`과 Result Processor를 구현합니다.
3. providerEventId dedupe를 구현합니다.
4. HTTP 응답보다 Receipt를 먼저 보내는 Provider mode를 만듭니다.
5. terminal 상태 후 `delivery.finalized.v1` 발행 보장을 구현합니다.

완료 조건:

- Receipt 재전송 N회에도 최종 상태와 논리적 후속 event가 하나입니다.
- Receipt/HTTP 순서가 바뀌어도 같은 terminal 상태로 수렴합니다.
- Receiver 응답 시간은 downstream 처리 시간과 분리됩니다.

## Phase 6. 독립 후속 처리와 PostgreSQL 이력

**목표:** 최종 결과를 기능별로 독립 소비합니다.

작업:

1. History, Usage, Client Callback, Statistics를 별도 Consumer Group으로 구성합니다.
2. PostgreSQL batch insert/update와 idempotent key를 구현합니다.
3. 한 Consumer를 중지해 다른 Group의 진행 여부를 검증합니다.

완료 조건:

- 한 Group의 장애가 다른 Group offset에 영향을 주지 않습니다.
- PostgreSQL 재처리에도 중복 이력이 생기지 않습니다.
- backlog replay 절차가 검증됩니다.

## Phase 7. Redis Admission과 Usage 정합성

**목표:** 빠른 유입 제어와 내구성 있는 사용량 계산을 분리합니다.

작업:

1. token bucket key와 월 quota key에 TTL을 적용합니다.
2. tenant 등급별 Redis fail-open/fail-closed 정책을 확정합니다.
3. Kafka accepted event 기반 usage 집계와 Redis 보정 절차를 구현합니다.
4. fixed/sliding window를 비교 실험합니다.

완료 조건:

- 동시 요청에서 admission 한도가 원자적으로 적용됩니다.
- Kafka 접수 실패가 확정 사용량으로 남지 않습니다.
- Redis 중단 시 문서화된 정책대로 동작합니다.

## Phase 8. 관측성

**목표:** 장애와 병목을 dashboard에서 진단합니다.

작업:

1. Actuator/Micrometer, Prometheus와 Grafana를 구성합니다.
2. 구조화 로그, trace propagation, Loki/Fluent Bit를 구성합니다.
3. lag, oldest age, retry/recovery/unknown과 end-to-end p99를 표시합니다.
4. alert와 runbook을 작성합니다.

완료 조건:

- 한 deliveryId의 전 구간 로그를 찾을 수 있습니다.
- 의도한 장애에서 대응 경보가 발생합니다.
- accepted, terminal, known in-flight, unknown, dead의 보존 관계를 확인합니다.

## Phase 9. Batch와 성능 기준선

**목표:** 전달 보장을 유지하며 처리량을 단계적으로 올립니다.

작업:

1. k6 시나리오와 결과 저장 형식을 고정합니다.
2. 100/1k/5k/10k TPS를 순서대로 실행합니다.
3. Producer batch/linger/compression, Consumer poll/fetch와 DB batch를 한 변수씩 실험합니다.
4. Provider capacity에 맞춰 concurrency와 backpressure를 조정합니다.

완료 조건:

- 환경, commit, 설정, 입력과 결과를 함께 보존합니다.
- 각 부하에서 유실/중복, p95/p99와 lag 회복 시간을 기록합니다.
- 병목 근거와 다음 scale 대상을 설명합니다.

## Phase 10. Kubernetes와 KEDA

**목표:** partition과 lag를 기준으로 안전하게 scale-out합니다.

kind/k3d, Helm, graceful shutdown, KEDA lag scaling과 ArgoCD를 순서대로 적용합니다. replica 상한은 활성 partition 수와 Provider concurrency를 함께 고려합니다.

## Phase 11. Fault Injection

**목표:** Kafka, Redis, PostgreSQL, DynamoDB, Provider와 Consumer 장애에서 보존과 수렴을 증명합니다.

[장애 시나리오와 검증표](03-failure-test-matrix.md)를 자동화하고 accepted Delivery의 위치와 복구 결과를 기록합니다.

## Phase 12. CDC

**목표:** 핵심 Delivery flow와 분리된 기준정보 동기화를 검증합니다.

PostgreSQL logical replication → Debezium → Kafka Connect → Consumer → Redis를 구성하고 connector 재시작, schema change와 중복 event를 검증합니다.
