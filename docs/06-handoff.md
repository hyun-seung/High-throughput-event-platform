# 작업 인수인계

작성 기준: 2026-09-23, `main` 브랜치 `2c629dd`.

## 1. 저장소 상태

- 원격 저장소: `https://github.com/hyun-seung/High-throughput-event-platform.git`
- 기준 브랜치: `main`
- 마지막 구현 커밋: `2c629dd Dispatch Attempt 멱등 처리 추가`
- 전체 Maven reactor: 9개 모듈
- 자동 테스트: 9건 통과
- Java: 21
- Maven Wrapper: 아직 없음
- Docker Compose: DynamoDB Local만 구성됨

다른 환경에서 시작할 때 다음 명령으로 기준 상태를 확인합니다.

```bash
git switch main
git pull --ff-only origin main
git status --short
git log --oneline -5
```

`git status --short` 출력이 없어야 합니다.

## 2. 현재 구현된 흐름

```text
Client
→ JWT 인증
→ Redis TPS/Quota 확인
→ deliveryId/eventId 생성
→ delivery.requested.v1 발행과 broker ack
→ 202 Accepted
→ Delivery Ingress Worker
→ DynamoDB DELIVERY#id / META conditional insert
→ delivery.dispatch-requested.v1 발행
→ Dispatch Worker
→ DynamoDB ATTEMPT#id claim + lease + version
→ Mock Provider 호출
→ DynamoDB Attempt ACCEPTED 기록
```

주요 보장은 다음과 같습니다.

- API는 Kafka broker ack 이후에만 `202`를 반환합니다.
- 같은 tenant와 Client Idempotency-Key는 같은 `deliveryId`와 최초 `eventId`를 만듭니다.
- Delivery 원본은 DynamoDB conditional insert 한 건으로 수렴합니다.
- Dispatch Attempt는 안정적인 `attemptId`로 점유합니다.
- 완료된 Attempt 재전달은 Provider를 다시 호출하지 않습니다.
- lease가 만료된 Attempt는 version을 증가시켜 같은 attemptId로 다시 점유합니다.
- Provider가 idempotency key를 지원할 때 재호출의 외부 부수 효과가 한 번으로 수렴합니다.

자세한 현재 상태는 [현재 구현 분석과 Gap](01-current-state.md), 구간별 동작은 [구간별 상세 처리 흐름](05-stage-by-stage-flow.md)을 기준으로 합니다.

## 3. 최근 커밋

```text
2c629dd Dispatch Attempt 멱등 처리 추가
00ca95e Ingress와 Dispatch 실행 모듈 분리
3cf0c50 Delivery 플랫폼 설계 문서 정리
ba5e2f0 로컬 Delivery 실행 설정 정리
81796fc Delivery 식별자와 Kafka 접수 테스트 추가
18b707b Kafka 기반 Delivery 처리 구조로 전환
```

커밋 메시지는 `feat:` 같은 접두사 없이 짧은 한글 문장으로 작성합니다. 한 커밋에는 검토와 되돌리기가 가능한 하나의 작업 단위만 넣습니다.

## 4. 다음 작업 순서

### 커밋 1: 로컬 인프라 Compose 구성

Kafka, Redis, PostgreSQL, DynamoDB Local을 Compose에 추가하고 healthcheck와 named volume을 설정합니다.

완료 조건:

- `docker compose up -d` 후 모든 인프라가 healthy입니다.
- Kafka는 외부 애플리케이션에서 `localhost:9092`로 접근할 수 있습니다.
- Redis는 `localhost:6379`, PostgreSQL은 `localhost:5432`, DynamoDB는 `localhost:8000`에서 접근할 수 있습니다.
- `docker compose config`가 통과합니다.

권장 커밋 메시지:

```text
로컬 인프라 Compose 구성
```

### 커밋 2: 로컬 통합 실행 절차 추가

Maven Wrapper, 공통 환경변수 예시, 애플리케이션 실행 순서, readiness 확인과 종료 절차를 추가합니다.

실행 대상:

```text
MockDeliveryProviderApplication
DeliveryIngressWorkerApplication
DispatchWorkerApplication
DeliveryApiApplication
```

인증 토큰 발급 애플리케이션과 Delivery API는 현재 기본 포트가 모두 `8080`이므로, 함께 실행할 때 포트를 분리하거나 테스트 토큰 준비 절차를 명확히 해야 합니다.

권장 커밋 메시지:

```text
로컬 통합 실행 절차 추가
```

### 커밋 3: 기본 E2E 흐름 검증 추가

한 건의 요청이 두 Kafka topic, DynamoDB Delivery/Attempt 항목과 Mock Provider까지 도달하는지 자동 검증합니다.

완료 조건:

- API 응답이 `202 Accepted`와 `deliveryId`를 반환합니다.
- `DELIVERY#id / META`가 한 건 존재합니다.
- `DELIVERY#id / ATTEMPT#id` 상태가 `ACCEPTED`입니다.
- 동일 요청과 동일 Dispatch event를 반복해도 Delivery와 Attempt가 하나로 수렴합니다.
- Mock Provider의 실제 부수 효과가 한 번입니다.

권장 커밋 메시지:

```text
기본 Delivery 흐름 검증 추가
```

기본 E2E는 위 세 커밋 완료 시점에 실행할 수 있습니다. 이후 Provider 결과 분류와 crash 테스트를 진행합니다.

## 5. 그다음 신뢰성 작업

1. HTTP 결과를 `ACCEPTED`, `RETRYABLE`, `PERMANENT`, `UNKNOWN`으로 분류합니다.
2. timeout과 connection reset을 Attempt `UNKNOWN`으로 저장합니다.
3. Provider idempotency key 지원/미지원 mode를 Mock Provider에 추가합니다.
4. Provider 성공 직후 Worker 종료와 offset commit 전 종료를 자동화합니다.
5. Kafka Consumer error handler, DLT와 replay 절차를 추가합니다.
6. Retry, Recovery, Fallback, Receipt 순서로 확장합니다.

`UNKNOWN`에서는 Primary Provider가 이미 처리했을 수 있으므로 자동 Fallback을 수행하지 않는 설계를 유지합니다.

## 6. 빌드와 테스트

현재 로컬에는 Maven Wrapper가 없으므로 IntelliJ에 포함된 Maven을 사용해 검증했습니다.

```bash
JAVA_HOME=/Users/hyunseung/Library/Java/JavaVirtualMachines/temurin-21.0.7/Contents/Home \
'/Applications/IntelliJ IDEA.app/Contents/plugins/maven-plugin/lib/maven3/bin/mvn' test
```

마지막 검증 결과:

```text
Reactor modules: 9
Tests: 9
Failures: 0
Errors: 0
BUILD SUCCESS
```

새 환경에서는 Java 21과 Maven 3.9 이상을 준비한 뒤 다음 명령을 사용할 수 있습니다.

```bash
mvn test
```

## 7. 현재 알려진 제한

- Compose에는 DynamoDB Local만 있으며 Kafka와 Redis 없이는 전체 흐름을 실행할 수 없습니다.
- PostgreSQL은 목표 구조에는 포함되지만 현재 Delivery 처리 코드에서 사용하지 않습니다.
- Provider 호출 예외는 아직 세부 분류하지 않아 Attempt가 lease 만료 전까지 `PROCESSING`에 남습니다.
- 유효한 Attempt lease에서 record가 재전달되면 listener가 실패합니다. 지연 재처리와 error handler는 아직 없습니다.
- DynamoDB claim/lease는 단위 테스트만 있으며 DynamoDB Local 경쟁 통합 테스트가 남았습니다.
- Mock Provider는 idempotency key 지원 mode만 구현했습니다.
- API의 Kafka publish timeout/실패를 명시적인 `503` 계약으로 변환하는 작업이 남았습니다.
- 상태 조회 API, Retry, Recovery, Fallback, Receipt와 최종화는 아직 구현하지 않았습니다.

## 8. 새 Codex 계정에서 이어가기

Codex 인증과 GitHub 인증은 별개입니다. `codex logout`은 Codex 저장 인증만 제거하며 Git remote와 GitHub 자격 증명을 변경하지 않습니다.

새 계정으로 Codex에 로그인합니다.

```bash
codex login
codex login status
```

저장소를 연 뒤 다음 요청으로 작업을 이어갈 수 있습니다.

```text
docs/06-handoff.md와 docs/02-roadmap.md를 읽고,
다음 커밋 단위인 "로컬 인프라 Compose 구성"부터 구현·검증·커밋해줘.
커밋 메시지는 접두사 없이 짧은 한글로 작성해줘.
```

GitHub push 계정을 바꿔야 하는 경우에는 Codex 로그인과 별도로 Git credential 또는 `gh auth login`을 변경해야 합니다.
