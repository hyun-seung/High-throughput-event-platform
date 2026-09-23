# 로컬 통합 실행

Java 21, Maven, Docker Compose가 필요합니다. 통합 검증 스크립트는 Python 3와 AWS SigV4를 지원하는 curl(7.75 이상)을 사용합니다. 현재 인증은 `auth-module` 라이브러리를 포함한 Delivery API에서 제공합니다. 별도 Auth AP는 실행하지 않습니다.

## 환경과 빌드

```bash
cp .env.example .env
# JAVA_HOME을 Java 21 경로로 설정합니다.
bash scripts/local.sh infra
bash scripts/local.sh init-db
bash scripts/local.sh init-policy
bash scripts/local.sh build
```

이미 `.env`가 있으면 덮어쓰지 말고 예시와 비교해 필요한 항목을 추가합니다. Maven이 PATH에 없으면 `.env`의 `MAVEN_BIN`에 실행 파일 경로를 큰따옴표로 감싸 지정합니다. Maven Wrapper는 아직 제공하지 않습니다.

예시는 기존 서비스와 충돌하지 않도록 Kafka 19092, Redis 16379, PostgreSQL 15432, DynamoDB 18000, API 18080, Provider 18090을 사용합니다. 스크립트는 이 포트를 애플리케이션 연결 설정에 반영합니다. `dev` 프로필로 실행하므로 개인 `application-local.yml`의 오래된 설정을 읽지 않습니다.

DB 초기화는 로컬 `users` 테이블과 `local-user / local-password` 테스트 사용자를 만듭니다. 재실행해도 기존 사용자 정보를 변경하지 않습니다. 이 계정과 예시 JWT secret은 로컬 전용입니다. JWT secret은 디코딩 후 최소 32바이트인 Base64 문자열이어야 합니다.

`init-policy`는 DB에서 `local-user`의 실제 ID를 조회해 Redis에 TPS 100, burst 100, 월 한도 100,000 정책의 누락 필드를 채웁니다. 기존 정책과 사용량은 유지합니다. 정책이 없으면 인증에 성공해도 Delivery 접수는 `POLICY_NOT_FOUND`(503)로 거절됩니다.

DynamoDB는 초기화 컨테이너에서 전용 데이터 볼륨의 소유권을 실행 사용자에게 부여합니다. healthcheck는 서명된 `ListTables` 요청의 성공을 검사하므로 DB 파일에 접근하지 못하는 상태를 정상으로 판단하지 않습니다.

## 애플리케이션 실행

각 명령을 별도 터미널에서 순서대로 실행합니다.

```bash
bash scripts/local.sh run external-api-simulator
bash scripts/local.sh run delivery-ingress-worker
# Ingress의 DynamoDB 테이블 초기화 완료 후 Dispatch를 실행합니다.
bash scripts/local.sh run dispatch-worker
bash scripts/local.sh run event-api
```

실행 JAR는 `build`가 만든 파일입니다. 코드 수정 뒤에는 다시 빌드합니다. 네 AP 모두 시작 완료 로그와 오류 없는 상태를 확인합니다. Worker의 Kafka partition 할당 로그까지 확인해야 consume 준비를 판단할 수 있습니다. 별도 자동 AP readiness endpoint는 아직 없습니다. 아래 smoke 명령은 실행된 앱의 실제 처리 경로를 검증합니다.

## 상태와 인증 확인

```bash
bash scripts/local.sh status
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"username":"local-user","password":"local-password"}' \
  http://localhost:18080/api/v1/auth/token
```

포트를 바꿨으면 요청 URL도 맞춥니다. 네 앱의 준비가 끝나면 다음 명령으로 현재 구현된 처리 경로를 검증합니다.

```bash
bash scripts/local.sh smoke
```

스크립트는 `.env`의 포트를 사용하며 다음을 확인합니다.

- 테스트 사용자 인증 및 토큰 발급
- 동일 멱등키로 두 번 접수했을 때 HTTP 202와 동일한 `deliveryId`
- 최대 30초 동안 DynamoDB를 조회하여 `META`와 단일 `ATTEMPT#...`의 `ACCEPTED`, `provider_processed_at` 확인

실행마다 새 멱등키를 사용하며 로컬 Kafka/DynamoDB에 검증 데이터가 남고 테스트 계정의 요청 한도를 사용합니다. 토큰은 출력하지 않습니다. 기존 정책이 차단 상태이거나 한도가 소진되면 실패합니다. 실패 시 API 및 Worker 로그를 확인합니다.

이 검증은 Kafka와 두 Worker를 거쳐 mock Provider 접수 결과가 저장되는 범위입니다. Kafka 각 레코드의 직접 비교, 장애 복구, Provider 실제 부수 효과 횟수, receipt 이후 최종 전달 완료를 검증하지는 않습니다.

## 종료와 데이터

각 AP 터미널에서 Ctrl+C로 종료한 뒤 `bash scripts/local.sh down`을 실행합니다. named volume은 유지합니다. `down --volumes`는 데이터 삭제이므로 평상시 종료에 사용하지 않습니다.

Compose 프로젝트 이름이 `reliable-event-platform`이므로 기존 `platform` 프로젝트의 DynamoDB volume을 자동으로 공유하지 않습니다. 기존 데이터와 컨테이너는 별도로 보존됩니다.

## 검증 범위

2026-09-23 Java 21에서 전체 9개 모듈 패키징과 테스트 14건을 통과했습니다. 셸 문법 검사도 통과했습니다. DynamoDB 자동 설정 등록 경로와 Worker 전용 활성화를 회귀 테스트로 확인했습니다.

실제 DB 초기화, Redis 정책 초기화, 네 앱 기동, 인증과 smoke 검증을 통과했습니다. 실행 검증에서 발견한 예시 JWT의 Base64 형식 오류, DynamoDB 볼륨 쓰기 권한 및 healthcheck 오류, Ingress JSON 직렬화 중복 설정, API 비동기 응답의 인증 누락을 수정했습니다.

비동기 인증 문제는 디버거에서 최초 요청의 사용자 ID `1`과 이후 `ASYNC` dispatch의 `InsufficientAuthenticationException`을 확인했습니다. JWT 필터가 비동기 dispatch에서도 인증하도록 수정했으며, 정상 202 응답·요청 한도 1회 차감·무인증 요청 거절을 회귀 테스트로 검증했습니다. Ingress 직렬화 테스트는 설정 적용 후 JSON type header가 없고 원시 byte 배열도 보존됨을 확인합니다.

Maven Wrapper와 별도 AP readiness endpoint는 후속 작업입니다. 장애·재시도·복구 시나리오는 별도 통합 테스트가 필요합니다.
