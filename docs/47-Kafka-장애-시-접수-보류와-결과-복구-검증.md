# Kafka 장애 시 접수 보류와 결과 복구 검증

2026-09-25. `scripts/poc/kafka_recovery.py`는 고유 Compose 프로젝트와 6개 실제 JVM을 띄우고 **소유한 Kafka 컨테이너만** 두 번 중단·재기동한다. PostgreSQL·DynamoDB·Redis는 유지한다. Kafka producer mock이나 앱 재시작으로 대체하지 않는다.

## 1. 검증할 경계

| ID | 장애 중 기대 동작 | 복구 후 판정 |
|---|---|---|
| K-01 신규 접수 | Kafka ack를 확인하지 못하면 503/9003, ORIGIN·고객 결과 생성 없음 | 동일 ClientMsgId로 다시 접수, 고유 이력 1건·HTTP 효과 1회 |
| K-02 업체 웹훅 | 미리 접수된 STEP에 대해 성공 웹훅을 보내도 Kafka 중단이면 503/RECEIPT_UNCONFIRMED, STEP은 ACCEPTED 유지 | 동일 receiptId·본문 재전송 후 202, DELIVERED 이력·고객 통지·정리 |
| K-03 최종 결과 인계 | 업체 접수 후 Kafka를 만료 기한까지 중단. STEP에 EXPIRED/result_event를 남기고 발행 실패 후 보존, SQL/고객/완료 Redis 표식은 아직 없음 | 같은 볼륨 복구 후 저장해 둔 동일 결과를 SQL·고객에게 전달하고 DDB 정리, 외부 재발송 없음 |

시험은 1차 60초·2차 80초, DDB 누락 복구 조회 3초다. API/웹훅 producer의 max.block/request/delivery timeout은 2/1/3초로 축소한다. 최종 결과 publisher는 기존 5/5/10초 설정을 유지한다. 운영 만료 3/4시간, DDB 조회 10분, API producer 기본값은 바꾸지 않는다. 시뮬레이터 외부 중복 제거는 끈다.

여섯 JVM PID 유지, 모든 결과의 SQL/실제 204 고객 본문 일치, ORIGIN·STEP 삭제, Redis 완료 TTL, 업체 호출/효과 각 1회, 모든 업무 토픽 lag 0을 대조한다. 중단 전·최종 offset/watermark가 후퇴하지 않는지도 확인한다. broker 중단 중에는 broker에서 offset을 읽을 수 없으므로 그 시점의 offset을 관측했다고 주장하지 않는다. offset 조회 도구는 subscribe/commit하지 않는다.

## 2. ack와 멱등성 해석

[Kafka producer 공식 문서](https://kafka.apache.org/40/configuration/producer-configs/)의 acks=all과 idempotence는 Kafka 인계의 근거다. HTTP 503은 일반적으로 저장 안 됨을 확정하는 응답이 아니다. ack가 유실된 경우에도 발생할 수 있으므로 같은 ID·본문을 재전송해야 한다. 이번 시험은 broker를 먼저 완전히 중단한 연결 실패이며, broker 저장 직후 ack만 유실하는 네트워크 절단 시험과 구분한다.

외부 HTTP/TCP의 업무 멱등성은 DynamoDB 선점과 기존 상태 판단이 맡는다. Kafka 재연결이 외부 업체 재발송의 근거가 되지 않는다. 기한 중 결과 인계를 완료할 수 없는 장기 장애에는 합의대로 복구 후 원래 기한의 만료 결과를 전달한다.

## 3. 시험 중 발견한 Kafka 데이터 마운트

첫 실행은 Docker inspect가 반환한 볼륨 목록의 **순서**가 바뀌어 동일성 검증이 실패했다. 마운트 경로→볼륨 이름으로 비교하도록 수정했다. 이것을 Kafka 데이터 유실이나 서비스 복구 성공으로 기록하지 않는다.

같은 점검에서 기존 Compose의 `kafka-data:/var/lib/kafka` 아래 실제 로그 경로 `/var/lib/kafka/data`에는 이미지가 만든 **별도 익명 볼륨**이 있음을 확인했다. 같은 컨테이너의 stop/start에는 남지만 컨테이너 재생성 때 명명 볼륨만 재연결하는 것으로 실제 로그를 보존한다고 믿을 수 없다. [Docker 공식 볼륨 설명](https://docs.docker.com/engine/storage/volumes/)도 익명 볼륨의 수명과 재사용을 명명 볼륨과 구분한다.

신규 환경의 마운트를 `kafka-data:/var/lib/kafka/data`로 수정했다. 시험은 이 경로의 실제 볼륨 이름까지 확인한다. **기존 환경의 데이터는 자동 이관하지 않았다.** `local.sh infra`·`monitoring.sh up`·고정 platform-poc 환경의 `scripts/poc/run.py`는 기존 컨테이너의 부모 경로 마운트를 발견하면 데이터 변경 없이 중단한다. 직접 docker compose up을 사용하는 경우에도 아래 이관 확인이 먼저다.

기존 데이터를 유지할 환경의 이관 순서:

1. 기존 컨테이너의 inspect에서 `/var/lib/kafka/data`의 실제 Source/Name을 보존한다. 부모 `kafka-data` 이름만 백업 대상으로 삼지 않는다.
2. 쓰기와 broker를 정상 중단하고 실제 로그 볼륨을 별도 백업한다. `meta.properties`, KRaft 메타데이터와 토픽·offset 데이터를 함께 보존한다.
3. **새 별도 볼륨**에 오프라인 복사한 뒤 권한·cluster/node ID·메타데이터를 검증하고 새 data 경로에 연결한다. 빈 디렉터리를 새 클러스터로 포맷해서 기존 데이터와 합치지 않는다.
4. 복구 기동 후 topic/partition·watermark·consumer offset과 표본 메시지까지 비교한 다음 트래픽을 재개한다. 대조가 끝나기 전 기존 컨테이너/익명 볼륨·백업을 삭제하지 않는다.

이번 자동 시험은 신규 격리 프로젝트에서의 동일 볼륨 재기동이다. 기존 로컬/모니터링 데이터의 위 이관이나 컨테이너 재생성 복구를 실행했다고 표현하지 않는다.

## 4. 실행과 범위

```sh
# JAVA_HOME을 JDK 21로 지정하고 기존 도구를 준비한다.
./mvnw package
.poc-tools/venv/bin/python -m unittest discover -s scripts/poc -p 'test_*.py'
.poc-tools/venv/bin/python scripts/poc/kafka_recovery.py
```

실행별 `.poc-results/`에 환경·도구/JAR 해시, 중단/복구 상태·마운트, 503 응답, 중단 중 원장·결과·발행 실패 계측, SQL/고객 결과·offset·정리를 남긴다. 인증 값이나 앱 실행 인수는 근거 파일에 저장하지 않는다. 종료 시 소유 앱·컨테이너만 정지하고 볼륨은 유지한다.

로컬 broker 1개·replication factor 1인 시험이다. 다중 broker ISR 변화·리더 선출·복제본 유실·디스크 파손·리전/AZ 장애·장기간 보존 한계·부하 성능은 검증하지 않는다. 기존 DynamoDB/Redis/SQL 장애 시험과 합쳐도 운영의 모든 무손실 조건이 증명된 것은 아니다.

## 5. 실행 결과

실행 `full-flow-4c6903717d09`의 **3개 시나리오 모두 통과**. [환경·중단/복구·응답·DB·고객·offset 원문](검증-결과/2026-09-25-Kafka-장애-접수-보류와-결과-복구.json)에 성공 실행과 첫 실행의 실패 원인을 함께 보관했다.

| 관측 | 결과 |
|---|---|
| 중단 중 접수/웹훅 | 각각 503/9003, 503/RECEIPT_UNCONFIRMED |
| 같은 ID 재전송 | 웹훅 동일 receiptId·본문과 신규 접수 동일 ClientMsgId를 복구 후 처리 |
| 중단 중 만료 | STEP/result_event 보존·발행 실패 증가, 기존 이력/고객 건수 유지, 완료 Redis TTL 없음 |
| 최종 대조 | SQL 이력 3건(DELIVERED 2, EXPIRED 1), 실제 고객 수신 일치, 각 ORIGIN·STEP 삭제 |
| 외부 호출/효과 | HTTP 각각 총 3회, TCP 0회. 요청당 1회 유지 |
| Kafka 최종 결과 | finalized 레코드 4건 → 고유 이력 3건. 재발행을 소비 측에서 중복 제거함 |
| 복구 | 여섯 JVM PID 유지, Kafka 컨테이너 ID·실제 데이터 명명 볼륨 유지, 업무 4개 토픽 전 파티션 lag 0 |
| 도구 검증 | Python 50개 통과, shell 문법 통과. 읽기 전용 마운트 보호 검사: 이전 구조 차단/신규 구조 허용 확인 |

Java 코드는 앞선 연결 풀 커밋의 전체 기능 6개·실행 테스트 148개를 통과한 동일 JAR를 사용했다. 이번 작업은 장애 도구·Compose·기동 보호 검사·문서 변경이며 Maven 전체를 다시 돌린 결과로 중복 집계하지 않는다.

다음은 DLT 조회·재처리와 원래 기한을 지키는 만료 인계다. 운영 상태 조회/조치, 장애 중 복구 조회 backoff, 배포·통합 회귀도 남아 있으며 성능 측정은 이 개발을 마친 뒤 진행한다.
