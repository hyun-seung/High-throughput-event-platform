# DynamoDB 장애 시 발송 차단과 만료 복구 검증

2026-09-25. [PostgreSQL 중단·자동 복구](43-PostgreSQL-장애-중-결과-보존과-자동-복구-검증.md)에 이어 실행 원장인 DynamoDB가 불가용할 때 발송·만료 경계를 검증한다. 성능 튜닝·Pod당 최대 TPS 측정은 [개발 완료 후 순서](41-전체-만-TPS-목표와-Pod별-성능-검증-계획.md)를 유지한다.

## 1. 시험 범위와 주입 방법

`scripts/poc/dynamo_recovery.py`는 실행별 Compose·포트·볼륨을 갖는 격리 환경에서 DynamoDB Local 컨테이너만 두 차례 stop/start한다. project/service label과 컨테이너 ID를 확인하며 동일 컨테이너·볼륨으로 복구한다. Kafka offset·DB 내용을 시험 도구로 수정하지 않는다.

첫 시나리오의 선점 전 경계를 맞추기 위해 도구가 생성한 dispatch JVM의 `Popen`에만 SIGSTOP을 보내고 OS 상태 `T`를 확인한다. 그동안 API·Ingress가 ORIGIN과 Kafka 발송 명령을 만든다. STEP 부재와 미완료 Kafka 입력을 확인하고 DB를 중단한 뒤 SIGCONT로 worker를 계속 실행한다. **프로세스를 정지해 놓은 상태의 무발송을 합격 근거로 삼지 않는다.** 재개한 worker의 선점 실패 계측 2회 이상 증가와 외부 호출 0회를 함께 확인한다. 실패 시에도 종료 처리 전에 정지한 worker를 재개한다.

| ID | 장애 전 경계 | DB 중단 중 판정 | 복구 후 판정 |
|---|---|---|---|
| D-01 | ORIGIN 저장, STEP 없음, Kafka 발송 명령 미완료 | worker 재개 후 선점 실패 증가, Kafka committed 위치 불변·lag >0, 업체 호출/효과 0회 | 앱 재시작 없이 최초 인입 시각 유지, 업체 호출/효과 각 1회, 정상 결과·고객 수신·정리 |
| D-02 | 업체 HTTP 접수·효과 각 1회, STEP ACCEPTED, 웹훅 없음 | 원래 deadline 경과까지 DB 중단. 만료 처리 실패 증가, 신규 이력·고객 결과·Redis 완료 표식 없음, 업체 호출/효과 1회 유지 | 최초 STEP deadline으로 EXPIRED 확정, 고객 통지·정리, 업체 재발송 없음 |

두 요청 모두 2차 발송 비허용이며 업체 중복 제거를 끈다. 시험에서만 1차 만료 90초·2차 40초·DDB 누락 복구 3초를 적용한다. 운영 기본값 3시간/4시간·10분과 AWS SDK 설정은 변경하지 않는다. 도구의 연결 불가 탐침만 1초 timeout·재시도 0으로 설정한다.

## 2. 완료 판정과 근거

- DB 불가용은 컨테이너 Exited, 도구의 ListTables 연결 실패, 앱의 실제 실패 계측으로 확인한다.
- 선점 실패는 `delivery_stage_duration_seconds_count{stage="dispatch_claim",result="failure"}`, 만료 실패는 `delivery_lifecycle_events_total{outcome="work_failed"}`를 사용한다. DynamoDB SDK 호출 실패 계측도 함께 저장한다.
- SQL 이력과 고객 수신은 **대상 requestKey 기준**으로 조회한다. 앞서 완료한 다른 요청이 있다는 이유로 실패하거나 통과하지 않는다.
- 복구 후 SQL 이력 2건의 ID 집합·결과를 대조하고 각 고객 수신 본문이 같은지 확인한다. ORIGIN·STEP 삭제, Redis 완료 TTL, SQL 통지 DELIVERED·정리 DONE이 모두 필요하다.
- 최초 인입 시각과 D-02의 deadline/resultAt은 장애 전 원장 값과 같아야 한다. 기한을 재시작 시각으로 연장하면 실패다.
- 최종 요청·발송·업체 웹훅·최종 결과 토픽의 모든 파티션 lag는 0이어야 한다. 조회용 consumer는 subscribe/commit하지 않는다.
- 여섯 JVM은 전체 시험에서 PID를 유지한다. D-01의 일시정지·재개와 앱 재시작을 구분한다.

```sh
# JDK 21, Docker, scripts/poc/setup.py 도구 준비 후
./mvnw -DskipTests package
.poc-tools/venv/bin/python -m unittest discover -s scripts/poc -p 'test_*.py'
.poc-tools/venv/bin/python scripts/poc/dynamo_recovery.py
```

원장 스냅샷·컨테이너 상태·연결 실패·worker 신호·계측 원문·Kafka 위치·SQL 이력·고객 수신을 `.poc-results/<실행 ID>/`에 보관한다. 시험 실패 시에도 자료와 볼륨을 유지한다. 종료 시 소유 JVM과 컨테이너만 정지한다.

## 3. 해석과 남은 범위

DynamoDB Local의 연결 불가와 동일 볼륨 재연결 시험이다. AWS 관리형 DynamoDB의 가용성, throttling/5xx, 쓰기 성공 후 응답 유실, 데이터 유실·PITR 복원·AZ 전환까지 검증한 것은 아니다. 장애 중 DB 내용을 직접 조회할 수 없으므로 그 구간의 STEP 존재를 조회했다고 주장하지 않는다. 장애 전 원장과 복구 후 결과·외부 효과를 대조한다.

만료 시각이 지나도 원장 변경을 저장할 수 없으면 결과 확정과 고객 통지는 복구까지 늦어진다. 복구 후 원래 업무 기한으로 확정하는 동작을 확인하며, 장애 중 기한 내 고객 수신을 보장한 시험은 아니다.

두 시나리오는 ORIGIN이 이미 저장된 요청이다. ORIGIN 저장 전 장애로 Ingress가 DLT에 격리하는 요청의 자동 만료·운영 재처리는 [DLT 절차](12-접수-실패-격리와-DLT-재처리-절차.md)의 후속 범위다. 업체 호출 후 결과 저장 중 장애, 웹훅 반영 대기, 정리 도중 DB 장애의 전체 통합 시험도 별도다. Redis·Kafka 장애와 운영 조회·조치 기능 보완도 남아 있다.

## 4. 2026-09-25 실행 결과

실행 `full-flow-4896c83ceb0b`의 **2개 시나리오가 모두 통과**했다. [경계·환경·결과 원문](검증-결과/2026-09-25-DynamoDB-장애-발송-차단과-만료-복구.json)에 근거와 JAR/도구 해시를 보관했다. 기준 커밋은 `7c51b74`이며 시험 도구를 추가한 작업 트리에서 실행했다. Java 운영 코드는 변경하지 않았다.

| 확인 항목 | 실측 결과 |
|---|---|
| 선점 전 경계 제어 | dispatch PID 8420을 약 2.23초 SIGSTOP 후 SIGCONT. 재개한 상태에서 장애 동작 확인 |
| D-01 장애 중 | 선점 실패 0→2, 업체 HTTP/TCP 호출·효과 0, 발송 Kafka 미커밋 위치 유지·lag 1 |
| D-01 복구 후 | 최초 인입 시각 유지, HTTP 호출/효과 각 1회, DELIVERED 이력·고객 수신·정리 |
| D-02 장애 중 | STEP ACCEPTED를 저장한 뒤 DB 중단. 원래 기한 경과 관측, lifecycle 작업 실패 1→2, 업체 호출/효과 1회 유지 |
| D-02 결과 | deadline/resultAt `10:14:11.385Z` 유지, 고객 EXPIRED 수신 `10:14:16.275520Z` |
| 두 요청의 완료 | SQL 이력·통지 완료·정리 완료 각 2건, ORIGIN·STEP 삭제와 Redis 완료 TTL 확인 |
| 외부 효과 | 최종 HTTP 호출/효과 각각 2회, TCP 각각 0회 |
| 자동 복구 | 같은 DB 컨테이너·볼륨 유지, 여섯 JVM PID 불변, 네 토픽 모든 파티션 lag 0 |
| 도구 시험·빌드 | Python 41개 통과, 전체 Maven package 통과(Java 시험은 이번 실행에서 생략) |

만료 결과의 실제 고객 수신은 업무 기한보다 약 4.89초 늦었다. 이 값은 로컬 DB 재기동·조회·SQL·고객 HTTP 처리를 포함하는 이번 관측값이며 운영 복구 시간이나 고객 수신 SLO로 사용하지 않는다.

### 장애 중 조회 횟수에서 확인한 후속 과제

D-02 중 dispatch의 DynamoDB 실패 **논리 SDK 호출 누적**은 8→809로 증가했다. 마지막 관측의 누적 실패 809회 중 Query가 805회였다. 시험에서 누락 복구 주기를 3초로 줄였고, 현재 scheduler가 장애 중에도 shard 조회를 반복한 결과다. 이는 네트워크 재시도 횟수나 요청당 과금 읽기 횟수가 아니며, 운영 10분 주기의 실측값도 아니다.

원장 보존·발송 차단은 통과했지만 저장소 장애가 지속될 때 복구 조회에 backoff를 적용하는 보완은 남아 있다. 후속 변경 시 복구 감지 지연·원래 만료 기한 보존을 함께 검증해야 한다.

시험 종료 후 소유 JVM과 다섯 컨테이너의 정지를 확인했고 볼륨·검증 자료를 유지했다.
