# 그라파나 대시보드 실행과 PPT 항목 대응

기준일: 2026-09-24. 제공된 `KT_RCS_AWS전환_대시보드개발_UI 시나리오_20260922_v0.1.pptx`의 3~9번 슬라이드를 기준으로 **실행 가능한 로컬 Grafana 화면 7개**를 구성했다. 원본 PPT는 수정하거나 저장소에 복사하지 않았다.

현재 HTTP 1차 접수 경로를 관찰할 수 있다. 결과 웹훅·TCP 2차·업무 만료·고객 최종 결과 통지는 아직 업무 코드가 없으므로 화면에도 미구현으로 표시한다. 예시 수치나 가상의 통신사·AWS 존을 정상 상태처럼 넣지 않는다.

## 1. 바로 열어 보기

- [통합 관제](http://localhost:13000/d/delivery-overview): KPI 6개, E2E 8단계, 내부 채널, 적체·지연·경보.
- [서비스](http://localhost:13000/d/delivery-services): 처리량, 지연, 오류, CPU/JVM/GC, consumer commit·파티션, 수집 상태 이력.
- [외부 업체](http://localhost:13000/d/delivery-providers): HTTP 접수, 호출 지연, 내부 중복 차단, 시뮬레이터 호출·처리 효과.
- [고객](http://localhost:13000/d/delivery-customers): 고객별 접수·중복·운영 확인 관측 TOP 10과 고객 ID 검색.
- [오류코드](http://localhost:13000/d/delivery-errors): 업체·단계·HTTP 오류코드별 관측, API 응답, DB 오류.
- [인프라](http://localhost:13000/d/delivery-infra): Kafka, DynamoDB SDK, Redis, PostgreSQL, 수집 상태·경보·제한 우회.
- [메시지 추적](http://localhost:13000/d/delivery-trace): deliveryId로 단계별 로그 조회. 빈 검색값은 전체다.

다크 테마, 상단 KPI, 가로 단계 흐름, 좌측 채널 영역, 상세 화면 이동을 PPT와 맞췄다. Grafana 기본 패널과 화면 링크를 사용하므로 PPT의 모달·탭을 그대로 복제한 별도 웹 앱은 아니다. 단계 클릭은 상세 대시보드로 이동한다.

## 2. 실행과 중지

Docker Desktop, JDK 21, Python 3.12 이상이 필요하다. Python은 기본 라이브러리만으로 데모·화면 검증·JSON 생성을 실행한다. Kafka 수집기의 외부 라이브러리는 컨테이너에 설치한다.

```bash
# 저장소 루트. JAVA_HOME은 자신의 JDK 21 경로로 설정한다.
./mvnw package
bash scripts/monitoring.sh up
bash scripts/monitoring.sh status

# 최초 앱 기동·consumer 할당·수집까지 약 1분 기다린 후 실행한다.
bash scripts/monitoring.sh demo --rate 20 --seconds 30

# 트래픽 종료 후 적체가 비워지고 다음 수집이 끝나면 검증한다.
bash scripts/monitoring.sh verify

# 오류 화면 확인이 필요할 때: 정상 요청 외에 의도적인 HTTP 500 한 건 추가
bash scripts/monitoring.sh demo --rate 5 --seconds 10 --errors

# 조회 / 중지. stop은 데이터를 삭제하지 않는다.
bash scripts/monitoring.sh logs dispatch
bash scripts/monitoring.sh stop
```

최초 이미지 다운로드 시간은 별도다. `up` 완료는 컨테이너 생성 완료이며 전체 업무 준비 완료를 보장하지 않는다. `status`와 Grafana 수집 상태를 확인한다. 생성된 요청 중 매 10번째는 직전 요청과 같은 키를 써서 중복 차단을 보여 준다. `--errors`는 의도적 업체 500을 만드는 시험 데이터다. 초기 버전은 lease 만료 후 운영 확인으로 남겼고, 실패 저장 구현 이후 새 빌드에서는 HTTP_ERROR 사유로 즉시 운영 확인에 저장한다. 자동 업무 복구를 구현한 것은 아니다. 이 기록은 환경의 DB에 유지된다.

Compose project는 `platform-monitoring`이며 기존 개발·PoC 환경과 DB, Kafka, Redis, 로그 볼륨을 분리한다. `.env`는 읽지 않고 전용 포트를 사용한다. Grafana `13000`, Prometheus `19099`, 테스트 API `38080`, Kafka `39092`, Redis `36379`, PostgreSQL `35432`, DynamoDB Local `38000`은 **127.0.0.1에만 공개**한다. 앱 관리 포트·Loki·Alloy·exporter는 Docker 내부 통신만 허용한다.

Grafana는 로컬 익명 Viewer로 읽기만 가능하다. 관리자 임의 비밀번호는 `.monitoring/grafana-admin`에 권한 0600으로 생성하고 커밋하지 않는다. 이는 개발용 설정이며 다중 사용자 운영 인증·권한 체계를 대신하지 않는다. 테스트 계정·DB 비밀번호·JWT 키는 기존 로컬 fixture를 사용한다. `up`은 고객 1의 Redis 데모 정책(TPS 2,000, quota 1,000,000)을 다시 설정한다.

앱은 `.monitoring/jars/<SHA256>/`에 복사한 JAR를 사용한다. 실행 중 `target/*.jar`를 다시 빌드해도 실행 파일이 바뀌지 않는다. 변경 반영은 빌드 후 `up`을 다시 실행한다. 과거 JAR 복사본은 자동 삭제하지 않으며, 정리가 필요하면 환경 중지 후 사용하지 않는 복사본을 지운다.

## 3. PPT와의 대응

| 슬라이드 | 요구 화면·항목 | 현재 반영 | 남은 조건 |
|---|---|---|---|
| 3 | 인입 TPS | API 요청 TPS. 중복·거절 포함 | 고유 업무량과 구분 |
| 3 | 전송 성공률 | HTTP 업체 **접수** 성공률로 명확히 표시 | 최종 성공률은 결과 수신·최종화 필요 |
| 3 | 결과 지연 3분 초과 | 접수 저장 지연 p95 및 Kafka 최장 대기를 표시 | 결과 웹훅 지연은 미구현 |
| 3 | 폴백 건수 | 미구현 명시 | TCP 2차·분기 로직 필요 |
| 3 | MSK Max Lag | 로컬 Kafka group committed 최대 파티션 lag | MSK 운영 연결은 후속 |
| 3 | MTTD·MTTR | 미연결 명시. 현재 경보 수를 별도 제공 | 장애 시작·탐지·복구 이력이 있어야 산출 가능 |
| 3 | E2E 8단계 | 인입·원본 저장/인계·큐·HTTP 접수는 실제 지표, 후속 4단계는 미구현 | 결과 수신·폴백·최종화·고객 통지 필요 |
| 3 | 내부 채널, retry/recovery, 폴백 sender/polling | HTTP 인입과 미구현 영역 안내 | 업체 결과 조회는 불가능하다는 합의에 따라 polling을 발송 복구 기능으로 추가하지 않음 |
| 3 | A/C 존 현황, 최근 경보, 지연 추이 | local 서비스 상태·경보·구간 지연·lag 추이 | 실제 AZ 배치·라벨·수집 연결 필요 |
| 4 | 서비스 처리량·p95·오류율 | 실제 구간별 지표 | K8s Pod 단위는 후속 |
| 4 | Pod 수·CPU·restart·상태 | 프로세스 CPU, 가동 시간, 관측한 앱 재시작, 수집 상태 이력 | EKS Pod/노드·restartCount는 미연결 |
| 5 | 업체별 TPS·성공률·p95·오류코드 | mock-provider HTTP·오류 로그, p50/p95/p99 | 실업체가 없으므로 SKT/KT/LGU 수치를 만들지 않음 |
| 5 | 결과 수신 지연·미수신·대체 발송 | 미구현 안내 | 결과 웹훅·기한·TCP 로직 필요 |
| 6 | 고객별 영향도 TOP, SLA | 로그 기반 API 접수·업체 접수·중복·운영 확인 TOP 10, 고객 검색 | 고유 실패·미수신·폴백·SLA 집계는 상태 projection 필요 |
| 7 | 존·업체·오류코드 매트릭스 | local/업체/단계/실제 HTTP 코드·전송 오류 표 | 71010/79998 등 업무 코드는 프로토콜 정의 후 추가 |
| 8 | Kafka lag/TPS | committed lag·오래된 대기·토픽 offset 증가율 | broker 내부 상세·MSK CloudWatch는 후속 |
| 8 | MemoryDB·PostgreSQL latency | Redis 메모리/명령/eviction/연결, 앱 제한 판단 p95, PG 연결/commit/rollback/연결 | 앱 판단 지연은 Redis 자체 지연이 아님. PG 쿼리 지연과 AWS MemoryDB는 미연결 |
| 8 | EKS 노드 CPU | 앱 프로세스 CPU/JVM/GC | 노드·컨테이너 자원 수집 필요 |
| 9 | 단계별 메시지 상세·로그 모달 | deliveryId 검색 + Loki 단계 로그, 고객별 로그 | 현재 DB 상태 목록·운영 조치 API는 후속. 모달 대신 별도 화면 |

PPT의 ‘3분’은 결과 지연 예시다. 합의된 1차 3시간·2차 4시간의 업무 만료를 바꾸지 않는다. 과금 기능도 현재 요구 범위에 추가하지 않는다.

## 4. PPT 외에 추가한 지표와 이유

| 추가 항목 | 판단에 도움이 되는 이유 |
|---|---|
| 인입 TPS와 신규 접수 저장 TPS 비교 | 입력은 받지만 처리가 밀리는 상태를 구분 |
| 가장 오래된 미커밋 Kafka record 나이 | 적체 건수가 작아도 오래 멈춘 요청 탐지 |
| Kafka 관측 성공·관측 시각 | 관측 실패를 lag=0 정상으로 오인하지 않음 |
| Consumer별 commit 지연·파티션 할당 | 앞선 100 TPS 병목 분석에서 쓰인 대기와 병렬도 확인 |
| DynamoDB 작업 종류·조건 불일치·일반 오류·p95 | 멱등 경합을 장애와 구분하고 최소 쓰기 설계의 비용 추세 확인 |
| 접수당 논리 SDK 쓰기 비율 | 불필요한 쓰기 증가 후보 확인. 정확한 WCU/건당 과금은 아님 |
| 내부 중복 차단과 외부 시뮬레이터 호출/효과 분리 | 외부 업체의 중복 제거에 기대는지 시험에서 비교 |
| 운영 확인 관측, DLT 인계 실패 경보 | 결과 불명·내구성 인계 실패를 일반 HTTP 오류와 구분 |
| Redis 제한 허용/차단/장애 우회·판단 지연 | Redis 장애에서도 진행한다는 정책이 실제 적용되는지 확인 |
| 수집 대상·DB backend 연결 경보 | exporter 프로세스 정상과 실제 DB 접속 정상을 구분 |
| JVM heap·GC·앱 재시작·Alloy 로그 전송 포기 | 자원 압박과 관측 자체의 누락 가능성 확인 |

## 5. 수집 구조와 수치 해석

```text
앱 MeterRegistry ───────────┐
API JWT 인증 지표 relay ────┤
읽기 전용 Kafka probe ──────┼─ Prometheus ─ Grafana 7개 화면
Redis / PostgreSQL exporter┘
앱 delivery.audit JSON ─ Alloy ─ Loki ──────┘
```

메트릭과 화면은 10초 주기다. Kafka probe는 group에 subscribe하거나 offset을 commit하지 않는다. 관측 실패 또는 마지막 성공 후 45초가 지나면 `platform_kafka_probe_up=0`으로 내보내고 오래된 lag 값을 숨긴다. Prometheus `up`과 probe 상태를 함께 봐야 한다.

API 관리 지표는 JWT 인증을 유지한다. collector가 로컬 계정으로 토큰을 받고 401일 때 한 번 갱신한다. 관리 포트를 무인증으로 외부에 열지 않는다. 모니터링용 DynamoDB Scan·추가 쓰기는 없다. DynamoDB 패널은 앱이 이미 수행하는 SDK 호출의 계측이며 AWS 물리 요청 수·WCU/RCU·throttling 전체를 의미하지 않는다.

`delivery.audit` 전용 로그만 Loki로 전달한다. `deliveryId`, `tenantId`, `attemptId`, 단계, 분류, 업체, 오류코드를 구조화하고 메시지 본문·멱등키·JWT·비밀번호는 기록하지 않는다. Loki label은 service/zone으로 제한하고 고객·메시지 ID는 JSON 필드로 검색한다. TOP 집계는 필요한 필드만 추출해 메시지별 시계열 폭증을 피한다.

로그는 운영 관측 자료이며 멱등 원장이나 유실 판정 근거가 아니다. 기록 전 프로세스 종료·로그 수집 장애·보관 종료에 따른 누락이 가능하다. `REVIEW_REQUIRED` 패널도 **관측 횟수**로, 실제 고유 미해결 건수가 아니다. 요청별 무손실·정합성 판정은 별도 PoC의 Kafka/DB/업체 대조를 유지한다. 최종 상태 조회를 위해 DynamoDB를 주기적으로 Scan하는 방식을 추가하지 않았다.

Prometheus counter의 재시작·rate/increase 외삽 때문에 화면의 구간 합계가 정수 입력 수와 정확히 일치하지 않을 수 있다. 로그 합계도 재전달에 따른 관측을 포함한다. 빈 결과는 ‘관측 없음/데이터 없음’으로 보며 성공률 100%로 채우지 않는다. 수집 상태 UP 역시 업무 처리 정상의 충분조건은 아니다.

Prometheus는 7일 또는 1GB 보관 설정, Loki는 로컬 파일 저장과 7일 보관 설정이다. 보관 정리는 비동기이며 디스크의 절대 상한 보장은 아니다. 앱 파일 로그는 파일당 10MB 회전, 이력 7일·전체 100MB 설정이다. 로컬 단일 인스턴스 구성으로 모니터링 자료의 무손실·다중 AZ 가용성을 보장하지 않는다.

## 6. 검증 기록

2026-09-24 로컬 Docker 환경에서 다음을 확인했다. 실행 산출물은 커밋 제외 경로 `.monitoring/`와 `/tmp/platform-monitoring-*.log`에 저장했다.

| 검증 | 입력·확인 내용 | 결과 |
|---|---|---|
| 앱 빌드·회귀 | Maven package, 단위 42건. 별도 환경이 필요한 통합 15건은 이번 빌드에서 skip | 성공. 이전 57건 통과와 이번 수행 범위를 구분 |
| collector 단위 | Kafka 실패/오래된 관측 숨김, group·partition 유지, JWT 401 갱신, 비인증 오류 보존 | 4건 통과 |
| 정상·중복 데모 1 | 20 TPS × 30초, 600회 제출/540개 고유 ID | 모두 202. 초기 Dispatch 적체는 재기동 후 해소 |
| 정상·중복 데모 2 | 20 TPS × 60초, 1,200회 제출/1,080개 고유 ID | 모두 202, 실제 Dispatch 접수 로그와 지표 표시 |
| 오류 화면 | 10 TPS × 20초, 200회/180개 고유 ID + 의도적 Provider 500 한 건 | API 202와 이후 업체 오류를 별개로 관측, 운영 확인 로그 표시 |
| 설정 | promtool config/rule 검사 | 11개 경보 규칙 문법 통과 |
| Redis 장애·복구 | 전용 Redis 중단 중 5회 제출, 제한 우회 counter와 두 경보 확인 후 재시작 | 5회 모두 202, 우회 5회, RedisBackendUnavailable·DeliveryLimitsBypassed firing. DB 5건 모두 ACCEPTED |
| 결과 불명 표본 대조 | 의도적 업체 500 요청의 DynamoDB Attempt 읽기 | REVIEW_REQUIRED 확인 |
| 전체 화면 질의 | 7개 대시보드의 PromQL/LogQL 76개, 수집 대상 10개, 최근 audit 로그 1,000줄 검사 | 질의 오류 없음, target 모두 UP, 실제 접수 저장·세 앱 로그·lag 0 확인 |
| 화면 렌더링 | 별도 임시 Chrome 프로필로 7개 화면 렌더링 | 한글·표·그래프·로그 표시 확인, 표의 불필요한 열과 긴 JSON 표시 정리 |
| 최종 화면용 트래픽 | 20 TPS × 30초, 600회/540개 고유 ID | 모두 202. 입력 중 KPI 화면 기록 후 배출·전체 질의 재검증 |

최초 수집 연결만 확인했을 때 모든 target이 UP이어도 Dispatch가 처리하지 않는 상태가 있었다. 최종 JAR로 앱을 재기동한 뒤 적체가 해소됐다. 원인을 단정하지 않으며, 실행 중 JAR 재빌드가 파일을 바꾸지 않도록 복사본을 사용하게 했다. 검증 스크립트도 target UP 외에 **실제 접수 저장 관측·세 앱의 audit 로그·Kafka 적체 해소**를 확인하도록 강화했다.

첫 Redis 장애 시험에서는 우회 5건을 기록했지만 우회 경보가 발생하지 않았다. counter의 첫 시계열 값이 이미 5여서 `increase()`가 증가량을 관측하지 못한 것이었다. API 시작 시 고정된 outcome별 Timer를 0으로 등록하도록 수정한 뒤, 실제 0 스크레이프 → Redis 중단 → 최초 5회 우회 → 두 경보 firing → Redis 복구를 다시 확인했다. 스크레이프 전에 프로세스가 종료되는 모든 짧은 사건까지 metric 기반 경보가 보장하는 것은 아니다.

Redis 시험은 기존 개발 환경이 아닌 `platform-monitoring-redis-1`만 중단했다. 재현할 때는 중단 전 `delivery_admission_duration_seconds_count{outcome="redis_unavailable_bypass"}` 기준값을 확인하고, `docker stop platform-monitoring-redis-1` 뒤 소량 데모를 제출한다. `redis_up=0`, 우회 횟수 증가, 두 경보를 확인한 뒤 **반드시 `docker start platform-monitoring-redis-1`로 복구**한다. 운영 미해결 상태 표본 읽기는 이 시험의 별도 검증이며 상시 수집 경로에 DB 조회를 추가한 것이 아니다.

화면 기록: [통합 관제](모니터링/통합-관제.png), [서비스](모니터링/서비스.png), [고객별 관측](모니터링/고객별-관측.png), [오류코드](모니터링/오류코드.png), [인프라](모니터링/인프라.png), [메시지 추적](모니터링/메시지-추적.png). 시험 중·종료 후의 실제 관측 화면이며 현재 상태를 고정한 예시 데이터가 아니다.

`verify`는 7개 화면의 실제 PromQL/LogQL 질의, 10개 수집 대상, 실제 로그의 종류·민감 필드 부재, 처리 진행을 검사하고 `.monitoring/verification.json`을 생성한다. 트래픽이 진행 중이면 적체 0 검사에서 실패할 수 있으므로 입력을 멈추고 배출 후 실행한다. 이는 요청별 전수 정합성 검사나 최대 성능 시험을 대신하지 않는다.

앞선 100 TPS 성능 결과는 [워커 동시 처리 비교](17-워커-동시-처리와-발행-대기-성능-비교.md)에 보존한다. 이번에는 로그와 collector/exporter가 추가되고 앱 실행 환경도 바뀌었으므로, 그 결과를 모니터링 동시 실행 상태의 성능 보장으로 옮겨 쓰지 않는다. 다음 성능 시험은 같은 조건에서 모니터링 유무를 비교하고 지연·lag·CPU·DynamoDB 호출 변화와 요청별 대조 결과를 함께 남긴다.

## 7. 설정 변경과 후속 작업

### 모니터링을 켠 상태의 성능 검증

최초 실행 결과는 [모니터링 동시 실행 성능 검증](20-모니터링-동시-실행-성능-검증.md), Kafka 수정 후 결과는 [카프카 패치와 성능 재검증](21-카프카-패치와-성능-재검증.md)에 별도로 남긴다.

`demo`는 화면 확인용이다. 요청별 정합성까지 검사하려면 기존 PoC 도구 설치 후 아래 명령을 사용한다.

```bash
python3 scripts/poc/setup.py
bash scripts/monitoring.sh benchmark --suite smoke
# 소량 → 10 TPS 1분 워밍업 → 100 TPS 20초 중복 → 50/100 TPS 각 3분
bash scripts/monitoring.sh benchmark --suite baseline
```

이미 실행 중인 `platform-monitoring`에 연결하며 앱·DB·수집 서버를 재시작하거나 정책을 바꾸지 않는다. 고유 시험 ID를 쓰고 실행 전후 offset 범위와 해당 ID의 DB·업체 기록을 대조한다. 기존 데모 데이터는 유지되며 시뮬레이터 관리 횟수 조회는 입력·배출 이후에만 수행한다. API 인증·Redis 제한은 계속 적용되고 시험 입력은 데모 고객의 quota를 소비한다.

관리 포트를 추가 공개하지 않는다. collector 내부에서 읽기 전용으로 앱 지표·시뮬레이터 횟수를 조회하며 토큰은 stdin으로 전달하고 증거 파일에는 저장하지 않는다. 실제 실행 중인 컨테이너 image ID와 JAR 해시, 동시성·linger 설정을 남긴다. `.poc-results/<실행 ID>/`의 요청별 대조·부하·구간/자원 파일 형식과 중단 기준은 [PoC 문서](15-격리된-성능-PoC-실행과-결과-대조.md)와 동일하다.

`demo`와 `benchmark`의 동시 실행은 파일 lock으로 차단한다. 시험 중 수동 API 제출이나 서비스 재기동도 하지 않는다. 예상 밖 Kafka ID·재시작·적체 증가·DLT·운영 확인·수집 장애가 발견되면 후속 입력률 증가를 중단한다. 종료해도 Grafana와 앱은 계속 실행된다. 별도 수집·Docker stats 비용이 시험 부하에 포함되며, 모니터링을 끈 대조군이 없으므로 **이 실행만으로 모니터링 오버헤드를 계산하지 않는다.**

각 단계가 끝나면 해당 시간 범위의 Kafka·API·Ingress·Dispatch 로그를 `*-runtime.log.gz`에 보존하고 `runtime-health.json`으로 검사한다. ERROR, offset commit 실패, coordinator 상태 불일치, 음수 histogram 오류 또는 로그 수집 실패가 있으면 후속 단계를 중단한다. `result.json`의 `dataAndLoadPass`는 완주·정합성, `runtimeHealthy`는 로그 검사 결과이며 최종 `pass`는 둘 다 참이어야 한다. 로그가 조용하다는 이유만으로 모든 장애가 없음을 증명하지는 않는다. 상세 stack trace는 여러 줄이므로 signal 횟수는 고유 장애 건수가 아니다.

화면 원본은 `scripts/monitoring/generate_dashboards.py`, 생성 JSON은 `monitoring/grafana/dashboards/`다. 생성기를 고친 뒤 `python3 scripts/monitoring/generate_dashboards.py`를 실행하고 JSON도 함께 커밋한다. Grafana는 파일 provisioning으로 읽으며 UI 변경을 저장하는 용도로 쓰지 않는다.

경보는 Prometheus 규칙 평가와 대시보드 표시까지 구현했다. 현재 임계값(lag 100건 1분, 최장 대기 30초 등)은 로컬 시험용이며 운영 SLO가 아니다. Slack·메일·문자 발송, Alertmanager 연동, 경보 담당자·반복 억제는 아직 없다.

남은 작업은 결과 웹훅/TCP/만료 업무별 계측, 고유 운영 미해결 상태 조회, 운영 환경의 AWS/컨테이너 자원 수집, 외부 경보 전달과 장애 이력이다. MTTD/MTTR은 이력이 생긴 뒤 계산한다.

## 8. 공식 근거

- [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/): 데이터소스·대시보드를 파일로 관리하고 저장소에서 재현한다.
- [Grafana Docker 설치](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/): 로컬 컨테이너 실행과 영속 볼륨 구성의 기준이다.
- [Prometheus 설정](https://prometheus.io/docs/prometheus/latest/configuration/configuration/): scrape target, 규칙 파일, SIGHUP 설정 재적용 기준이다.
- [Alloy 파일 로그 수집](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.file/) 및 [로그 처리](https://grafana.com/docs/alloy/latest/reference/components/loki/loki.process/): 파일 추적·JSON 처리·허용 로그 필터의 근거다.
- [Loki 설정 예제](https://grafana.com/docs/loki/latest/configure/examples/): 단일 로컬 파일 저장 구성을 참고했다. 운영 고가용성 구성의 근거로 사용하지 않는다.
- [Redis exporter](https://github.com/oliver006/redis_exporter), [PostgreSQL exporter](https://github.com/prometheus-community/postgres_exporter): 앱 지표와 별도로 DB 연결·서버 상태를 관측한다.
