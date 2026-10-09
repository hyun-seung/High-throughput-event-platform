# 메인 메시징 경로 모니터링 실행

이 문서는 현재 `/api/v1/messages`부터 통신사별 1차 HTTP 발송, 업체 웹훅, 결과 판단, 완료 이력, 고객 웹훅까지 실행하는 로컬 관측 환경을 설명한다. TCP 2차 발송기도 함께 실행한다.

## 실행과 확인

Docker Desktop과 JDK 21, Python 3.10 이상이 필요하다. 저장소 루트에서 실행한다.

```bash
./mvnw package
bash scripts/monitoring.sh up
bash scripts/monitoring.sh status
bash scripts/monitoring.sh diagnose
bash scripts/monitoring.sh demo --rate 1 --seconds 1 --errors --secondary
bash scripts/monitoring.sh verify
```

`demo`는 정상 메시지를 `/api/v1/messages`로 넣고 SQL 최종 이력, 1차 과금 대상 기록, 고객 웹훅 전달까지 기다린다. `--rate`는 초당 1~100건, `--seconds`는 1~120초다. `--errors`를 주면 1차 HTTP 실패 1건도 제출해 실패 이력과 고객 웹훅을 확인한다. `--secondary`를 추가하면 HTTP 실패 후 TCP 성공 메시지 1건도 제출하고, 이 메시지에는 과금 대상 기록이 생기지 않는지 검사한다. 고객 웹훅은 outbox의 전달 상태와 `TBL_WEBHOOK_HIST`의 수신 확인 이력을 함께 확인한다. 결과는 `.monitoring/demo-latest.json`에 저장하며 `cases`에 모든 `clientMsgId`와 검증 경로를 남긴다. 업무 결과를 기다리기 전에 `pass=false`로 저장하고 전체 검증을 마쳐야 `pass=true`가 된다. `exampleClientMsgId`로 로그를 검색할 수 있다. `verify`는 발행 복구를 포함한 11개 AP, Kafka 관측기, Redis·PostgreSQL exporter의 Prometheus 수집 상태와 Grafana 대시보드 7개를 확인하고 `.monitoring/verification.json`에 결과를 저장한다. 데모의 업무 결과 검증과 관측 연결 검증은 서로 다른 검사다.

```bash
bash scripts/monitoring.sh demo --rate 5 --seconds 10 --errors
bash scripts/monitoring.sh logs result-manager
bash scripts/monitoring.sh stop
```

`stop`은 컨테이너만 정지하고 데이터 볼륨은 유지한다. JAR를 수정했다면 빌드 후 `up`을 다시 실행한다. 스크립트는 실행 JAR를 `.monitoring/jars/<SHA256>/`에 복사하므로 실행 중 빌드가 컨테이너의 파일을 바꾸지 않는다.

`up`은 AP를 순서대로 띄우며 각 관리 지표가 수집될 때까지 기다린다. Prometheus와 Alloy도 재시작해 변경된 지표·로그 수집 설정을 반영한다. 로컬 Compose에서는 JVM 메모리와 Kafka 소비자 병렬도를 낮춰 기동한다. 실제 처리량 측정에는 이 로컬 설정을 그대로 사용하지 않는다.

## 화면과 데이터 경로

Grafana는 [통합 관제](http://localhost:13000/d/messaging-overview), [서비스](http://localhost:13000/d/messaging-services), [통신사](http://localhost:13000/d/messaging-providers), [고객](http://localhost:13000/d/messaging-customers), [오류](http://localhost:13000/d/messaging-errors), [인프라](http://localhost:13000/d/messaging-infra), [메시지 추적](http://localhost:13000/d/messaging-trace) 화면을 제공한다. Prometheus는 [19099](http://localhost:19099), 테스트 API는 `127.0.0.1:38080`이다. `trace` 화면에서는 `clientMsgId`로 로그를 찾는다.

```text
MSG-RECEIVE-API → MESSAGE-RECEIVED → PRE-SEND-MANAGER
                                  → MSG-SKT/KT/LGU-SENDER → 업체 웹훅
                                  → WEBHOOK-RECEIVE-API → MSG_RESULT
                                  → MSG-RESULT-MANAGER → MSG-RESULT-FINALIZED
                                                       → WEBHOOK-SEND
                                  → MSG-COMPLETE-MANAGER → TBL_MSG_HIST/TBL_CDR_HIST
                                  → MSG-WEBHOOK-SENDER → 고객 웹훅/TBL_WEBHOOK_HIST
```

별도 `MSG-TCP-SENDER`는 2차 발송 명령이 나올 때 실행한다. 모든 신규 AP의 `/actuator/prometheus`, JWT가 필요한 API 지표 relay, 읽기 전용 Kafka offset 관측기, Redis·PostgreSQL exporter를 Prometheus가 수집한다. Alloy가 신규 AP의 구조화 파일 로그를 Loki로 보낸다. Kafka 관측기는 메시지를 소비하거나 offset을 commit하지 않는다. 메시지 ID와 고객 ID는 Prometheus label로 넣지 않는다.

Compose project는 `platform-messaging-monitoring`이다. 과거 `platform-monitoring`의 Kafka 데이터는 자동 삭제·이관하지 않는다. 기존 Kafka 볼륨의 실제 마운트 구조가 다를 수 있어 새 project와 데이터 볼륨을 분리했다. 양쪽의 고정 호스트 포트가 같으므로 과거 project가 실행 중이면 먼저 정지해야 한다. 새 환경의 포트는 로컬 loopback에만 공개하고, 앱 관리 포트와 Loki·Alloy·exporter는 Docker 내부에 둔다. 로컬 Grafana는 익명 Viewer이며 관리자 비밀번호는 `.monitoring/grafana-admin`에 생성한다.

이 화면은 로컬 mock 통신사와 고객 웹훅 수신기로 재현한 흐름이다. 실제 통신사·고객의 처리량이나 SLA를 뜻하지 않는다. HTTP 200은 업체 접수이고 최종 성공은 업체 웹훅 결과다. 과금 패널은 1차 성공의 과금 **대상 기록**이며 금액 계산은 하지 않는다. 2차 TCP 규격도 실제 업체 계약 전까지 임시 규격이다.


2026-10-08 로컬 검증에서는 신규 AP 10개·Kafka 관측기·DB exporter의 수집과 7개 Grafana 화면이 모두 통과했다. 정상 1건은 최종 이력 1건·과금 대상 1건·고객 웹훅 전달 1건까지 확인했다. 정상 1건과 1차 HTTP 실패 1건을 함께 보낸 시험에서는 최종 이력 2건·과금 대상 1건·고객 웹훅 전달 2건을 확인했다. 이는 소량 기능 검증이며 부하 성능 결과가 아니다.

## 지연 진단과 2026-10-08 재검증

```bash
bash scripts/monitoring.sh diagnose
bash scripts/monitoring.sh demo --rate 1 --seconds 1 --errors --secondary
bash scripts/monitoring.sh verify
```

`diagnose`는 Docker CPU·메모리 용량, 실행 컨테이너 수, 같은 저장소의 다른 Compose 프로젝트, VM의 CPU·메모리·I/O pressure, 스왑 상태, Prometheus 응답 시간과 수집 상태를 읽어 `.monitoring/diagnostics.json`에 기록한다. 컨테이너를 자동 정지하거나 데이터를 삭제하지 않는다. Docker 조회는 30초, HTTP 조회는 20초로 대기를 제한한다.

이번 지연은 Docker VM의 메모리 압력과 스왑·페이지 회수 경합에서 발생했다. 약 8GB VM에 35개 컨테이너가 실행됐고, 1GB 스왑의 여유는 148KiB였다. 10초 평균 CPU pressure `some`은 89.36%, 메모리는 49.55%였다. Kafka heartbeat 재연결과 DynamoDB 10초 타임아웃, Prometheus 7~20초 응답 지연이 함께 관측됐다.

소유 경로가 이 저장소로 확인된 `reliable-messaging-service`와 `reliable-event-platform`의 DB 각 3개, 이전 `platform` 프로젝트의 `dynamodb-local` 1개를 정지했다. 현재 모니터링 스택과 다른 저장소의 컨테이너는 유지했고 데이터 볼륨은 보존했다. 실행 수는 28개로 줄었고, 이후 CPU pressure는 0.38%, 메모리는 0.27%, Prometheus 조회는 32.89ms였다. 스왑 사용량 자체는 즉시 줄지 않았으므로 사용량만으로 지연 지속 여부를 판단하지 않는다.

중복 DB를 정지한 뒤 실행한 `runId=71516490`의 결과는 다음과 같다. 업체와 고객 수신기는 로컬 mock이며 업무 AP·Kafka·Redis·DynamoDB·PostgreSQL은 실제 실행했다.

| 경로 | 최종 단계·결과 | TBL_MSG_HIST | TBL_CDR_HIST | 고객 웹훅 |
|---|---|---|---|---|
| HTTP 200 → 성공 웹훅 | PRIMARY·SUCCESS | 1건 | 1건 | 전달·이력 확인 |
| HTTP 실패, 2차 전문 없음 | PRIMARY·FAILURE | 1건 | 0건 | 전달·이력 확인 |
| HTTP 실패 → TCP 성공 | SECONDARY·SUCCESS | 1건 | 0건 | 전달·이력 확인 |

세 메시지의 정리 상태는 모두 `DONE`이며 DynamoDB ORIGIN의 일관 읽기로 실제 삭제도 확인했다. 메시지별 ID는 `.monitoring/demo-latest.json`에 있다. 16개 Prometheus 대상, Grafana 7개 화면, Kafka offset 관측, Loki 로그 조회도 모두 통과했다. 소량 접수 p95는 127.31ms였으며 처리량·성능 보장 수치로 사용하지 않는다.

다시 기본 로컬 DB나 과거 프로젝트를 함께 기동하면 같은 VM의 자원을 공유한다. 실행 전 `diagnose`로 중복 프로젝트를 확인하고, 사용하지 않는 환경만 소유·사용 여부를 확인해 정지한다. 현재 검증 경로에는 기본 로컬 DB 프로젝트를 별도로 띄울 필요가 없다.

## 최초 Kafka 발행 실패 복구 검증

`publication-recovery`는 `messaging-publication-recovery-app` JAR을 별도로 실행한다. ORIGIN의 최초 인입 +60초 복구 후보를 조회해 같은 `clientMsgId`와 원문을 `message.received.v1`에 재발행한다. 로컬 조회 주기는 5초이며 기본 실행 설정은 60초다. 관리 지표는 `publication-recovery:19098`에서 수집하고, `publication-recovery.log`는 Alloy로 전달한다. 복구 AP는 Redis를 사용하지 않아 해당 전이 의존성을 제외했다.

```bash
bash scripts/monitoring.sh recovery-test
bash scripts/monitoring.sh verify
```

`recovery-test`는 이 모니터링 프로젝트의 복구 AP를 잠시 정지하고 접수 API의 Kafka 주소만 도달할 수 없는 로컬 주소로 바꿔 API를 재기동한다. 메시지를 한 번 접수해 DynamoDB 원본·복구 인덱스가 저장됐고 Kafka 발행은 실패했는지 확인한다. 이후 API 설정을 복원하고 복구 AP를 다시 시작한다. 복원은 검증 도중 예외가 발생해도 수행하며, 검증 중에는 같은 스택의 `up`이나 `demo`를 함께 실행하지 않는다.

고객 요청을 재접수하지 않고 복구 AP가 원래 ID로 발행했다는 로그, 최종 이력·과금 대상·고객 웹훅 이력 각 1건, DynamoDB 원본 삭제를 검증한다. 단계와 ID·성공 여부는 `.monitoring/recovery-latest.json`에 남긴다. 이 명령은 격리된 로컬 스택의 접수 API를 재기동하는 장애 시험이며 Kafka 브로커나 다른 프로젝트는 중단하지 않는다.

2026-10-08 발행 복구 검증 `runId=6c6aab71`, `clientMsgId=246db7c15fd74ce4b628fe86670cd756`는 통과했다. 접수 API의 최초 발행 실패와 복구 AP의 동일 ID 재발행 로그를 모두 확인했고, 고객 재접수 없이 1차 성공 이력·과금 대상·고객 웹훅 각 1건과 원본 삭제까지 완료했다. 인증 모듈 통합 후 일반 성공·최종 실패·TCP 전환의 세 경로도 재검증했다.

복구 AP 추가 후 Prometheus 대상 17개와 Grafana 화면 7개가 통과했으며 API·복구 AP의 구조화 로그를 Loki에서 확인했다. 장애 시험 뒤 API의 정상 설정 복원과 복구 AP의 전체 상태 `UP`도 확인했다.

## 통신사 이동·재시도 정책 검증

```bash
bash scripts/monitoring.sh policy-test
```

기존 AP를 그대로 사용해 13건을 접수하고, 로컬 mock의 메시지별 응답만 바꾼다. 기본 완료 대기는 420초이며 실제 정책의 1분 재시도 간격을 줄이지 않는다. 모든 결과가 완료된 뒤에도 70초 더 관측해 잘못 예약된 추가 발송이 없는지 확인한다. 결과·메시지별 ID·발송 순서·회차·간격·SQL 이력은 `.monitoring/policy-latest.json`에 남긴다.

| 입력과 조건 | 확인할 결과 | 건수 |
|---|---|---|
| HTTP `66001`·실패 웹훅 `66001` | SKT → KT 성공, 또는 SKT → KT → LGU 소진 후 `40002` | 4 |
| HTTP `66002`·실패 웹훅 `66002` | SKT에서 1분 이후 성공, 또는 최초+재시도 3회 소진 후 `40001` | 4 |
| HTTP 무응답 | DynamoDB의 5초 타임아웃 관찰, 1분 이후 성공 또는 총 4회 소진 후 `40003` | 2 |
| 세 통신사 불일치·TPS·무응답 소진 + 2차 전문 | TCP 성공으로 완료, 1차 과금 대상 0건 | 3 |

모든 케이스에서 최종 이력 1건, 고객 웹훅 상태·오류 코드와 전달 이력, 원본 삭제를 확인한다. 1차 성공에만 CDR 1건이 있어야 하며 실패와 TCP 성공에는 없어야 한다.

이 시나리오는 `--allow-message-scenarios`로 실행한 로컬 mock만 `payload.simulatorScenario`를 해석한다. 실제 업무 AP의 분기·재시도 시간은 바꾸지 않는다. `policy-test` 실행 중 mock을 재기동하거나 `recovery-test`로 같은 스택을 변경하지 않는다. 기존에 기동한 mock에 옵션이 없다면 `bash scripts/monitoring.sh up`으로 최신 구성을 반영한다.

2026-10-08 검증 `runId=243807d5`에서 13개 시나리오가 모두 통과했다. 첫 무응답의 타임아웃은 5.006~5.014초였고, TPS 재발송 요청 간격은 60.769~71.357초, 무응답 재발송 요청 간격은 65.640~66.413초였다. 무응답 간격에는 5초 응답 대기가 포함된다. 모든 최종 결과 후 70초 동안 추가 발송은 없었다. 이는 소량 기능 검증의 관측값이며 성능 보장 수치가 아니다.

검증 중 `TBL_MSG_HIST.carrier`가 1차 결과에서 비어 저장되는 조건 오류를 발견해 수정했다. 수정 후 KT 성공·LGU 소진 실패·SKT 재시도 결과의 최종 통신사와 회차를 확인했다. 관련 Java 테스트 37개가 통과했으며, 여기에는 별도 임시 스키마를 사용하는 PostgreSQL 테스트 7개가 포함된다. Prometheus 대상 17개와 Grafana 화면 7개를 포함한 관측 검증도 통과했다.

## 웹훅 배치·중복·순서 역전과 PostgreSQL 장애 검증

```bash
bash scripts/monitoring.sh up
bash scripts/monitoring.sh webhook-test
```

`webhook-test`는 격리된 모니터링 스택의 PostgreSQL을 잠시 정지한다. 같은 스택의 `demo`, `policy-test`, `recovery-test`, `up`과 함께 실행하지 않는다. DB는 장애 구간의 `finally`에서 다시 시작하며, 결과와 식별자·관측 근거는 `.monitoring/webhook-latest.json`에 남긴다. `up`은 소스가 변경된 mock을 다시 읽도록 해당 컨테이너를 재생성한다.

| 검증 구간 | 확인 내용 |
|---|---|
| HTTP 200 이전 웹훅 | 업체 mock이 성공 웹훅의 202를 먼저 받은 뒤 HTTP 200을 반환해도 최종 성공으로 완료 |
| 100건 혼합 웹훅 | HTTP 200을 기록한 100건의 성공 50건·실패 50건을 배열 하나로 전송, `MSG_RESULT` offset 합계가 1만 증가 |
| PostgreSQL 중단 중 배치 재전송 | 같은 100건 배열을 새 요청으로 한 번 더 전송, DynamoDB 최종 상태·원본 100건 유지, 완료 소비 그룹 lag 100건 유지 |
| PostgreSQL 복구 | 고객 재접수 없이 메시지별 이력 1건, 성공에만 과금 대상 1건, 고객 결과 1건, 원본 삭제, 완료 lag 0 |
| 정리 이후 중복·상충·미확인 ID | 기존 100건 배치와 성공 뒤 늦은 TPS 실패·실패 뒤 늦은 성공·미확인 ID를 추가 전송, 기존 최종 결과 유지 |
| 늦은 결과 보존·추가 발송 | 늦은 inbox 103건의 처리 상태·7일 TTL을 확인하고 70초 관측 후 발송·고객 결과·과금의 중복 여부 재확인 |

업무 AP·Kafka·Redis·DynamoDB·PostgreSQL은 실제 실행하며 업체와 고객 수신기는 로컬 mock이다. 첫 순서 역전 사례는 웹훅 접수와 HTTP 응답 반환 시각을 비교한다. 결과 판단 스케줄러가 HTTP 기록 이전에 실제 실행됐다는 보장까지 검증하는 시험은 아니다.

동일 통신사의 다음 회차 대기 중 이전 회차 웹훅이 다시 오는 모호성은 이 검증의 해결 범위에 포함되지 않는다. 업체의 회차 식별자가 없는 상태에서 기존에 보류한 정책을 유지한다. 이번 장애 시험은 PostgreSQL 중단·복구를 다루며 Redis 유실·DynamoDB 중단·Kafka 중단은 각각 별도 검증 범위다.

2026-10-08 검증 `runId=29628013`이 통과했다. SQL 중단 중 원본 100건과 완료 lag 100건을 확인했고, 복구 후 조기 웹훅 사례를 포함한 이력 101건·과금 대상 51건·고객 결과 101건·원본 삭제 101건과 lag 0을 확인했다. 늦은 결과 103건 처리 후 70초 동안 추가 발송·과금·고객 웹훅은 없었다. 복구 뒤 Prometheus 대상 17개와 Grafana 화면 7개를 포함한 관측 검증도 통과했다.

## Redis 발송 키 유실·저장소 장애 복구 검증

```bash
bash scripts/monitoring.sh storage-test
```

기동된 모니터링 스택에서 실행한다. 이 명령은 시험 메시지의 Redis HTTP 발송 키 1개를 제거하고, Redis 프로세스를 pause해 응답을 중단하고 DynamoDB 컨테이너는 일시 정지한다. 완료 AP의 DynamoDB 주소를 잠시 연결 불가 주소로 바꾸는 시험도 포함한다. 장애 구간마다 `finally`에서 서비스와 설정을 복원하며, 같은 스택의 다른 트래픽 생성·장애 시험·`up`과 함께 실행하지 않는다. 결과는 `.monitoring/storage-latest.json`에 남긴다.

| 검증 구간 | 확인 내용 |
|---|---|
| HTTP 200 기록 후 Redis 발송 키 유실 | 해당 키를 제거한 뒤 저장된 원본 명령을 Kafka에 3번 재발행해도 HTTP 호출 1회 유지. 관찰 기록이 바뀌거나 Redis 키가 다시 생기지 않음 |
| 최종 정리 이후 발송 명령 재전달 | 같은 원본 명령을 다시 발행해도 ORIGIN 부재로 새 발송 없음 |
| 첫 발송 전 Redis 응답 중단 | STEP은 `PENDING`, Sender lag는 1, 업체 호출은 0회. Redis 복구 후 같은 명령으로 발송·완료 |
| HTTP 200 기록 후 DynamoDB 중단 | 성공 웹훅은 Kafka 저장 후 202로 접수. 결과 Manager와 재전달을 받은 Sender는 lag 1씩 유지하고 최종 이력·새 HTTP 호출 없음 |
| DynamoDB 복구 | 웹훅·고객 요청을 다시 제출하지 않고 결과 처리·이력·과금·고객 웹훅·원본 정리 완료 |
| SQL 저장 후 DynamoDB 정리 연결 실패 | SQL 이력·과금은 각 1건, ORIGIN 유지, `cleanup_status=PENDING`과 시도 횟수·오류 기록. 연결 복구 후 정리 재시도 |

모든 메시지에 대해 HTTP 호출·최종 이력·과금 대상·고객 결과 각 1건, ORIGIN 삭제, Sender·결과·완료 소비 그룹의 lag 0을 확인한다. 완료 후 70초를 추가 관측하고 같은 검증을 반복한다. 업체·고객 수신기는 mock이며 업무 AP와 저장소는 실제 실행한다.

Redis 유실 시험은 **기존 `clientMsgId`의 발송 키 1개 유실**이다. Redis 전체 초기화나 고객의 접수 중복 키 유실 뒤 새 API 요청을 보내는 경우까지 검증한 것은 아니다. DynamoDB 시험도 **HTTP 관찰이 저장된 뒤의 연결 중단**과 **완료 AP의 정리 연결 실패**를 다룬다. 업체 호출 직후 HTTP 관찰 저장 전에 끊기는 경우, 전체 데이터 유실, DynamoDB TTL의 실제 만료 삭제는 별도 검증 범위다.

로컬 Redis 응답 중단 시험은 Sender를 잠시 pause해 명령을 대기시킨 다음 Redis를 pause하고 Sender를 재개한다. 두 컨테이너를 stop/start하면 Docker가 IP를 서로 바꿔 할당할 수 있고, 기존 AP가 캐시한 이전 Redis 주소로 재접속을 계속 시도하는 현상이 초기 시험에서 관찰됐다. 주소 변경과 응답 중단을 구분하기 위해 이 시험은 네트워크 주소를 유지한다. Redis 엔드포인트 주소 변경은 아래 `redis-dns-test`에서 따로 검증한다.

2026-10-08 검증 `runId=5eab0a20`의 네 시나리오가 통과했다. Redis 키 유실·정리 이후 명령을 총 4회 재발행해도 해당 메시지의 외부 호출은 1회였다. Redis 응답 중단 중 호출 0회·Sender lag 1, DynamoDB 중단 중 Sender/결과 lag 각 1·최종 이력 0건을 확인했다. 정리 연결 실패는 `PENDING`, 시도 1회, `SdkClientException`으로 기록됐고 과금 대상과 ORIGIN은 보존됐다.

최종적으로 외부 HTTP 호출·메시지 이력·과금 대상·고객 결과·원본 삭제가 각 4건, 관련 소비 lag가 0이었다. 완료 후 70초 동안 추가 발송은 없었다. 서비스·설정 복원 뒤 Prometheus 대상 17개와 Grafana 화면 7개를 포함한 관측 검증이 통과했고, 공통 검증 함수 변경 후 이전 웹훅 시험의 이력 101건·과금 51건·고객 결과 101건도 다시 대조했다.

추가 전체 health 점검에서 Redis를 사용하지 않는 웹훅 수신·결과 Manager·완료 Manager·고객 웹훅 Sender가 공통 모듈의 전이 의존성으로 `localhost:6379`에 연결하며 503을 반환하는 문제를 발견했다. 네 AP에서 미사용 Redis 의존성을 제외하고 실행 JAR에서도 제거됐는지 확인했다. 관련 Java 테스트 116개와 수정 후 정상·최종 실패·TCP 전환의 세 경로가 통과했다.

`verify`는 이제 mock 컨테이너의 내부 네트워크에서 AP 11개의 `/actuator/health`를 조회한다. 접수 API는 로컬 JWT로 인증한다. 하나라도 전체 상태가 `UP`이 아니면 검증에 실패하고 `.monitoring/verification.json`의 `applicationHealth`에 기록한다. 수정 후 AP 11개 전체가 `UP`이며 Prometheus 대상 17개·Grafana 화면 7개를 포함한 관측 검증이 통과했다. 지표 수집 가능 여부와 AP 전체 health를 함께 확인해야 한다.

## Redis 주소 변경 후 AP 재시작 없는 복구

```bash
bash scripts/monitoring.sh redis-dns-test
```

기동된 격리 모니터링 스택에서 단독 실행한다. 먼저 정상·최종 실패·TCP 전환 3건을 완료해 health용 비동기 연결과 업무용 동기 Redis 연결을 만든다. Redis 컨테이너의 데이터는 유지한 채 해당 프로젝트 네트워크의 빈 IPv4 주소로 옮기고 서버의 기존 일반 클라이언트 연결을 종료한다. 호스트명 `redis`와 별칭은 유지한다. AP 전체 health와 AP별 Redis 연결 수가 회복된 뒤 같은 세 경로를 다시 확인한다. AP 시작 시각을 비교해 재시작이 없었는지 검증하고, `finally`에서 원래 IP·별칭을 복원한 뒤 다시 연결 회복을 확인한다. 메시지 6건의 ORIGIN 삭제는 DynamoDB 일관 읽기로 직접 대조한다. 결과는 `.monitoring/redis-dns-latest.json`에 남긴다. 다른 장애 시험·`up`·트래픽 생성과 함께 실행하지 않는다.

주소 변경 지연에는 두 원인이 있었다.

- Lettuce 7.5.2의 기본 Netty DNS 캐시는 DNS 응답 TTL을 유지한다. TTL 600초의 로컬 DNS 재현에서 서버 주소를 바꿔도 이전 IP를 반환했고 질의 횟수는 1회였다. IntelliJ 로그포인트로 반환값과 서버 주소를 확인했다.
- DNS 캐시만 제한한 첫 실제 IP 변경 시험 `1204522b`는 90초 내에 복구되지 않았다. Redis AP 6개가 명령 시간 초과를 반복했고 새 Redis 주소에 업무 연결이 생기지 않았다. 원래 IP 복원 후에는 13.8초에 전체 health가 회복됐다. 별도 재현 테스트의 로그포인트에서 명령 timeout 이후 `openAfterTimeout=true`, `tcpUserTimeout=false`, `NioSocketChannel`을 확인했다. 명령 timeout만으로 기존 TCP 연결이 닫히지는 않았다.

공통 `RedisConnectionAutoConfiguration`은 주소·CNAME·권한 DNS 서버 캐시의 최대 TTL을 기본 5초로 제한하고 실패한 DNS 조회는 캐시하지 않는다. Linux에서는 미확인 TCP 전송을 제한하는 `TCP_USER_TIMEOUT`을 기본 10초로 적용한다. 설정은 각각 `messaging.redis.dns.max-ttl-seconds`, `messaging.redis.tcp.user-timeout-seconds`이며 1~300초를 허용한다. 기존 접속 timeout 등 소켓 설정은 유지한다.

TCP 제한을 실제 적용하기 위해 Redis를 사용하는 접수 API·발송 전 준비·HTTP Sender·TCP Sender·참조 캐시 AP에 Linux ARM64와 x86_64용 Netty epoll 라이브러리를 포함했다. Redis가 없는 AP에는 추가하지 않았다. macOS의 NIO 실행에는 이 Linux TCP 제한이 적용되지 않는다. 직접 `ClientResources` 빈을 제공하면 공통 자동 설정이 적용되지 않으므로 해당 빈과 클라이언트 설정에서 연결 정책을 관리해야 한다. 네이티브 전송과 TCP 제한의 적용 조건은 [Lettuce 운영 안내](https://redis.io/docs/latest/develop/clients/lettuce/produsage/)를 참고한다.

2026-10-09 검증 `runId=690bce4b`가 Linux ARM64에서 통과했다. x86_64는 네이티브 라이브러리의 실행 JAR 포함까지 확인했으며 이 환경에서 실행 검증한 것은 아니다.

| 확인 항목 | 결과 |
|---|---|
| Redis 주소 | `10.254.252.18` → `10.254.252.254` → `10.254.252.18` |
| 새 주소에서 연결 회복 | 20.38초, Redis AP 6개의 연결 10개·전체 AP health 11개 정상 |
| 원래 주소 복원 후 회복 | 18.67초, 동일 연결 수·health 정상 |
| AP 재시작 | 없음 |
| 변경 전·후 메시지 | 각각 정상·최종 실패·TCP 전환 3건 완료 |
| 최종 이력·과금 대상·고객 결과·ORIGIN 삭제 | 6건·2건·6건·6건 |
| 후속 관측 검증 | AP health 11개, Prometheus 대상 17개, Grafana 화면 7개 정상 |

회복 시간은 주소 이동 명령이 끝난 뒤 health와 연결 수를 함께 관측한 값이다. DNS 5초와 TCP 제한 10초를 전체 복구 시간의 보장으로 해석하지 않는다. Redis 전체 데이터 유실, 복제 failover, Sentinel/Cluster 전환, 목표 처리량은 검증 범위에 포함하지 않는다.

전체 Java 테스트 212개가 실패·생략 없이 통과했다. DNS 주소 갱신, 명령 timeout 이후 연결 유지 재현, TCP 제한 적용 시 기존 연결 설정 보존, 잘못된 설정 거부와 기존 PostgreSQL·Redis 회귀 테스트를 포함한다.

앞서 JAR 반영 중 발생한 Docker Kafka OOM 종료와 Desktop 종료 대기는 사용자 승인 후 Docker를 다시 기동해 복구했다. 기존 `rcs` 컨테이너 6개를 재기동하고 메시징 AP를 새 JAR로 갱신했다. 현재 위의 IP 변경·원복·전체 관측 검증까지 완료했으며, 기동 문제로 통합 시험이 보류됐던 상태는 해소됐다.

## 미사용 Docker 리소스 정리

2026-10-09 사용자 요청에 따라 다음 리소스를 제거했다. 상세 목록은 로컬 `.monitoring/docker-cleanup-latest.json`에 남긴다.

| 대상 | 제거 수 | 확인 기준 |
|---|---:|---|
| 구형 컨테이너 | 156 | Compose 작업 경로가 이 저장소이고 현재 모니터링 프로젝트가 아닌 종료·생성 상태의 컨테이너 |
| 볼륨 | 607 | 위 컨테이너의 전용 볼륨, 과거 시험 프로젝트 라벨의 미참조 볼륨, 모든 컨테이너에서 참조가 사라진 익명 볼륨 |
| 네트워크 | 29 | 구형 프로젝트 소유이며 연결된 컨테이너가 없음 |
| 이미지 | 1 | 현재 컨테이너가 사용하지 않는 `platform-monitoring-collector` |

삭제 전 현재·중지 컨테이너의 볼륨 참조를 대조했고 강제 제거 옵션은 사용하지 않았다. 마지막 확인에서 볼륨은 22개 모두 참조 중이며 미참조 볼륨은 0개였다. 남은 볼륨 크기는 약 2.925GB다. 현재 모니터링 컨테이너와 기존 `rcs` 컨테이너 6개는 실행 중이다. 프로젝트 소유를 확인하지 못한 기존 `redis`, `mongodb`, `postgres-batch` 중지 컨테이너 3개와 그 참조 볼륨은 유지했다. 공유 이미지와 빌드 캐시도 이번 구형 프로젝트 제거에 포함하지 않았다.

재발 방지를 위해 `scripts/test-java.sh integration`의 종료 정리를 `down --timeout 20 --volumes`로 변경했다. 실행마다 고유한 `java-test-<시각>-<PID>` 프로젝트만 대상으로 하며, 성공·실패 모두 이름 있는 볼륨과 익명 볼륨을 제거한다. `.test-results`의 로그·결과 파일은 유지한다. 실제 정리 함수를 별도 임시 프로젝트에서 실행해 종료 코드 0과 7이 각각 유지되고 컨테이너·네트워크·볼륨 2개씩이 제거되는지 확인했다. 이 정리 검증은 전체 Java 통합 실행기를 다시 실행한 것은 아니다. 결과는 `.monitoring/java-cleanup-verification.json`에 있다.
