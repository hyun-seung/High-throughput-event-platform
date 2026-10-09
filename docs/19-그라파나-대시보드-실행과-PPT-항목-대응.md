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

기동된 격리 모니터링 스택에서 단독 실행한다. Redis 컨테이너의 데이터는 유지한 채 해당 프로젝트 네트워크의 빈 IPv4 주소로 옮기고 기존 일반 클라이언트 연결을 종료한다. 호스트명 `redis`와 별칭은 유지한다. AP를 재시작하지 않고 전체 health 11개가 회복되는지, 정상·최종 실패·TCP 전환 3건의 이력·과금 대상·고객 웹훅·원본 정리가 완료되는지 확인한다. `finally`에서 원래 IP와 별칭을 복원하고 health 회복까지 기다린다. 결과와 복구 관측 시간은 `.monitoring/redis-dns-latest.json`에 남긴다. 다른 장애 시험·`up`·트래픽 생성과 함께 실행하지 않는다.

원인은 사용 중인 Lettuce 7.5.2의 Netty DNS 캐시가 DNS 응답 TTL을 그대로 유지하는 데 있었다. TTL 600초의 로컬 DNS 재현에서 서버 주소를 `10.20.0.1`에서 `10.20.0.2`로 바꾼 뒤에도 동일 resolver가 이전 IP를 반환했고 DNS 질의 횟수는 1회였다. IntelliJ 로그포인트로 실제 반환값과 서버 주소를 확인했다.

공통 Redis 자동 설정은 주소·CNAME·권한 DNS 서버 캐시의 최대 TTL을 기본 5초로 제한하고 실패한 DNS 조회는 캐시하지 않는다. `messaging.redis.dns.max-ttl-seconds`로 1~300초 범위에서 조정할 수 있다. Lettuce의 네이티브 전송 선택은 유지하며 Redis 의존성이 없는 AP에는 적용되지 않는다. 직접 `ClientResources` 빈을 제공하는 경우에는 해당 빈의 DNS 정책이 우선한다. 캐시 5초는 연결 복구 보장 시간이 아니며 연결 단절 감지·접속 timeout·재시도 간격이 추가될 수 있다. Redis 전체 데이터 유실·복제 failover·Sentinel/Cluster 전환 시험은 아니다.

2026-10-09 코드 검증에서는 전체 Java 테스트 209개가 실패·생략 없이 통과했다. DNS 회귀 테스트는 긴 TTL의 이전 주소 유지, 제한 TTL 이후 같은 resolver의 새 주소 조회, 잘못된 TTL 설정 거부를 확인했다. PostgreSQL 이력·과금·웹훅 테스트와 Redis 사용량 제한 테스트도 포함한다.

**실제 IP 변경 통합 시험은 아직 미완료다.** 새 JAR 반영 중 Docker VM의 Kafka가 `OOMKilled=true`, 종료 코드 137로 중단됐다. Kafka는 재기동 후 healthy로 돌아왔지만 `db-init`과 새 API 컨테이너의 시작 요청이 계속 대기했다. Compose 명령을 중단하고 AP만 `--no-deps`로 기동해도 API 시작이 완료되지 않았다. API는 재생성됐으나 기동을 확인하지 못했고 다른 AP는 이전 JAR로 실행 중이다. Redis IP 변경 명령은 실행하지 않아 원래 주소 `10.254.252.2`를 유지한다. 다른 프로젝트 컨테이너와 볼륨은 변경하지 않았다. Docker 기동 문제를 복구한 뒤 `up`, `redis-dns-test`, `verify`를 순서대로 다시 실행해야 하며, 위의 과거 전체 health 성공 결과를 현재 상태의 검증으로 해석하면 안 된다.

같은 날 후속 복구에서 `docker desktop restart --timeout 180`도 종료 단계의 시간 초과로 실패했다. 이후 Desktop 상태는 `stopping`이며 `docker ps` 조회도 15초 내에 응답하지 않았다. 현재 AP와 다른 프로젝트의 실행 상태를 확인할 수 없으므로 정상 가동으로 간주하지 않는다. 재시작 전 실행 목록은 메시징 컨테이너 22개와 `rcs` 컨테이너 6개였다. 강제 종료·재기동은 아직 수행하지 않았으며 전체 프로젝트에 영향을 주는 복구 작업이다. 복구 후 기존 실행 목록을 대조하고, 메시징 AP 갱신 및 위의 두 검증 명령을 완료해야 한다. Redis IP 변경 시험과 복구 시간 측정은 여전히 미실행 상태다.
