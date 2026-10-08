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
