# 메인 메시징 경로 모니터링 실행

이 문서는 현재 `/api/v1/messages`부터 통신사별 1차 HTTP 발송, 업체 웹훅, 결과 판단, 완료 이력, 고객 웹훅까지 실행하는 로컬 관측 환경을 설명한다. TCP 2차 발송기도 함께 실행한다.

## 실행과 확인

Docker Desktop과 JDK 21, Python 3.10 이상이 필요하다. 저장소 루트에서 실행한다.

```bash
./mvnw package
bash scripts/monitoring.sh up
bash scripts/monitoring.sh status
bash scripts/monitoring.sh demo --rate 1 --seconds 1
bash scripts/monitoring.sh verify
```

`demo`는 정상 메시지를 `/api/v1/messages`로 넣고 SQL 최종 이력, 1차 과금 대상 기록, 고객 웹훅 전달까지 기다린다. `--rate`는 초당 1~100건, `--seconds`는 1~120초다. `--errors`를 주면 1차 HTTP 실패 1건도 제출해 실패 이력과 고객 웹훅을 확인한다. 결과 JSON의 `exampleClientMsgId`로 로그를 검색할 수 있다. `verify`는 10개 AP, Kafka 관측기, Redis·PostgreSQL exporter의 Prometheus 수집 상태와 Grafana 대시보드 7개를 확인하고 `.monitoring/verification.json`에 결과를 저장한다. 데모의 업무 결과 검증과 관측 연결 검증은 서로 다른 검사다.

```bash
bash scripts/monitoring.sh demo --rate 5 --seconds 10 --errors
bash scripts/monitoring.sh logs result-manager
bash scripts/monitoring.sh stop
```

`stop`은 컨테이너만 정지하고 데이터 볼륨은 유지한다. JAR를 수정했다면 빌드 후 `up`을 다시 실행한다. 스크립트는 실행 JAR를 `.monitoring/jars/<SHA256>/`에 복사하므로 실행 중 빌드가 컨테이너의 파일을 바꾸지 않는다.

`up`은 AP를 순서대로 띄우며 각 관리 지표가 수집될 때까지 기다린다. 로컬 Compose에서는 JVM 메모리와 Kafka 소비자 병렬도를 낮춰 기동한다. 실제 처리량 측정에는 이 로컬 설정을 그대로 사용하지 않는다.

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
