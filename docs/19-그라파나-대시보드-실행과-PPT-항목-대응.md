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

`verify`의 구조화 로그 존재 확인은 최근 24시간을 조회한다. 접수 API와 발행 복구 AP가 조용한 동안 로그를 계속 생성한다고 가정하지 않는다. `.monitoring/verification.json`의 `logEvidence`에 마지막 로그 나이와 최근 1시간 기록 여부를 남기며 AP health·Prometheus 수집 상태는 별도로 검사한다. 오래된 로그를 찾은 것만으로 현재 로그 전송 지연까지 검증한 것은 아니다. 24시간 내 기록이 전혀 없거나 조회할 수 없으면 로그 증거 부족으로 검증에 실패한다.

## 성능 측정 도구

```bash
# 정상 경로의 낮은 부하
bash scripts/monitoring.sh performance --rate 5 --seconds 20
# 정상 80%·1차 최종 실패 10%·HTTP 실패 후 TCP 성공 10%
bash scripts/monitoring.sh performance --rate 5 --seconds 20 --failure-percent 10 --secondary-percent 10
# 집계 계산 회귀 검사
python3 -m unittest discover -s scripts/monitoring -p test_performance_flow.py
```

기동된 격리 모니터링 스택에서 단독 실행한다. AP 전체 health와 주요 소비 그룹 8개의 lag 0, AP 11개의 CPU·힙·수집 상태를 확인한 뒤 요청을 일정 간격으로 보낸다. 대기열을 무제한 늘리지 않고 `--workers`(기본 32)만큼만 요청을 진행한다. 송신 슬롯이 없거나 예정 시각보다 `max(50ms, 요청 간격)` 이상 늦으면 해당 요청을 보내지 않고 원인을 기록한다. 애플리케이션의 TPS·Quota 정책은 변경하지 않는다.

`--rate`는 1~1000, `--seconds`는 10~600이며 한 실행은 최대 100,000건이다. 이 범위는 도구의 입력 제한이지 서비스 처리량 보장이 아니다. 혼합 비율은 100건 단위의 고정 순서로 적용하고 실제 접수된 경로별 건수도 기록한다. 기본 HTTP 대기는 10초, 인입 종료 후 처리 대기는 180초이며 `--request-timeout`·`--timeout`으로 조절한다. 각 요청은 한 번만 보내고 자동 재접수하지 않는다.

| 측정 항목 | 의미 |
|---|---|
| 목표·실제 TPS | 설정한 예정 요청 수와 실제 HTTP 시작·202 접수·완료 건수를 분리. 부하 시간 내 TPS와 남은 처리까지 포함한 전체 구간 평균 완료 TPS를 각각 기록 |
| 접수 지연 | 클라이언트의 HTTP 시작~응답을 monotonic 시계로 측정. 전체 응답과 202 접수의 p50/p95/p99·최대, 예정 시각 대비 실제 시작 지연을 별도로 기록 |
| 최종 완료 지연 | 서버 `received_at`부터 최종 판단·이력 기록·고객 통지 확인 기록·DynamoDB 정리 기록까지 각각 측정. 최종 완료는 `max(recorded_at, outbox.updated_at, cleaned_at)` 기준 |
| 오류 | HTTP 상태별 건수·접수 오류율·발행 여부가 불명확한 응답·부하 도구 자체 미발송을 구분. 예상된 업체 실패와 TCP 전환은 접수 오류로 세지 않음 |
| 적체 | Prometheus에서 수집한 그룹/토픽별 Kafka lag와 최대 합계. 결과 대조 후 새 collector 표본에서 lag 0을 확인한 부하 종료 후 시간도 기록 |
| 자원 | AP별 CPU 비율·JVM 힙 사용량의 관측 최대값, Docker CPU·메모리 할당, 실행 JAR 경로/해시·측정 소스 해시·Git 상태 |

5초마다 Prometheus를 조회하며 실제 scrape·Kafka 관측기 주기는 각각 10초이므로 표본 사이의 순간 피크는 놓칠 수 있다. CPU 비율은 JVM 지표이며 힙은 컨테이너 전체 메모리/RSS가 아니다. `lagZeroObservedAfterLoadSeconds`는 결과 대조·수집 지연이 포함된 **lag 0 확인 시간의 상한 관측값**으로, 실제 적체가 사라진 정확한 시각이 아니다. DB의 기록 시각도 트랜잭션 commit 완료 시각과 완전히 같지는 않다. 서버와 부하 도구의 시계 차이 추정치·불확실성을 함께 남긴다.

보고서는 `.monitoring/performance-latest.json`, 실행별 원본은 `.monitoring/performance/<runId>/`의 `report.json`, `requests.jsonl`, `results.json`, `telemetry.jsonl`, `jars.env`다. 요청 시간·실제 메시지 ID, 1초별 건수, 원래 표본을 보존하므로 집계를 다시 대조할 수 있다. 네트워크 타임아웃·5xx는 미접수 확정이 아니며 `unconfirmedAdmissions`에 포함한다. 이런 실행은 실패로 표시하고 같은 실행 ID의 늦은 이력을 추가 확인해야 한다.

**`pass=true`는 성능 SLA 합격이 아니라 측정·데이터 대조 성공**이다. 예정한 요청이 모두 접수되고 동일한 메시지 ID·예상 결과·과금 대상·고객 통지·정리 상태가 맞아야 한다. ORIGIN 삭제를 직접 확인하고, 필수 지표 누락·부하 도구 미발송·접수 오류가 있으면 성공으로 표시하지 않는다. `capacityOrSlaVerdict`는 아직 `not-evaluated`다. 단계별 처리량 한계와 지속 부하 결과는 별도로 측정한다.

2026-10-10 최종 도구 검증 `runId=c8755276`은 5 TPS·20초의 혼합 100건을 모두 접수했다. 정상 1차 성공 80건·1차 실패 10건·TCP 성공 10건의 최종 이력·고객 통지·원본 삭제 100건과 과금 대상 80건이 일치했다. HTTP 오류와 도구 미발송은 0건이었다. 접수 p95/p99는 230.576/595.039ms, 정리까지 포함한 전체 완료 p95/p99는 21.014/22.766초였다. 부하 시간 내 완료 TPS는 1.30, 남은 처리까지 포함한 31.714초 구간의 평균 완료 TPS는 3.153이었다. 이는 시작·종료 효과를 포함한 짧은 실행이며 지속 처리량이 아니다. 필수 지표 10개 표본 모두 유효했고, 관측 최대 Kafka lag 합계는 4였다. 집계 회귀 검사 10개가 통과했다.

### 단계별 혼합 부하 기준선 — 2026-10-10

동일한 실행 JAR·설정과 깨끗한 작업 트리(`969516c`)에서 정상 80%·1차 최종 실패 10%·TCP 성공 10%로 측정했다. 각 단계는 60초 동안 요청하고 전체 완료·원본 삭제·새 Kafka lag 0 표본을 확인한 뒤 다음 단계를 시작했다. 실제 실행 순서는 **5 → 20 → 10 TPS**다. 20 TPS에서 지연이 크게 늘어 계획했던 50 TPS 증가는 생략하고 10 TPS로 범위를 좁혔다. 단계마다 한 번 측정했으며 별도 워밍업·반복 측정은 하지 않았다.

```bash
bash scripts/monitoring.sh diagnose
bash scripts/monitoring.sh performance --rate 5 --seconds 60 --failure-percent 10 --secondary-percent 10
bash scripts/monitoring.sh performance --rate 20 --seconds 60 --failure-percent 10 --secondary-percent 10
bash scripts/monitoring.sh performance --rate 10 --seconds 60 --failure-percent 10 --secondary-percent 10
bash scripts/monitoring.sh verify
```

Docker 할당은 10 CPU·7.75 GiB, 실행 컨테이너는 다른 프로젝트를 포함해 29개였다. AP는 소비 병렬도 1, `ActiveProcessorCount=2`, 최대 JVM 힙 320MiB의 로컬 설정을 유지했다. 동일 고객 1명·동일 수신 번호의 SKT 경로를 사용하고 실패 사례 일부만 TCP로 전환했다. 따라서 3사 균등 분산이나 다수 고객 부하는 검증하지 않았다. 업체 성공 웹훅은 mock이 약 0.5초 뒤 보내며 실제 업체 지연 분포와 다르다.

| 목표 TPS / runId | 접수·완료 / 과금 대상 | 접수 p95 / p99 | 전체 완료 p95 / p99 | 관측 최대 lag 합계 |
|---|---:|---:|---:|---:|
| 5 / `5bf22442` | 300 / 240 | 39.455 / 65.754ms | 15.533 / 17.919초 | 5 |
| 10 / `5670ef58` | 600 / 480 | 39.092 / 80.253ms | 25.310 / 28.732초 | 1 |
| 20 / `3d8b459a` | 1,200 / 960 | 152.486 / 371.908ms | 102.815 / 110.566초 | 256 |

총 2,100건 모두 접수·최종 이력·고객 수신 확인·ORIGIN 삭제를 확인했고 과금 대상은 1,680건이었다. HTTP 오류·발행 여부 불명확 응답·도구 미발송은 모두 0건이었다. 필수 지표 표본은 각각 18·19·35개로 모두 유효했다. 시험 후 AP 11개 health, Prometheus 대상 17개, Grafana 대시보드 7개와 로그 조회가 통과했다. 이 표의 완료는 고객 통지뿐 아니라 DynamoDB 정리 기록까지 포함한다.

| 목표 TPS | 부하 60초 내 완료 TPS | 남은 처리 포함 평균 완료 TPS / 구간 | 인입→판단 p95 | 판단→이력 p95 | 이력→정리 p95 |
|---|---:|---:|---:|---:|---:|
| 5 | 4.167 | 4.200 / 71.432초 | 6.295초 | 5.821초 | 4.765초 |
| 10 | 6.383 | 7.952 / 75.456초 | 10.085초 | 7.225초 | 10.284초 |
| 20 | 3.050 | 7.661 / 156.636초 | 50.951초 | 17.672초 | 45.953초 |

평균 완료 TPS는 시작·종료 효과를 포함하므로 지속 처리 한계가 아니다. 20 TPS에서 60초 내 1,198건이 202를 받았고 나머지 2건의 응답은 60초 직후 도착했다. 전체 1,200건은 모두 접수됐다. 구간별 p95는 각 메시지의 두 시각 차이를 계산한 값이며 서로 더해 전체 p95를 구할 수 없다.

20 TPS의 그룹별 최대 lag는 PRE-SEND-MANAGER 207, SKT Sender 52, 결과 관리자 11이었다. 그룹별 최대값은 서로 다른 시각의 값이므로 합계 피크 256과 일치할 필요가 없다. 완료 토픽 lag는 최대 1이었지만 이력→정리 p95는 약 46초였다. **Kafka lag만으로 완료 지연을 판단할 수 없으며 DB에 남은 후속 처리도 함께 봐야 한다.** 10 TPS도 전체 완료 p95가 약 25초여서 성능 합격으로 보지 않는다.

다음 진단은 PRE-SEND-MANAGER의 메시지당 처리 시간과 저장소 대기, 결과 예약 발행과 완료 정리 작업의 배치 처리 시간·대기 건수를 우선 확인한다. 코드 설정에는 결과 outbox와 완료 cleanup의 5초 조회 주기·100건 페이지가 있지만, 이번 표본만으로 특정 설정을 원인으로 확정하지 않는다. AP CPU·힙 표본과 시험 전후 VM pressure만으로 부하 중 저장소 I/O·스왑 영향을 배제할 수도 없다. 원인 증거를 확보한 뒤 한 번에 한 요소를 수정하고 같은 10/20 TPS에서 비교하는 것이 다음 작업이다.

원본은 `.monitoring/performance/<runId>/`에 보존했다. 비교 집계는 `.monitoring/performance-baseline/summary.json`, 실행 로그와 전후 자원 진단은 같은 `performance-baseline/` 디렉터리에 있다. 로컬 산출물은 Git에서 제외되며 위 표와 재현 명령은 이 문서로 관리한다. 운영 SLA·최대 처리량은 미판정이며 30~60분 지속 시험, 순간 증가, 부하 중 장애 복구는 아직 남아 있다.

### 완료 정리 지연 개선 — 2026-10-10

20 TPS를 기존 JAR로 다시 측정한 `600cc1dd`에서도 전체 완료 p95 100.464초, 이력→정리 p95 48.178초로 지연이 재현됐다. STEP 개별 삭제를 배치로 묶은 중간 버전 `ec570c91`은 전체 완료 p95 101.543초여서 **배치 삭제만으로는 완료 지연 개선을 확인하지 못했다.**

이어 완료 관리자만 잠시 정지해 실제 메시지 210건의 이력을 쌓고 IntelliJ 디버거에서 정리 페이지의 시작·종료를 관찰했다. SQL `PENDING=210`에서 `100 → 100 → 10`건을 처리했고, 100건 처리에 약 3.6~4.4초가 걸린 뒤 대기가 남아 있어도 다음 페이지까지 약 5초 쉬었다. 이 진단은 호스트 JVM의 제어된 실행으로, Docker 성능 비교 수치에는 포함하지 않는다. 진단 210건도 이력·과금 대상·고객 수신 확인·ORIGIN 삭제를 모두 확인하고 컨테이너와 디버거 상태를 복원했다.

최종 변경은 기존 완료 AP 안에서 가득 찬 페이지를 최대 4개까지 연속 처리하고, STEP 삭제를 최대 25건의 배치로 묶는 것이다. 병렬 작업자를 늘리지 않으며 덜 찬 페이지에서는 종료한다. TTL 설정과 모든 STEP 삭제 확인 후 ORIGIN을 삭제하는 순서를 유지한다. 배치 미처리 항목은 SQL 정리 대기로 남겨 기존 지연 재시도로 처리한다. 설정과 운영 권한은 [완료 처리 계약](53-신규-메시지-케이스별-Call-Flow.md)에 기록했다.

| 20 TPS·60초 혼합 부하 | 기존 `600cc1dd` | 배치 삭제만 `ec570c91` | 최종 `724ffb85` |
|---|---:|---:|---:|
| 접수·완료 / 과금 대상 | 1,200 / 960 | 1,200 / 960 | 1,200 / 960 |
| 접수 p95 | 145.689ms | 75.412ms | 58.708ms |
| 인입→판단 p95 | 43.938초 | 41.184초 | 38.970초 |
| 인입→이력 p95 | 56.464초 | 56.703초 | 56.505초 |
| 이력→정리 p95 | 48.178초 | 54.004초 | 21.761초 |
| 전체 완료 p95 | 100.464초 | 101.543초 | 74.871초 |
| 남은 처리 포함 평균 완료 TPS | 8.004 | 7.675 | 10.203 |
| 관측 최대 lag 합계 | 125 | 88 | 45 |

세 실행 모두 접수 오류·도구 미발송 없이 최종 상태·과금 대상·고객 통지·원본 삭제 대조를 통과했다. 동일한 20 TPS 조건에서 전체 완료 p95가 약 25%, 이력 이후 정리 p95가 약 55% 감소했다. 완료 관리자 JAR만 교체했고 다른 AP JAR·소비 병렬도·업무 정책·측정 명령은 유지했다. 각 버전을 한 번씩 순차 측정한 개발 환경 결과라 JVM 워밍업·공유 저장소 변동을 통제한 통계적 효과 추정이나 운영 SLA 보장은 아니다. 인입→이력 p95는 여전히 약 56초로 남아 있어 발송 전 준비와 결과 인계 경로의 저장소 대기·예약 발행을 다음 진단 대상으로 둔다.

최종 버전의 10 TPS·60초 확인 `d4b6ac2e`도 600건 완료·과금 대상 480건으로 통과했다. 접수 p95는 44.544ms, 전체 완료 p95는 27.500초였다. 앞선 기준선의 25.310초보다 높아 이 부하에서는 개선으로 보지 않는다. 최종 버전의 두 부하 실행 합계 1,800건이 모두 완료됐고, 처리 한계나 장시간 안정성을 판정한 것은 아니다.

원본 보고서는 `.monitoring/performance/<runId>/`, 비교 집계 `comparison.json`과 빌드·검증 로그, 디버거 관측 증거는 `.monitoring/cleanup-performance/`에 있다. 완료 관리자 단위 테스트 13개와 PostgreSQL 고유 임시 스키마 테스트 7개가 통과했다. 페이지 처리 상한, 부분 배치 실패, 재시도, TTL 보호와 ORIGIN 삭제 순서를 검사했다. 추가 AP·Maven 모듈은 없다.

최종 JAR의 저장소 장애 재검증 `storage-test`, `runId=d4a4080b`도 통과했다. Redis 발송 키 유실·Redis 중단·DynamoDB 중단·완료 정리 연결 실패의 네 사례에서 이력·과금 대상·고객 결과·업체 호출·ORIGIN 삭제가 각각 4건이었다. 정리 연결 실패 중 SQL은 `PENDING`과 재시도 정보를 보존했고 복구 후 완료됐다. 추가 70초 동안 중복 발송 없이 소비 lag 0을 확인했다. 이어 `verify`에서 AP 11개 health, Prometheus 대상 17개, Grafana 7개와 로그 조회가 통과했다.

### 완료 이력·과금 대상의 SQL 배치 저장 — 2026-10-10

과금 대상 판단은 메시지별로 유지하고, 완료 토픽의 한 poll에서 받은 최대 100건을 한 SQL 트랜잭션으로 저장한다. Kafka 레코드는 메시지당 한 건 그대로다. `TBL_MSG_HIST`는 전체 메시지, `TBL_CDR_HIST`는 그중 1차 성공만 JDBC batch로 INSERT한다. 기존 행의 내용 대조도 목록 단위로 수행한다. 예를 들어 1차 성공 80건·실패 10건·TCP 성공 10건이면 같은 트랜잭션에 이력 100건·과금 대상 80건을 기록한다. 한 건이라도 오류·충돌이 있으면 전체 rollback하며, commit이 끝난 뒤 Kafka offset을 반영한다.

100건이 모일 때까지 기다리지는 않는다. 브로커 fetch는 16KiB 또는 최대 25ms를 기준으로 응답하고 소비자는 받은 만큼 저장한다. 이 지연 상한은 배치를 만들기 위한 브로커 대기만 가리킨다. DynamoDB 정리는 commit된 SQL 대기 행을 읽는 별도 작업으로 유지해, 저장소 정리가 느려도 소비 스레드의 이력·과금 저장을 직접 막지 않도록 한다. 판단·저장·정리는 기존 완료 AP 안에 있으며 새 AP·테이블·토픽은 추가하지 않았다.

실제 PostgreSQL·Kafka를 사용하는 테스트를 포함해 완료 AP 테스트 31개가 통과했다. 혼합 100건·배치 내 중복·상충 결과·과금 INSERT 실패·역순으로 겹친 동시 배치·빈 입력·상한 초과를 확인했다. Kafka 시험에서는 과금 INSERT를 실패시켜 이력/과금 0건과 offset 0을 확인하고, DB commit 직후 offset 반영을 막은 상태에서 소비자를 재시작했다. 재전달 후에도 이력·과금은 각각 100건이고 offset만 100으로 진행했다. 이후 별도 1건도 정상 처리해 100건 충족을 기다리지 않음을 확인했다. 시험용 토픽·소비 그룹·SQL 스키마는 제거했다.

동일한 저장 구현을 `storeBatch(1건)`으로 100회 호출한 경우와 `storeBatch(100건)`으로 한 번 호출한 경우를 워밍업 후 순서를 바꿔 세 번 측정했다. 100건 모두 1차 성공이며 테이블·고유키·내용 검증 조건은 같다. 최종 실행의 단건 방식은 **197.073~224.993ms**, 100건 배치는 **14.966~17.287ms**였다. 이는 호스트 JVM·로컬 PostgreSQL의 SQL 저장 구간 비교이며 예전 구현과의 직접 비교나 전체 서비스 TPS 개선율이 아니다. 시간에 대한 합격 임계값을 테스트에 넣지 않았고, 실제 행 수·중복 방지·트랜잭션 수를 검사했다.

변경 전 전체 경로 재측정 `5eb1761d`는 요청 생성기 미발송 7건과 제한 시간 내 새 lag 0 표본 미확인으로 실패 표시됐다. 접수된 1,193건은 모두 완료됐고 과금 대상 954건·고객 통지·원본 삭제가 일치했다. 당시 Docker VM의 I/O pressure와 스왑 활동이 관측됐으며 원인 전체를 확정한 것은 아니다. 이 실행은 변경 전후 성능 개선율 산출에서 제외한다.

변경 후 `45993c7e`는 20 TPS·60초, 정상 80%·1차 실패 10%·TCP 성공 10%의 1,200건을 오류·미발송 없이 접수하고 모두 완료했다. 이력·고객 통지·원본 삭제 1,200건, 과금 대상 960건이 일치했다. 인입 후 대조 제한 시간은 기존 180초에서 300초로 늘려 검증했으며 업무 정책은 바꾸지 않았다. 접수 p95는 110.139ms, 인입→이력 p95는 85.312초, 전체 완료 p95는 106.746초였다. **전체 서비스의 지연 문제가 해소됐다는 결과는 아니다.**

해당 실행의 실제 SQL commit은 1,153회, 처리 레코드는 1,200건으로 평균 1.041건/배치였고 관측 최대 크기는 3건이었다. DB 트랜잭션 시도 시간 합계는 4.814초였다. 현재 20 TPS 경로에서는 앞단에서 결과가 천천히 도착해 큰 배치가 거의 형성되지 않았다. 100건 집중 저장 시험의 이득을 이 실행의 전체 처리량 개선으로 환산하지 않는다. 배치는 밀린 결과를 효율적으로 저장할 준비이며, 발송 전 준비·결과 인계 구간의 지연 진단은 계속 필요하다.

별도 Docker 웹훅·장애 복구 시험 `3ed8dbf1`도 통과했다. PostgreSQL 중단 중 원본 100건과 완료 lag 100을 보존했고, 복구 후 선행 웹훅 1건을 포함한 이력·고객 통지·원본 삭제 101건 및 과금 대상 51건을 대조했다. 이 실행의 성공 SQL 배치는 3회·101건, 관측 최대 크기는 99건이었다. 늦은 중복·상충·미등록 웹훅을 처리한 뒤 70초 동안 추가 발송이 없었으며 완료 lag은 0이었다. 복구 후 모니터링 최종 검증도 통과했다.

Prometheus에는 다음 지표를 추가했다. 배치 크기 count/sum은 **성공적으로 commit된 소비 레코드 수를 재전달 포함** 집계하므로 고유 메시지 수나 과금 건수로 사용하지 않는다. DB duration은 실패한 트랜잭션 시도도 포함한다.

```promql
# 성공한 SQL 트랜잭션의 평균 배치 크기
rate(messaging_complete_sql_batch_size_sum{job="complete-manager"}[5m])
  / rate(messaging_complete_sql_batch_size_count{job="complete-manager"}[5m])
# 트랜잭션 한 번의 평균 DB 시간(ms)
1000 * rate(messaging_complete_sql_batch_duration_seconds_sum{job="complete-manager"}[5m])
  / rate(messaging_complete_sql_batch_duration_seconds_count{job="complete-manager"}[5m])
```

시험·빌드·SQL 시간 비교 로그는 `.monitoring/completion-batch/`, 전체 경로의 요청·결과는 실행별 `.monitoring/performance/<runId>/`에 보존한다. offset·재시도·영구 오류 처리 한계는 [완료 처리 계약](53-신규-메시지-케이스별-Call-Flow.md#81-msg-complete-manager의-처리-로직)에 있다.

### 발송 전 통신사·전문 확정의 저장소 호출 축소 — 2026-10-10

PRE-SEND-MANAGER의 신규 정상 처리에서 `PreSendDecisionStore → PreSendPreparation → InitialCarrierStore`를 디버거로 재현했다. 같은 원본을 각각 읽은 뒤 통신사와 전문을 따로 갱신하는 순서가 관측됐다. 이 디버거 재현은 실제 업무 클래스와 mock DynamoDB를 사용한 호출 경로 확인이며 저장소 지연 측정은 아니다.

첫 통신사와 `pre_send_dispatch`를 하나의 조건부 UpdateItem으로 묶었다. 정상 신규 건은 DynamoDB 조회 2회·갱신 2회에서 **조회 1회·갱신 1회**로 줄었고, 재전달은 조회 1회로 저장된 결과를 재사용한다. 기존 `InitialCarrierStore` 클래스와 별도 빈은 제거했다. 다른 소비자가 먼저 저장하면 그 결과를 사용하고, 만료가 먼저 확정되면 새 명령을 만들지 않는다. 구버전의 통신사만 저장된 원본도 유지하며, 준비 중 구버전이 다른 통신사를 고정한 경우 조건 실패로 재시도한다. Kafka 발행 확인 후 offset 반영과 실패 재처리 방식은 유지한다.

완료된 검증은 발송 전 AP 테스트 28개이며 실제 DynamoDB 조건 검증 6개를 포함한다. 통신사·전문의 동시 저장, 참조 데이터 변경 후 재전달, 8개 동시 소비의 서로 다른 통신사 선택, 구버전 원본 및 동시 갱신, 저장 직전 완료 전이, 계약 실패를 확인했다. 실제 DB 시험은 임의 이름의 전용 테이블로 격리하고 종료 시 제거한다. 기존 통합 시험 스크립트의 `DYNAMODB_TEST_ENDPOINT`를 사용한다.

```bash
# 실행 중인 로컬 DynamoDB의 전용 시험 테이블에서 검증
DYNAMODB_TEST_ENDPOINT=http://127.0.0.1:38000 \
  ./mvnw -pl messaging-pre-send-manager -am -Dtest='PreSend*Test' \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

변경 전 `e7c7d95b`와 변경 후 `6cda61ad`를 동일한 20 TPS·60초·정상 80%/1차 실패 10%/TCP 성공 10%·대조 대기 300초로 측정했다. 두 실행 모두 접수·이력·고객 통지·원본 정리 1,200건과 과금 대상 960건이 일치했고, 접수 오류·도구 미발송·추가 업체 호출은 없었다. 각 실행의 mock HTTP 수신도 1,200개 ID·1,200회였다. 최종 lag 0과 AP 전체 health·모니터링 검증도 통과했다. JAR 목록을 대조해 PRE-SEND만 교체했음을 확인했다.

| 관측 항목 | 변경 전 | 변경 후 |
|---|---:|---:|
| 접수 → 첫 업체 HTTP 수신 p95 | 27.195초 | 18.943초 |
| 업체 HTTP 수신 → 최종 판단 p95 | 49.903초 | 41.420초 |
| 최종 판단 → SQL 이력 p95 | 28.353초 | 22.250초 |
| SQL 이력 → DynamoDB 정리 p95 | 23.653초 | 23.364초 |
| 접수 → 전체 완료 p95 | 107.536초 | 81.356초 |
| PRE-SEND 소비 그룹 최대 lag | 371건 | 54건 |
| SKT Sender 소비 그룹 최대 lag | 89건 | 274건 |

접수→업체 HTTP에는 PRE-SEND뿐 아니라 Sender의 소비·저장소 대기도 포함된다. 각 구간 p95는 같은 메시지의 구간 차이를 계산한 분포이며 서로 더할 수 없다. 전후 각 1회인 로컬 관측으로, 저장소 호출 절반 감소를 전체 처리량 2배나 운영 SLA 달성으로 환산하지 않는다. 앞단 적체는 줄었지만 Sender 적체가 커졌고, 결과 판단 이후 이력 인계도 여전히 수십 초 걸렸다. **다음 범위는 HTTP Sender의 저장소 대기와 결과 Manager의 inbox/outbox 예약 처리 지연 진단**이다. 이번에는 그 구간의 병렬도·재시도·스케줄 정책을 바꾸지 않았다.

디버거 근거·테스트 로그·변경 전 JAR 목록과 전후 비교 자료는 `.monitoring/pre-send-performance/`에 보존한다. 업무 정책·메시지 규격·토픽·실행 AP 및 모듈 수는 바꾸지 않았다.

### 결과 Manager 예약 작업의 실행 대기 분리 — 2026-10-10

실제 `PendingResultDispatcher`와 `PrimaryDecisionOutboxDispatcher`를 등록한 Spring 컨텍스트로, inbox의 DynamoDB 조회를 보류했을 때 outbox가 진행하는지 재현했다. 기존 AP 설정을 읽은 디버거에서 스케줄러 풀 크기 1과 outbox 미진행을 확인했다. 이는 mock 저장소로 실행 순서를 통제한 재현이며 실제 DynamoDB의 응답 시간을 측정한 결과는 아니다.

결과 Manager의 `spring.task.scheduling.pool.size`를 5로 명시했다. inbox 결과 판단·outbox 발행·후속 HTTP 발행·1차 만료·선택적으로 활성화하는 만료 backfill, 총 5개 예약 작업이 다른 작업의 실행 종료를 기다리지 않도록 한다. 각 작업은 기존 fixed-delay와 동기화된 poll을 유지해 동일 작업을 중첩 실행하지 않는다. 기본 5초 조회 간격·1분 후 재시도·최초 이후 3회 재시도·만료 정책과 DynamoDB 조건부 전이는 유지했다. AP·모듈·토픽 추가는 없다.

공통 31개·결과 Manager 70개, 총 101개 Java 테스트가 통과했다. 새 Spring 컨텍스트 시험은 운영 YAML과 실제 inbox/outbox 예약 메서드를 사용해, 한쪽 저장소 조회가 기다리는 동안 다른 쪽이 진행하는지를 양방향으로 확인한다. 같은 poll의 중첩 실행도 발생하지 않았다. 스레드 풀은 AP 내부 실행 대기를 분리하며 공유 DynamoDB·Kafka의 자원 경합까지 제거하지는 않는다.

변경 전 `8a73646c`와 변경 후 `791e43bb`를 20 TPS·60초·정상 80%/1차 실패 10%/TCP 성공 10%·대조 대기 300초로 측정했다. 두 실행 모두 1,200건 접수·이력·고객 통지·원본 정리, 과금 대상 960건과 업체 HTTP 요청 1,200회를 대조했다. 접수 오류·도구 미발송은 없었고 전체 AP health와 새 lag 0 표본도 확인했다. 실행 JAR 목록의 차이는 결과 Manager 한 개였다.

| p95 구간 | 변경 전 | 변경 후 |
|---|---:|---:|
| 접수 → 첫 업체 HTTP 수신 | 26.626초 | 23.167초 |
| 업체 HTTP 수신 → 최종 판단 | 61.480초 | 44.939초 |
| 최종 판단 → SQL 이력 | 34.850초 | 28.198초 |
| SQL 이력 → DynamoDB 정리 | 33.437초 | 33.082초 |
| 접수 → 전체 완료 | 114.096초 | 81.100초 |

구간별 p95는 메시지별 시각 차이의 분포이고 합산할 수 없다. 전후 각 1회의 개발 환경 관측이며, 저장소 경합·스케줄 시작 시점 등의 변동을 포함한다. 스케줄러 대기가 분리됐다는 회귀 시험과 별개로 이 수치를 운영 성능 개선율로 보장하지 않는다. HTTP Sender 코드는 이번에 변경하지 않았고, Sender 저장소 대기·결과 Manager의 페이지 처리 시간·완료 정리 지연은 후속 진단 범위다.

배포 후 Prometheus의 `executor_pool_core_threads{job="result-manager",name="taskScheduler"}` 값도 5로 확인했다. `applicationTaskExecutor`는 별도 실행기이므로 이 지표를 볼 때 `name`을 구분한다.

정책 시험 `cebfee86`의 13개 사례도 통과했다. HTTP 응답·실패 웹훅 각각의 통신사 이동과 TPS 재시도, 5초 무응답, 최초 이후 재시도 3회 소진 및 TCP 전환을 확인했다. 재발송 요청 간격은 60.396~70.795초였고 이력·과금·고객 통지·원본 정리가 일치했다. 최종 처리 후 70초 추가 관찰에서도 초과 발송이 없었다.

웹훅·SQL 장애 시험 `6fa95788`도 통과했다. PostgreSQL 중단 중 원본 100건·완료 lag 100건을 보존했고, 복구 후 조기 웹훅 1건을 포함한 이력·고객 통지·원본 삭제 각 101건과 과금 대상 51건이 일치했다. 늦은 중복·상충·미등록 웹훅을 처리한 뒤 70초 동안 추가 발송이 없었고 완료 lag은 0이었다.

검증 로그·배포 JAR 목록·전후 비교는 `.monitoring/result-scheduling/`에 보존한다.

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

## Kafka 브로커 중단·복구 검증

```bash
bash scripts/monitoring.sh kafka-test
bash scripts/monitoring.sh verify
```

`kafka-test`는 실행 중인 `platform-messaging-monitoring`의 Kafka 컨테이너를 `pause`해 응답을 중단한다. AP나 브로커의 주소·데이터는 변경하지 않는다. 실행 전에 전체 AP health와 주요 소비 그룹의 lag 0을 확인하고, 로컬 업체 mock만 재시작해 시험용 응답 제어를 반영한다. 다른 장애 시험·`up`·트래픽 생성과 함께 실행하지 않는다. 예외가 발생해도 `finally`에서 Kafka를 `unpause`하고 mock 응답 제어 파일을 제거한다.

| 경계 | 장애 중 확인 | 복구 후 확인 |
|---|---|---|
| 신규 접수 | `RECEIVED` 반환, ORIGIN·발행 복구 인덱스 보존, 최초 Kafka 발행 확인 실패 로그, 업체 호출 0건 | 고객 재접수 없이 성공·과금 대상·고객 웹훅 각 1건 |
| HTTP 실패 결과 | mock이 Kafka 중단 후 HTTP 400·`61001` 반환, STEP에 `OBSERVED`·`publish_state=PENDING` 유지 | HTTP 재호출 없이 최종 실패 1건, 과금 대상 0건 |
| 업체 성공 웹훅 | HTTP 200 기록 후 웹훅 제출, 수신 API가 `503 MESSAGE_WEBHOOK_UNCONFIRMED` 반환 | 동일 결과 재전송을 202로 접수, 성공·과금 대상·고객 웹훅 각 1건 |

발행 타임아웃은 미발행 확정이 아니다. 일시 중단된 브로커가 복구 후 이미 전송된 요청을 처리할 수도 있으므로, 이 시험은 복구 AP 로그를 필수 조건으로 삼지 않는다. 실제 로그 유무는 `recoveryPublicationLogged`에 기록한다. 복구 AP의 재발행 경로 자체는 앞 절의 `recovery-test`에서 별도로 검증한다.

2026-10-10 `runId=23f4d6b8`은 브로커를 23.03초 중단한 상태에서 세 경계를 확인했다. 웹훅의 503 응답은 10.11초에 도착했고, 장애 중 최종 이력은 0건이었다. 복구 후 최종 이력 3건·과금 대상 2건·고객 결과 3건·업체 HTTP 호출 3건·ORIGIN 삭제 3건을 확인했다. 추가로 70초 관측해 중복 발송·과금·고객 결과가 없었고, 주요 소비 그룹 5개의 lag는 모두 0이었다. AP 11개는 재시작 없이 복구 후 전체 health `UP`을 확인했다. 상세 ID·관찰 결과·복원 여부는 `.monitoring/kafka-latest.json`에 남는다.

이는 단일 로컬 브로커의 일시 중단·재개 시험이다. 브로커 프로세스 강제 종료, 디스크 유실, 다중 브로커 리더 전환, HTTP 응답 관찰을 저장하기 전 Sender 종료까지 검증한 결과는 아니다.

## HTTP 응답 관찰 저장 전 Sender 종료 검증

```bash
bash scripts/monitoring.sh sender-crash-test
```

실행 중인 격리 모니터링 스택에서 단독 실행한다. 임시 DynamoDB 프록시를 띄우고 SKT Sender만 그 주소를 사용하게 한다. 프록시는 Sender가 보낸 HTTP 관찰 저장 요청을 캡처하되 DynamoDB에 전달하지 않는다. 캡처한 값이 `ACCEPTED`·HTTP 200이고 실제 STEP은 `SENDING`·관찰 없음인지 확인한 뒤 Sender를 `SIGKILL`한다. 따라서 업체가 응답을 보냈다는 로그뿐 아니라 **Sender도 HTTP 응답을 해석했지만 결과는 저장하지 못했다**는 경계를 검증한다. 종료 코드 137과 Kafka 소비 lag 1도 확인한다.

두 가지 경우를 실행한다. 첫째는 Sender가 정지된 동안 성공 웹훅을 접수해 inbox `PENDING`을 확인하고, 재기동 뒤 첫 회차 성공으로 마치는 경우다. 둘째는 웹훅 없이 재기동해 30초 이상 된 `SENDING`을 `HTTP_TIMEOUT`으로 복구하고, 타임아웃 판단 1분 이후 같은 ID로 두 번째 호출을 실행한 다음 성공 웹훅으로 마치는 경우다. 원래의 30초 복구 유예와 1분 재시도 정책을 줄이지 않는다.

최초 시험 `21a68d6c`에서는 이미 도착한 성공 웹훅을 HTTP 관찰 대기로 미뤄 둔 사이, 복구 타임아웃이 다음 회차를 예약해 불필요한 두 번째 호출이 발생했다. 디버거 재현에서도 미처리 성공 웹훅이 있는데 `TIMEOUT → invocation=2`로 진행하는 것을 확인했다. 이를 고쳐 타임아웃 판단 시 동일 메시지·통신사의 미처리 웹훅을 먼저 처리한다. 명시적인 HTTP 실패나 이미 처리된 웹훅의 정책은 바꾸지 않는다.

각 결과의 이력·과금 대상·고객 결과가 1건이고 ORIGIN이 삭제되는지 확인하고, 추가 70초 동안 잘못된 재시도가 없는지 다시 검사한다. `.monitoring/sender-crash-latest.json`에 결과와 메시지 ID를 기록한다. 정상·실패 종료 모두 SKT Sender를 원래 DynamoDB 주소·재시작 정책으로 복원하고 임시 프록시 컨테이너를 제거한다. 별도 실행 모듈이나 상시 인프라는 추가하지 않는다.

2026-10-10 수정 후 `runId=f1f88aeb`은 두 사례 모두 통과했다. 먼저 도착한 웹훅은 첫 회차·HTTP 호출 1회로 완료했고, 웹훅이 없는 사례는 복구 타임아웃 관찰 이후 65.47초에 두 번째 호출이 이뤄졌다. 이력 2건·과금 대상 2건·고객 결과 2건·ORIGIN 삭제 2건·HTTP 호출 총 3회를 확인했다. 추가 70초 관측 후 소비 lag 0, AP 11개 health `UP`, Sender 정상 설정 복원과 임시 프록시 제거도 확인했다. 관련 공통·결과 관리자 Java 테스트 99개가 통과했다. 실제 업체의 2시간 중복 차단은 mock에서 검증한 것이 아니며 업체 계약에 의존한다.

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

2026-10-10 웹훅 우선 처리 수정 후 `runId=b737b559`로 같은 13개 시나리오를 재검증했다. 통신사 이동, HTTP·웹훅 TPS 초과, 무응답 재시도 소진, TCP 전환의 최종 결과와 과금 대상·고객 통지가 모두 통과했고, 추가 70초 동안 불필요한 발송도 없었다.

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

Redis 유실 시험은 **기존 `clientMsgId`의 발송 키 1개 유실**이다. Redis 전체 초기화나 고객의 접수 중복 키 유실 뒤 새 API 요청을 보내는 경우까지 검증한 것은 아니다. DynamoDB 시험도 **HTTP 관찰이 저장된 뒤의 연결 중단**과 **완료 AP의 정리 연결 실패**를 다룬다. HTTP 관찰 저장 전 Sender 종료는 앞 절의 `sender-crash-test`에서 별도로 검증한다. 전체 데이터 유실과 DynamoDB TTL의 실제 만료 삭제는 아직 별도 검증 범위다.

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
