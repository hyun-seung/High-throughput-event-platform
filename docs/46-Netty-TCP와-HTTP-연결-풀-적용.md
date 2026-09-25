# Netty TCP와 HTTP 연결 풀 적용

2026-09-25. 2차 TCP를 Netty로 바꾸고 1차 HTTP·DynamoDB 전송 계층의 연결 풀을 명시했다. 발송 선점·재시도·만료·DynamoDB 쓰기 횟수와 Kafka offset 인계 순서는 유지한다. 아래 기본값은 기능 검증 출발점이며 10,000 TPS 또는 Pod 용량을 입증하는 값이 아니다.

## 1. 선택과 공식 근거

| 구간 | 적용 | 이유·근거 |
|---|---|---|
| 2차 TCP | Netty 4.2.15.Final, NIO·FixedChannelPool | 길이 프레임과 연결 대여·반납을 직접 제어한다. [FixedChannelPool](https://netty.io/4.2/api/io/netty/channel/pool/FixedChannelPool.html)은 최대 연결·대기 개수·대기 timeout을 지원한다. 닫은 연결도 풀에 반납해야 한다. |
| Reactor Netty 비교 | 이번에는 도입하지 않음 | [TCP 문서](https://projectreactor.io/docs/netty/release/reference/tcp-client.html#_connection_pool)는 TCP 연결을 풀에 반환하지 않고 닫는 동작을 설명한다. 임의 TCP 업무 프레임의 완료 지점을 라이브러리가 알 수 없으므로 HTTP 풀처럼 자동 재사용한다고 가정할 수 없다. 이 서비스의 연결 재사용 계약을 직접 구현하기 위해 Netty를 선택했다. 절대 성능 우열을 측정한 결과는 아니다. |
| 1차 HTTP | RestClient + Apache HttpClient 5.6.1 | Spring [RestClient](https://docs.spring.io/spring-framework/reference/integration/rest-clients.html)가 지원하는 Apache 어댑터를 사용한다. [HttpClientBuilder](https://hc.apache.org/httpcomponents-client-5.6.x/current/httpclient5/apidocs/org/apache/hc/client5/http/impl/classic/HttpClientBuilder.html)의 retry·redirect 비활성화와 idle eviction을 명시한다. |
| DynamoDB | 동기 AWS SDK 2.54.12 + Apache5HttpClient | [AWS 공식 설정](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/http-configuration-apache5.html)에 따라 apache5-client를 직접 의존하고 httpClientBuilder로 풀을 설정한다. SDK client가 HTTP 자원의 수명을 관리한다. |

공식 웹 API 문서는 이후 패치 버전을 표시할 수 있다. 위 숫자는 이번 프로젝트의 실제 해결된 의존 버전이다. Netty의 보급 근거는 [공식 사용 프로젝트 목록](https://netty.io/wiki/adopters.html)이며 정확한 시장 점유율 수치를 뜻하지 않는다.

Redis의 Spring Data Redis/Lettuce, Kafka의 Spring Kafka/공식 client, PostgreSQL의 JDBC/HikariCP, 고객 통지의 재사용 JDK HttpClient는 유지한다. [Spring Kafka 비동기 반환](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/async-returns.html)은 ack 동작에도 영향을 준다. 전송 라이브러리를 바꾸는 이번 작업에서 listener를 fire-and-forget으로 전환하지 않았다. 동기 저장·발송 결과가 끝나기 전에 입력 처리가 완료되지 않는다.

## 2. TCP 연결 계약

- 4바이트 big-endian 길이 + UTF-8 JSON, 본문 최대 1 MiB. [LengthFieldBasedFrameDecoder](https://netty.io/4.2/api/io/netty/handler/codec/LengthFieldBasedFrameDecoder.html)로 분할 수신·길이 검증을 처리한다. 수신 ByteBuf는 SimpleChannelInboundHandler가 해제하고 발신 버퍼는 pipeline에 소유권을 넘긴다. [참조 계수 원칙](https://netty.io/wiki/reference-counted-objects.html).
- 연결 하나에는 동시에 요청 하나만 보낸다. 정상 업무 응답 후 재사용한다. 시뮬레이터도 한 연결에서 여러 프레임을 순차 처리한다.
- deliveryId/attemptId, 필수 필드, accepted/code를 검증한다. RECEIVED는 업체 접수 확인이며 최종 배달 성공은 기존 웹훅으로 판단한다.
- 응답 유실·기한 초과·손상·ID 불일치 연결은 닫고 반납한다. 같은 attemptId로 재시도하더라도 이전 스트림의 늦은 응답을 읽지 않는다. 유효한 RETRY_1S/RETRY_10S/REJECTED 응답은 재사용 가능하다.
- 풀 상한을 넘겨 새 연결을 만드는 AcquireTimeoutAction.NEW는 사용하지 않는다. 포화·대기 만료는 실패하며 대기했던 요청이 나중에 자동 발송되지 않는다. 대기 중 스레드가 중단돼도 뒤늦게 획득한 연결은 폐기·반납한다.
- 네트워크 계층은 재전송하지 않는다. NO_RESPONSE는 기존 내구성 재시도 정책으로 처리한다. 업체에 실제 효과가 생긴 뒤 응답만 유실된 경우의 중복 가능성은 그대로다.
- 기존 동기 포트를 유지하므로 Kafka 작업 스레드는 결과를 기다린다. Netty 도입만으로 전체 파이프라인이 비동기가 되거나 TPS가 증가했다고 단정할 수 없다.

## 3. 기본 설정

| 설정 | 기본값 | 의미 |
|---|---|---|
| external-tcp.max-connections | 32 | dispatch 인스턴스당 연결 상한 |
| external-tcp.max-pending-acquires | 64 | 풀 대기 상한 |
| external-tcp.acquire-timeout / connect-timeout / exchange-timeout | 2s / 2s / 5s | 풀 대기 / 연결 / 획득 후 쓰기·응답 전체 대기 |
| external-tcp.io-threads | 2 | Netty I/O 스레드, 업무 동시성 설정과 별개 |
| external-api.max-connections / max-connections-per-route | 64 / 64 | HTTP 전체·업체 경로별 상한, STRICT 풀 |
| external-api.acquire-timeout / connect-timeout / read-timeout | 2s / 2s / 5s | 풀 대여 / 연결 / 응답 읽기 제한 |
| external-api.max-idle-time | 30s | 유휴 연결 정리 기준 |
| delivery.dynamodb.max-connections | 64 | DynamoDB client를 가진 JVM별 풀 상한 |
| delivery.dynamodb.acquire-timeout / connect-timeout / socket-timeout | 2s / 2s / 5s | SDK 전송 계층별 제한 |
| delivery.dynamodb.max-idle-time | 30s | SDK idle reaper 기준 |

TCP/HTTP 환경변수는 dispatch-worker/application.yml의 EXTERNAL_TCP_*·EXTERNAL_API_*에 노출했다. DynamoDB는 공통 @ConfigurationProperties로 모든 worker에 적용하며 `--delivery.dynamodb.max-connections=...` 같은 Spring 설정으로 변경한다. 음수 용량·0 이하 timeout은 시작 시 거부한다.

HTTP 읽기 제한과 DynamoDB socket timeout은 호출 전체 wall-clock 기한과 같지 않다. SDK 자체 재시도와 DNS·스케줄링도 별도로 고려해야 한다. DynamoDB API 전체/회차 timeout과 SDK 재시도 정책을 이번에 변경하지 않았다. 풀 상한 증가만으로 worker 동시성·DB 용량 부족이 해결되지는 않는다. 최적 값은 개발 완료 후 Pod별 실측으로 정한다.

## 4. 검증

실제 소켓 시험은 TCP 정상·조각 프레임·응답 코드·ID 불일치·과대/잘린 프레임·무응답, 정상/거절 뒤 연결 재사용, 풀 포화 시 미발송과 용량 반환, timeout 후 같은 attempt의 새 연결 사용을 검증한다. HTTP는 연결 재사용과 응답 유실 시 내부 재전송 없음, 기존 응답/오류 분류를 검증한다. TCP 시뮬레이터의 지속 연결도 확인한다.

초기 전체 흐름 실행은 기본 JDK 17을 사용해 앱 시작 전에 실패했다. 기준 JDK 21을 명시해 재실행하며, 실패 환경의 소유 컨테이너·프로세스는 정지했다.

JDK 21 전체 Maven package 통과: **실행 148개, 실패/오류 0개**, 별도 DB·Kafka 환경 조건의 141개는 미실행이다. 이 숫자를 289개 통과로 계산하지 않는다.

격리 실행 `full-flow-e59b60bb0640`의 **전체 기능 6개 모두 통과**: HTTP 성공·고객 재전송, 실패 웹훅 뒤 TCP 성공, 1차 재시도, Redis 일정 유실 뒤 DDB 만료 복구, 1차 만료 뒤 TCP 성공, 양 단계 만료. 고객 실제 수신/SQL 이력 6건(성공 4·만료 2), 요청별 업체 호출 수, DDB 삭제, 완료 중복 차단을 대조했다. 새 Apache5 DynamoDB client와 Spring 설정 바인딩도 실제 실행에 포함됐다. [실행 환경·결과 원문](검증-결과/2026-09-25-Netty와-HTTP-풀-전체-흐름.json).

이번 결과는 기능 회귀 검증이다. 풀별 사용률/대기시간 계측, 긴 유휴 후 연결 경합, TCP 장시간 부하와 Pod별 성능은 성능 준비 단계에서 추가한다. 다음 기능 작업은 Kafka 실제 중단·재기동 시 접수/웹훅 ack와 내구성 인계 복구 검증이다.
