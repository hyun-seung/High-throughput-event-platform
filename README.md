# 메시징 서비스

고객 메시지를 `/api/v1/messages`로 접수해 통신사 HTTP로 1차 발송하고, 필요한 경우 TCP로 2차 발송하는 Kafka 기반 서비스입니다. 접수에는 Redis TPS·월 Quota 검사, DynamoDB 원본 저장, Kafka 발행을 사용합니다. Kafka 성공 ack 확인 후 `200 OK`를 반환하며 발행 실패·확인 시간 초과에는 `503`을 반환합니다. HTTP 200 뒤 업체 웹훅이 최종 1차 결과이며, 명시적 HTTP 실패와 타임아웃은 Sender가 결과 토픽으로 직접 인계합니다.

현재 설계와 구현 범위는 [요구사항과 전체 설계](docs/00-요구사항과-전체-설계.md), [케이스별 Call Flow](docs/53-신규-메시지-케이스별-Call-Flow.md), [현재 구현 상태](docs/01-현재-구현-상태와-남은-작업.md)를 순서대로 확인하세요. [문서 안내](docs/문서-안내.md)에 오류 코드와 CDC 자료도 정리했습니다.

Maven 모듈 12개는 실행 AP 10개, 공통 라이브러리, 로컬 검증 도구로 구성됩니다. [모듈별 역할과 저장소 의존성](docs/01-현재-구현-상태와-남은-작업.md#코드-구조와-정리-범위)에 현재 경계를 정리했습니다. Redis·DynamoDB 의존성은 해당 저장소를 사용하는 AP에서 직접 선언합니다.

JDK 21과 Docker가 필요합니다. 로컬에서 메인 경로를 실행하고 관측하려면 다음 명령을 사용합니다.

```sh
./mvnw package
bash scripts/monitoring.sh up
bash scripts/monitoring.sh demo --rate 1 --seconds 1 --errors
bash scripts/monitoring.sh verify
```

[Grafana 통합 관제](http://localhost:13000/d/messaging-overview)에서 AP·Kafka·저장소 지표와 로그를 볼 수 있습니다. 실행 옵션은 [로컬 실행 방법](docs/06-로컬-개발-환경-실행-방법.md), 화면 구성은 [모니터링 안내](docs/19-그라파나-대시보드-실행과-PPT-항목-대응.md)를 참고하세요.

단위 테스트는 `bash scripts/test-java.sh unit`, 별도 Docker 인프라를 쓰는 통합 테스트는 `bash scripts/test-java.sh integration`으로 실행합니다. 2차 TCP 전문은 실제 업체 규격 확인 전까지 임시 형식입니다.

테스트 집계는 루트 POM에 등록된 모듈만 대상으로 합니다. 삭제된 모듈의 구형 `target` 결과는 제외하며, 통합 테스트에서 제외된 테스트가 있으면 검증에 실패합니다.
