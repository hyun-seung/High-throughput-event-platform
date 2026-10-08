# 계약·전화번호 통신사 CDC 캐시

`PRE-SEND-MANAGER`가 사용할 참조 데이터의 원본은 PostgreSQL `delivery_results` 스키마이다. `client_message_contracts`와 `phone_carrier_mappings`를 사용한다. 현재 계약 테이블에는 사용 여부와 기존 TPS·Quota 필드만 있어, 실제 발송 정책·전문 생성에 필요한 추가 필드는 별도 설계가 필요하다. 이 문서는 캐시 변경 전파 준비 범위다.

| PostgreSQL 원본 | Debezium Kafka 토픽 | Redis 키와 값 |
|---|---|---|
| `delivery_results.client_message_contracts` | `messaging_reference.delivery_results.client_message_contracts` | `message:contract:{client:<id>}` → 계약 행 JSON |
| `delivery_results.phone_carrier_mappings` | `messaging_reference.delivery_results.phone_carrier_mappings` | `message:phone-carrier:<010번호>` → `SKT`, `KT`, `LGU` |

PostgreSQL `wal_level=logical`, `messaging_reference_pub` publication, `messaging_reference_slot` replication slot을 로컬 Compose에서 사용한다. Debezium Connect가 두 테이블의 최초 스냅샷과 이후 변경을 Kafka에 발행하고 `messaging-reference-cache`가 Redis에 투영한다. 추가·수정·스냅샷은 같은 키에 덮어쓰고 삭제 이벤트는 키를 지운다. 뒤따르는 Kafka tombstone은 이미 삭제된 키를 다시 건드리지 않는다. Redis 갱신에 실패하면 Kafka 오프셋을 완료하지 않고 재시도한다. CDC 토픽은 compact 정책으로 생성한다. Kafka 토픽과 Redis 키는 번호·계약 정보를 담으므로 접근 및 보존 설정을 운영 배포에서 별도로 정해야 한다.

## 로컬 실행

```bash
bash scripts/local.sh infra
bash scripts/local.sh init-db
bash scripts/local.sh cdc
bash scripts/local.sh build
bash scripts/local.sh run messaging-reference-cache
```

`cdc` 명령은 참조 테이블과 publication을 확인하고 Debezium Connect 프로필을 시작한 뒤 `scripts/cdc/connector-config.json`을 REST API에 등록한다. 로컬 `delivery` 계정·비밀번호와 단일 Kafka 브로커 복제 계수 1은 개발 환경 전용이다. PostgreSQL logical slot이 살아 있는 동안 CDC 소비가 장기간 중단되면 WAL이 쌓일 수 있으므로 운영 환경은 slot 지연·WAL 사용량을 관측해야 한다.

예를 들어 로컬 데이터 변경은 다음처럼 확인할 수 있다. `<client_id>`에는 `users`의 실제 ID를 사용한다.

```sql
INSERT INTO delivery_results.phone_carrier_mappings(phone_number, carrier)
VALUES ('01012345678', 'SKT')
ON CONFLICT (phone_number) DO UPDATE SET carrier = EXCLUDED.carrier, updated_at = now();

INSERT INTO delivery_results.client_message_contracts
    (client_id, enabled, tps_limit, quota_general, quota_noti, quota_adv, quota_alert)
VALUES (<client_id>, true, 100, 100000, 100000, 100000, 100000)
ON CONFLICT (client_id) DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now();
```

`messaging-pre-send-manager`의 참조 조회 코드는 계약 캐시가 없으면 PostgreSQL `client_message_contracts`를 읽고, 번호→통신사 캐시가 없으면 **인라인 PostgreSQL 조회 대신 SKT를 첫 1차 HTTP 통신사로 선택**한다. SQL 조회 결과를 Redis에 임의로 재기록하지 않는다. 캐시 데이터가 잘못된 경우에는 누락으로 취급하지 않고 오류를 내므로 잘못된 계약·통신사 값으로 발송하지 않는다. Kafka 소비·전문 생성과 `66001`의 다음 통신사 이동은 구현됐다. CDC가 늦게 반영된 *기존* Redis 값은 단순 캐시 미스로 감지되지 않는다. 이미 발행한 시도의 통신사와 시도 ID는 재처리 중 바꾸지 않고, 다음 통신사로 이동할 때 새 시도를 만든다. 찾은 통신사 정보를 PostgreSQL 원본에 반영할지와 기존 매핑의 불일치 처리 범위는 아직 정하지 않았다.

Redis 데이터 전체가 소실되어도 이미 커밋한 Kafka 소비 오프셋은 자동으로 되감기지 않는다. 이때는 캐시 앱을 멈추고 이 앱이 소유한 두 키 공간을 비운 뒤, `messaging-reference-cache` 소비 그룹을 CDC 토픽의 earliest로 재설정해 다시 투영해야 한다. 운영용 자동 재구축·정합성 대조는 아직 구현 전이다.



설정 근거: [PostgreSQL 논리 복제 설정](https://www.postgresql.org/docs/17/logical-replication-config.html), [publication과 replica identity](https://www.postgresql.org/docs/17/sql-createpublication.html), [Debezium PostgreSQL 커넥터와 삭제 이벤트](https://debezium.io/documentation/reference/stable/connectors/postgresql.html), [Debezium Connect 실행 예](https://debezium.io/documentation/reference/stable/kc-tutorial.html).
