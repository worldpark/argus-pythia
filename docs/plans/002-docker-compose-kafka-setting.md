● 002. Docker Compose Kafka 환경 구성 — 구현 계획 
                                                                                          
  ---                                                                                                                               
  1. 선택한 Kafka 구성 방식                                                                                                                                                                                                                                             
  결정: KRaft 모드 (Zookeeper 미사용)                                                                                                                 
  ┌──────────────────┬─────────────────────────┬────────────────────────────────────────────┐
  │    비교 항목     │     Zookeeper 방식      │             KRaft 방식 (선택)              │
  ├──────────────────┼─────────────────────────┼────────────────────────────────────────────┤
  │ 컨테이너 수      │ 2개 (zookeeper + kafka) │ 1개 (kafka 단독)                           │
  ├──────────────────┼─────────────────────────┼────────────────────────────────────────────┤
  │ 기동 시간        │ 30~45s                  │ 10~15s                                     │
  ├──────────────────┼─────────────────────────┼────────────────────────────────────────────┤
  │ 메모리           │ 1GB+ (JVM 2개)          │ ~512MB                                     │
  ├──────────────────┼─────────────────────────┼────────────────────────────────────────────┤
  │ 운영 환경 정합성 │ 레거시                  │ Kafka 3.3+ 프로덕션 권장 / 4.0부터 ZK 제거 │
  ├──────────────────┼─────────────────────────┼────────────────────────────────────────────┤
  │ 설정 복잡도      │ listener + ZK 연결      │ listener만                                 │
  └──────────────────┴─────────────────────────┴────────────────────────────────────────────┘

  - 이미지 후보: apache/kafka:3.8.x (공식, 경량) 또는 confluentinc/cp-kafka:7.6.x (광범위 문서). 본 Task는 apache/kafka:3.8.x 채택 —   공식이고 KRaft 기본 설정이 단순.
  - 단일 노드 단일 프로세스 모드 (process.roles=broker,controller) — 로컬 개발에 충분.

  ---
  2. docker-compose 서비스 구성

  배치 위치: C:\side_project\docker\docker-compose.yml (argus·pythia 형제 디렉터리의 부모 — 양 서비스 공용 인프라).

  서비스 목록

  ┌─────────────────┬───────────────────────────────┬───────────────────────────────────┬─────────────────────────────────┐
  │    서비스명     │            이미지             │               역할                │          라이프사이클           │
  ├─────────────────┼───────────────────────────────┼───────────────────────────────────┼─────────────────────────────────┤
  │ kafka           │ apache/kafka:3.8.x            │ KRaft 단일 노드 브로커 + 컨트롤러 │ 상시                            │
  ├─────────────────┼───────────────────────────────┼───────────────────────────────────┼─────────────────────────────────┤
  │ kafka-init      │ apache/kafka:3.8.x            │ 토픽 생성 일회성 컨테이너         │ kafka healthy 후 1회 실행, 종료 │
  ├─────────────────┼───────────────────────────────┼───────────────────────────────────┼─────────────────────────────────┤
  │ kafka-ui (선택) │ provectuslabs/kafka-ui:latest │ 웹 UI (브라우저 디버깅용)         │ 상시                            │
  └─────────────────┴───────────────────────────────┴───────────────────────────────────┴─────────────────────────────────┘

  kafka-ui는 선택사항으로 분류 — 본 Task의 Acceptance Criteria 충족에는 불필요. 디버깅 편의를 위해 포함하되 별도 profile로 격리하여
  docker compose --profile ui up으로만 기동 가능하게 분리.

  네트워크

  - 단일 사용자 정의 네트워크 argus-net (bridge) 정의.
  - 향후 Argus/Pythia를 컨테이너로 올릴 때 이 네트워크에 external: true로 합류시켜 컨테이너 → kafka:29092 접근 가능.

  볼륨

  - kafka-data 명명 볼륨 → /var/lib/kafka/data 마운트. 재기동 시 토픽·메시지 보존.
  - 의도적 제외: 호스트 바인드 마운트 (Windows 경로/권한 이슈 회피).

  ---
  3. 포트 및 listener 설계

  핵심 원칙

  - 컨테이너 내부 통신과 호스트 통신은 다른 hostname/port로 분리해야 한다 — 같은 advertised address를 양쪽이 쓸 수 없음.

  Listener 3종 선언

  ┌───────────────┬─────────────────────────────────┬───────────────┬───────────────────┬──────────────────┐
  │ Listener 이름 │              용도               │     bind      │    advertised     │ 호스트 노출 포트 │
  ├───────────────┼─────────────────────────────────┼───────────────┼───────────────────┼──────────────────┤
  │ CONTROLLER    │ KRaft 컨트롤러 quorum           │ 0.0.0.0:9093  │ (advertise 안 함) │ 비공개           │
  ├───────────────┼─────────────────────────────────┼───────────────┼───────────────────┼──────────────────┤
  │ INTERNAL      │ Docker 네트워크 내부 클라이언트 │ 0.0.0.0:29092 │ kafka:29092       │ 비공개           │
  ├───────────────┼─────────────────────────────────┼───────────────┼───────────────────┼──────────────────┤
  │ EXTERNAL      │ 호스트 머신 클라이언트          │ 0.0.0.0:9092  │ localhost:9092    │ 9092:9092        │
  └───────────────┴─────────────────────────────────┴───────────────┴───────────────────┴──────────────────┘

  환경 변수 정책 (실제 값은 yml에서)

  KAFKA_NODE_ID=1
  KAFKA_PROCESS_ROLES=broker,controller
  KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
  KAFKA_LISTENERS=CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
  KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:29092,EXTERNAL://localhost:9092
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
  KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
  KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
  KAFKA_AUTO_CREATE_TOPICS_ENABLE=false
  CLUSTER_ID=<UUID, base64>

CLUSTER_ID:
- 고정 UUID 사용 (KRaft 요구사항)
- 실행 전 1회 생성 필요
- docker-compose.yml에 환경 변수로 주입

생성 방법:
docker run --rm apache/kafka:3.8.0 kafka-storage.sh random-uuid

주의:
- 임의 문자열 사용 금지
- 반드시 kafka-storage.sh random-uuid로 생성

  ---
  4. Topic 생성 전략

  결정: kafka-init 일회성 컨테이너 방식 (자동 생성 OFF)

  ┌──────────────────────────────────┬──────────┬──────────────────────────────────────────────────────────────────────────────┐
  │               옵션               │  채택    │                                     사유                                     │
  │                                  │   여부   │                                                                              │
  ├──────────────────────────────────┼──────────┼──────────────────────────────────────────────────────────────────────────────┤
  │ A.                               │ ❌       │ partitions/replication 제어 불가, 오타시 의도치 않은 토픽 생성, 프로덕션과   │
  │ auto.create.topics.enable=true   │          │ 동작 차이                                                                    │
  ├──────────────────────────────────┼──────────┼──────────────────────────────────────────────────────────────────────────────┤
  │ B. kafka-init init 컨테이너      │ ✅       │ 명시적, partitions/replication 제어, idempotent (--if-not-exists)            │
  ├──────────────────────────────────┼──────────┼──────────────────────────────────────────────────────────────────────────────┤
  │ C. 수동 명령 문서화              │ ❌       │ Acceptance Criteria "docker compose up -d 실행 시" 만족 어려움               │
  └──────────────────────────────────┴──────────┴──────────────────────────────────────────────────────────────────────────────┘

  생성 토픽 정의

  ┌─────────────────┬────────────┬────────────────────┬───────────┬──────────────────────────────────────────────────────────────┐
  │      Topic      │ partitions │ replication-factor │ retention │                             사유                             │
  ├─────────────────┼────────────┼────────────────────┼───────────┼──────────────────────────────────────────────────────────────┤
  │ jvm.metrics.raw │ 3          │ 1                  │ 7d (기본) │ Argus가 produce하는 고빈도 메트릭. 향후 Pythia consumer 그룹 │
  │                 │            │                    │           │  병렬화 위해 partition 다수.                                 │
  ├─────────────────┼────────────┼────────────────────┼───────────┼──────────────────────────────────────────────────────────────┤
  │ jvm.alert       │ 1          │ 1                  │ 7d (기본) │ Pythia가 produce하는 저빈도 알림. 순서 보장 우선 → partition │
  │                 │            │                    │           │  1.                                                          │
  └─────────────────┴────────────┴────────────────────┴───────────┴──────────────────────────────────────────────────────────────┘

  kafka-init 동작

  - depends_on: kafka: condition: service_healthy — 헬스체크 통과 후 실행.
  - Entrypoint: kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists ... 2회 호출.
  - 종료 코드 0이면 한 번만 실행되고 종료. restart: "no".

  Healthcheck (kafka)

  - kafka-broker-api-versions.sh --bootstrap-server localhost:9092 가 0 종료시 healthy.
  - interval: 10s, timeout: 5s, retries: 10, start_period: 20s.

  ---
  5. 검증 절차

  5.1 자동 검증 (Acceptance Criteria 매핑)

  ┌───────────────────┬──────────────────────────────────────────────────────────────────────┬──────────────────────────────────┐
  │      AC 항목      │                              검증 명령                               │               기대               │
  ├───────────────────┼──────────────────────────────────────────────────────────────────────┼──────────────────────────────────┤
  │ AC-1 정상 실행    │ docker compose up -d → docker compose ps                             │ kafka가 healthy, kafka-init이    │
  │                   │                                                                      │ exited (0)                       │
  ├───────────────────┼──────────────────────────────────────────────────────────────────────┼──────────────────────────────────┤
  │ AC-2 호스트 접근  │ docker compose exec kafka kafka-broker-api-versions.sh               │ 정상 응답                        │
  │                   │ --bootstrap-server localhost:9092                                    │                                  │
  ├───────────────────┼──────────────────────────────────────────────────────────────────────┼──────────────────────────────────┤
  │ AC-2 호스트 접근  │ 호스트에서 nc -zv localhost 9092 또는 Java 클라이언트 연결           │ 연결 성공                        │
  │ (외부)            │                                                                      │                                  │
  ├───────────────────┼──────────────────────────────────────────────────────────────────────┼──────────────────────────────────┤
  │ AC-3 토픽 생성    │ docker compose exec kafka kafka-topics.sh --bootstrap-server         │ jvm.metrics.raw, jvm.alert 출력  │
  │                   │ kafka:29092 --list                                                   │                                  │
  └───────────────────┴──────────────────────────────────────────────────────────────────────┴──────────────────────────────────┘

  5.2 end-to-end 동작 확인 (선택)

  # producer
  docker compose exec kafka kafka-console-producer.sh \
    --bootstrap-server kafka:29092 --topic jvm.metrics.raw

  # consumer (다른 터미널)
  docker compose exec kafka kafka-console-consumer.sh \
    --bootstrap-server kafka:29092 --topic jvm.metrics.raw --from-beginning

  producer에 입력한 메시지가 consumer에 출력되면 통과.

  5.3 호스트 접근 검증

  - Argus 머신(=Docker 호스트)에서 localhost:9092로 Kafka admin 클라이언트가 토픽 목록을 가져올 수 있어야 함. advertised.listeners
  오설정 시 여기서 실패 → 디버깅 1차 지점.

  ---
  6. 후속 작업에 전달할 설정값

  6.1 Argus (Spring Boot — 호스트에서 실행)

  argus/src/main/resources/application.properties 추가:
  spring.kafka.bootstrap-servers=localhost:9092
  spring.kafka.producer.client-id=argus-producer
  spring.kafka.consumer.group-id=argus-alert-consumer
  spring.kafka.consumer.auto-offset-reset=latest
  kafka.topic.metrics-raw=jvm.metrics.raw
  kafka.topic.alert=jvm.alert

  6.2 Pythia (호스트에서 실행)

  spring.kafka.bootstrap-servers=localhost:9092
  spring.kafka.consumer.group-id=pythia-metrics-consumer
  spring.kafka.consumer.auto-offset-reset=earliest
  kafka.topic.metrics-raw=jvm.metrics.raw
  kafka.topic.alert=jvm.alert

  6.3 컨테이너로 띄울 때 (장래 Task용)

  양 서비스가 도커로 이동하면 argus-net 네트워크에 합류 후:
  spring.kafka.bootstrap-servers=kafka:29092

  6.4 통신 방향 합의 (후속 Task 가이드)

  ┌─────────────────┬──────────┬──────────┬───────────────────────────┐
  │      Topic      │ Producer │ Consumer │           비고            │
  ├─────────────────┼──────────┼──────────┼───────────────────────────┤
  │ jvm.metrics.raw │ Argus    │ Pythia   │ Argus가 수집한 JVM 메트릭 │
  ├─────────────────┼──────────┼──────────┼───────────────────────────┤
  │ jvm.alert       │ Pythia   │ Argus    │ Pythia가 분석/탐지한 알림 │
  └─────────────────┴──────────┴──────────┴───────────────────────────┘

  ---
  7. 제외 범위 (의도적 미포함)

  ┌───────────────────────────────────┬───────────────────────────────────────────────────────────────┐
  │               항목                │                             사유                              │
  ├───────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ TLS / SASL 인증                   │ 로컬 개발 단일 호스트, 외부 노출 없음. 운영 이관 시 별도 Task │
  ├───────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ Schema Registry / Avro            │ 현 단계는 JSON 직렬화 충분. Plan에 요구 없음                  │
  ├───────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ 다중 브로커 클러스터              │ replication=1로 충분, 단일 머신 리소스 절약                   │
  ├───────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ Kafka Connect / KSQL              │ 본 Task 범위 외                                               │
  ├───────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ Prometheus JMX exporter for Kafka │ 별도 Task (Argus 본체 메트릭 수집과 혼동 방지)                │
  ├───────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ 토픽 ACL                          │ PLAINTEXT 단일 사용자 환경                                    │
  ├───────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ 호스트 바인드 마운트              │ Windows 호환성 이슈, 명명 볼륨으로 충분                       │
  ├───────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ kafka-ui 필수화                   │ 디버깅 편의용 — --profile ui로 옵션화                         │
  └───────────────────────────────────┴───────────────────────────────────────────────────────────────┘

  ---
  산출 파일 체크리스트

  1. C:\side_project\docker\kafka\docker-compose.yml — kafka, kafka-init [, kafka-ui] 서비스
  2. C:\side_project\docker\.env (선택) — CLUSTER_ID, 이미지 태그 등 변수 외부화
  3. C:\side_project\docker\README.md (선택, 또는 docs) — 기동/검증 명령 요약
  4. Argus/Pythia의 application.properties는 본 Task가 아닌 후속 Task에서 추가 (002 범위는 인프라까지)