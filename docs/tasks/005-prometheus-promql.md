# Task: 005. Prometheus PromQL 작성

## 1. 목표 (Goal)
- JVM/HTTP/DB 관련 PromQL을 정의한다.
- PrometheusClient를 사용해 각 PromQL을 실행하고 PrometheusResponse로 응답받는다.
- 메트릭별 PromQL은 재사용 가능하도록 별도 구조로 관리한다.

### 수집할 메트릭 데이터
- 최근 1분간 서비스별 CPU 사용률
- 최근 1분간 힙 사용률 (%)
- 최근 1분간 GC 평균 소요시간
- 최근 1분간 GC 발생 횟수
- 최근 1분간 활성 스레드 수
- 최근 1분간 엔드포인트별 P99 응답시간 (초)
- 최근 1분간 요청 처리량 (초당 요청 수)
- 최근 1분간 사용 중인 커넥션
- 최근 1분간 대기 중인 커넥션 요청

## 2. 입력 (Input)
- 사전에 정의된 메트릭 타입
  - CPU_USAGE
  - HEAP_USAGE
  - GC_AVG_DURATION
  - GC_COUNT
  - ACTIVE_THREADS
  - HTTP_P99_RESPONSE_TIME
  - HTTP_RPS
  - HIKARI_ACTIVE_CONNECTIONS
  - HIKARI_PENDING_CONNECTIONS

## 3. 출력 (Output)
{
    status: string,
    data:{
        resultType: string,
        result:[
            metric:{},
            value:[]
        ]
    }
}

## 4. 제약 (Constraints)
- com.example.argus.client.PrometheusClient 사용
- com.example.argus.dto.PrometheusResponse 사용
- com.example.argus.exception.PrometheusQueryException 사용
- 테스트용 Prometheus는 localhost:9090 을 사용
- PromQL은 Spring Boot Actuator + Micrometer의 기본 Prometheus metric name 기준으로 작성한다.

## 5. 현재 문제 (Problem)
- 현재 PromQL이 작성되어있지 않음

## 6. 성공 기준 (Acceptance Criteria)
- Prometheus로 부터 정상적으로 PrometheusResponse을 받으면 성공

## 7. 제외 범위 (Out of Scope)
- Grafana Dashboard 구성 제외
- Prometheus scrape 설정 변경 제외
- Micrometer metric name 커스터마이징 제외
- 알림/이상탐지 로직 제외
- 결과 데이터 가공/분석 제외