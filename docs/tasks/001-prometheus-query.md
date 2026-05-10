# 001. Prometheus Query 기능 구현

## Target
argus

---

## Goal
Prometheus HTTP API를 호출하여 PromQL 쿼리 결과를 받아오는 기능을 구현한다.

---

## Context
- Prometheus API endpoint: /api/v1/query
- Base URL: http://localhost:9090
- PromQL 예시:
  - process_cpu_usage
  - jvm_memory_used_bytes

응답 구조:

```json
{
  "status": "success",
  "data": {
    "result": [
      {
        "metric": {},
        "value": [timestamp, "value"]
      }
    ]
  }
}
```

## Requirements
- Prometheus API를 호출하는 Client 구현
- PromQL을 파라미터로 받아 실행 가능해야 한다
- 결과 JSON을 DTO로 변환
- 단일 값(double)으로 반환하는 메서드 제공
- 결과가 없을 경우 0.0 반환

## Constraints
- WebClient 사용
- Controller에서 직접 호출 금지
- 외부 API 호출은 client 패키지에서만 수행

## Files
- PrometheusClient.java
- PrometheusQueryService.java
- PrometheusResponse.java

## Acceptance Criteria
- PromQL 실행 시 값이 정상 반환된다
- process_cpu_usage 조회 가능
- 결과가 없을 경우 0.0 반환

## Test
- Prometheus API 호출 성공 테스트
- 빈 결과 처리 테스트