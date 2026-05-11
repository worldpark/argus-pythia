# 018. LLM 사용을 위한 Spring AI 환경 세팅

## Target
pyshia

## 1. 목표 (Goal)
- pyshia 프로젝트에 메트릭 데이터를 request 받고 분석할 LLM과 통신하기 위한 Spring AI 환경 세팅
- Request DTO 까지 작성
- 현재 LLM은 OpenAI의 GPT-4.1 mini 모델을 사용할것이지만 차후 Anthropic의 모델을 사용할 가능성도 있으니 확장성 있게 설계

## 2. 입력 (Input)

- request 프롬프트 내용

```
# 분석 대상
- application: {application}
- instance: {instance}
- range: 최근 15분

# 메트릭 요약
- 각 메트릭별 평균값 혹은 최대값

# 시계열 데이터
{timeSeriesTable}

# 분석 요청
- 이상 징후를 판단하라
- 원인 후보를 우선순위로 제시하라
- 추가 확인할 지표를 제안하라
- 조치 방안을 제시하라
```
  
## 3. 제약 (Constraints)
* Controller/API 구현 제외
* Spring AI 버전은 2.0.0-M6 (Spring Boot 4.0.6 호환 — 1.1.6은 Spring Framework 7과 런타임 비호환으로 변경, 2026-05-10)
* Request data는 dto로 먼저 만든뒤에 prompt 변환

## 4. 성공 기준 (Acceptance Criteria)
- 이후 구현 Task에서 바로 사용 가능
- 정상적으로 LLM과 연동 가능

## 5. 제외 범위 (Out of Scope)
* Micrometer metric name 변경
* 멀티 노드 Aggregation
* range query 처리