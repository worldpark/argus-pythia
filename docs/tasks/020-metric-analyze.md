# 020. 메트릭 데이터가 임계값 도달 시 메트릭값을 토대로 LLM에 분석을 맡긴 후 결과를 이메일 내용에 추가하여 지정된 사용자에게 이메일을 보냄

## Target
phthia

## 1. 목표 (Goal)
- 메트릭 데이터가 임계값 도달 시 메트릭값을 토대로 LLM에 분석
- LLM 분석 결과를 이메일 내용에 추가
  
## 2. 입력 (Input)
- 메트릭 데이터를 `com.example.pythia.ai.dto.MetricAnalysisRequest` 로 변환하여 LLM에 분석 요청을 보냄

## 3. 제약 (Constraints)
* `com.example.pythia.ai.service.MetricAnalysisService` 의 analyze 함수 사용

## 4. 성공 기준 (Acceptance Criteria)
- 이후 구현 Task에서 바로 사용 가능
- 임계값 도달 알림 이메일에 LLM 분석 결과값 포함