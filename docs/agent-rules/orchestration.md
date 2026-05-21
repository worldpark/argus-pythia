# Agent Orchestration Rules

## Agent Responsibilities

### planner
- 요구사항 분석
- 구현 범위 정의
- 수정 대상 파일 예측
- 테스트 전략 정의
- 아키텍처 영향 분석
- 코드 수정 금지

### implementor
- planner 범위 내에서만 구현
- 불필요한 리팩토링 금지
- 테스트 코드 반드시 작성
- 기존 스타일 유지
- 작업 완료 후 PR 생성 가능

### reviewer
- 아키텍처 규칙 위반 검사
- 테스트 누락 검사
- 과도한 변경 검사
- 보안 위험 검사
- 요구사항 충족 여부 검사
- 직접 코드 수정 금지

### fixer
- reviewer 피드백만 수정
- 새로운 기능 추가 금지
- 최소 수정 원칙 준수

---

# Review Loop Rules

- 최대 review/fix 반복 횟수는 3회
- reviewer -> fixer -> reviewer 를 1 loop 로 계산한다
- 3회 이후에도 critical issue 존재 시 자동 작업 중단
- unresolved issue를 정리 후 human review 요청

---

# Pull Request Rules

- 모든 구현 작업은 feature branch 에서 수행
- main/master 직접 수정 금지
- PR 본문에는 다음 내용 포함:
    - 구현 요약
    - 변경 파일
    - 테스트 결과
    - known limitations

---

# Human Approval Rules

다음 상황에서는 반드시 human approval 필요:
- architecture 변경
- dependency 추가
- database schema 변경
- docker 구조 변경
- security 관련 변경
- review loop 3회 초과

---

# Safety Rules

- secrets/env 파일 수정 금지
- CI/CD workflow 수정 금지
- 운영 설정 자동 변경 금지
- destructive migration 금지

---

# Task Scope Rules

- 하나의 Task는 하나의 기능만 구현
- 수정 범위를 명확히 제한
- 기존 코드 스타일 유지
- 테스트 없이 기능 추가 금지