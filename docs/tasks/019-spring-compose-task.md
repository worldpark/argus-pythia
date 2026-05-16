# 019. argus, pythia Dockerfile, docker-compose.yml 작성

## Target
root

## 1. 목표 (Goal)
- argus, pythia를 작동시키는 Dockerfile, docker-compose.yml 작성
  
## 2. 제약 (Constraints)
* docker/was 폴더 내부에서 작성
* argus의 포트포워딩은 80:80, pythia는 8080:8080
* 환경변수는 .env로 적용

## 3. 성공 기준 (Acceptance Criteria)
- 이후 구현 Task에서 바로 사용 가능
- 정상적으로 컨테이너 생성 및 동작

## 4. 제외 범위 (Out of Scope)
* Micrometer metric name 변경
* 멀티 노드 Aggregation
* range query 처리