# 015. Pythia Email alarm 구현

## Target
pythia

## Goal
- pythia 프로젝트에 운영자에게 이메일을 보낼수 있는 기능 추가


## Context
- smtp 및 계정 정보는 application.yml 에 추가할것

## Requirements
- google smtp 사용예정

## Constraints
- 별도의 email 패키지를 생성하여 구현해야함
- emailService 를 만들어 별도로 send 함수 작성

## Acceptance Criteria
- 성공적으로 이메일이 보내지면 성공

## Test
- 임계값 처리와 이메일 설정이 끝나면 작성