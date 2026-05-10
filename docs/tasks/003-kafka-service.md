# 002. Kafka Producer 구성

## target
argus

---

## Goal
Argus에 Kafka에 메시지를 Producer 하는 기능을 구현한다
  
---

## Kafka Topic
- jvm.metrics.raw

---

## Message Structure

{
  service: string,
  metric: string,
  value: double,
  timestamp: long
}

---

## Requirements
- KafkaTemplate을 사용하여 메시지를 전송
- DTO를 JSON으로 직렬화하여 전송
- topic은 jvm.metrics.raw 사용
- key는 serviceId 사용

---

## Constraints
- Spring kafka 라이브러리 사용

---

## Files
- KafkaConfig.java
- MetricEventProducer.java
- MetricEvent.java

---

## Test
- KafkaTemplate send 호출 테스트
- DTO 직렬화 테스트