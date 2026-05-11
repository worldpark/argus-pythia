package com.example.pythia.kafka.config;

import com.example.pythia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pythia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pythia.kafka.dto.jvm.JvmMetricSnapshotDto;
import tools.jackson.databind.json.JsonMapper;
import java.util.Map;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

  private final KafkaProperties kafkaProperties;
  private final JsonMapper jsonMapper;

  public KafkaConsumerConfig(KafkaProperties kafkaProperties, JsonMapper jsonMapper) {
    this.kafkaProperties = kafkaProperties;
    this.jsonMapper = jsonMapper;
  }

  private Map<String, Object> commonConsumerProps() {
    return kafkaProperties.buildConsumerProperties();
  }

  private <T> JacksonJsonDeserializer<T> deserializerFor(Class<T> targetType) {
    JacksonJsonDeserializer<T> deserializer = new JacksonJsonDeserializer<>(targetType, jsonMapper);
    deserializer.addTrustedPackages("com.example.pythia.kafka.dto");
    return deserializer;
  }

  @Bean
  public DefaultErrorHandler commonErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
  }

  @Bean
  public ConsumerFactory<String, JvmMetricSnapshotDto> jvmConsumerFactory() {
    return new DefaultKafkaConsumerFactory<>(
        commonConsumerProps(), new StringDeserializer(), deserializerFor(JvmMetricSnapshotDto.class));
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, JvmMetricSnapshotDto>
      jvmKafkaListenerContainerFactory(DefaultErrorHandler commonErrorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, JvmMetricSnapshotDto> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(jvmConsumerFactory());
    factory.setCommonErrorHandler(commonErrorHandler);
    return factory;
  }

  @Bean
  public ConsumerFactory<String, HttpMetricSnapshotDto> httpConsumerFactory() {
    return new DefaultKafkaConsumerFactory<>(
        commonConsumerProps(), new StringDeserializer(), deserializerFor(HttpMetricSnapshotDto.class));
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, HttpMetricSnapshotDto>
      httpKafkaListenerContainerFactory(DefaultErrorHandler commonErrorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, HttpMetricSnapshotDto> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(httpConsumerFactory());
    factory.setCommonErrorHandler(commonErrorHandler);
    return factory;
  }

  @Bean
  public ConsumerFactory<String, HikariMetricSnapshotDto> hikariConsumerFactory() {
    return new DefaultKafkaConsumerFactory<>(
        commonConsumerProps(), new StringDeserializer(), deserializerFor(HikariMetricSnapshotDto.class));
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, HikariMetricSnapshotDto>
      hikariKafkaListenerContainerFactory(DefaultErrorHandler commonErrorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, HikariMetricSnapshotDto> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(hikariConsumerFactory());
    factory.setCommonErrorHandler(commonErrorHandler);
    return factory;
  }
}
