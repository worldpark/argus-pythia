package com.example.pyshia.ai.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.pyshia.ai.service.MetricAnalysisService;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Spring AI 2.0.0-M6 + Spring Boot 4.0.6 호환 검증.
 *
 * <p>Round 1에서 Spring AI 1.1.6과 Spring Boot 4.0.6 간 {@code HttpHeaders.addAll(MultiValueMap)}
 * NoSuchMethodError(Spring Framework 6 vs 7 충돌)가 발생했던 회귀를 방지한다. 본 테스트는 컨텍스트 로딩 + ChatClient 빈
 * 주입까지만 검증하며, 실제 OpenAI HTTP 호출은 수행하지 않는다.
 */
@SpringBootTest
@ActiveProfiles("test")
class ChatClientConfigIntegrationTest {

  @Autowired private ChatClient chatClient;
  @Autowired private MetricAnalysisService metricAnalysisService;

  @Test
  void chatClientBeanIsRegistered() {
    assertThat(chatClient).isNotNull();
  }

  @Test
  void metricAnalysisServiceIsRegistered() {
    assertThat(metricAnalysisService).isNotNull();
  }
}
