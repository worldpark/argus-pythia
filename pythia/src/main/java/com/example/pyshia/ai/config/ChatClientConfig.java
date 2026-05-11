package com.example.pyshia.ai.config;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring AI ChatClient 빈 등록 설정.
 *
 * <p>Provider 중립성을 위해 호출부는 {@link ChatClient}에만 의존한다. 모델/옵션 설정은
 * {@code spring.ai.openai.chat.options.*} (application.yml)에 위임하며, 향후 system prompt 등 공통
 * 옵션 추가가 필요할 경우 본 클래스에서 단일 지점으로 변경한다.
 *
 * <p>Spring AI starter가 {@link ChatClient.Builder} 빈을 자동 구성으로 공급하므로 본 설정은 그 Builder를 주입받아
 * {@link ChatClient}로 빌드한다. starter가 비활성화된 환경에서는 컨텍스트 시작이 실패하므로(의도된 fail-fast) AI starter를
 * 의존성으로 보유하지 않는 환경에서는 이 모듈도 로드되지 않아야 한다.
 */
@Configuration
public class ChatClientConfig {

  @Bean
  public ChatClient chatClient(ChatClient.Builder builder) {
    return builder.build();
  }
}
