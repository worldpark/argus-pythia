package com.example.pyshia.ai.service;

import com.example.pyshia.ai.dto.MetricAnalysisRequest;
import com.example.pyshia.ai.exception.AiAnalysisException;
import com.example.pyshia.ai.exception.AiErrorCode;
import com.example.pyshia.ai.prompt.MetricAnalysisPromptFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.stereotype.Service;

/**
 * 메트릭 분석 LLM 호출 진입점.
 *
 * <p>요청 → Prompt 변환 → ChatClient 호출 → 응답 본문(String) 반환까지를 담당하며, 본 task 범위에서는 응답 파싱/구조화는
 * 수행하지 않는다. Spring AI runtime 예외(인증/네트워크/모델 오류 등)는 모두
 * {@link AiAnalysisException}({@link AiErrorCode#LLM_CALL_FAILURE})으로 변환해 호출자에게 전파한다.
 */
@Slf4j
@Service
public class MetricAnalysisService {

  private final ChatClient chatClient;
  private final MetricAnalysisPromptFactory promptFactory;

  public MetricAnalysisService(ChatClient chatClient, MetricAnalysisPromptFactory promptFactory) {
    this.chatClient = chatClient;
    this.promptFactory = promptFactory;
  }

  public String analyze(MetricAnalysisRequest request) {
    Prompt prompt = promptFactory.build(request);

    String content;
    try {
      content = chatClient.prompt(prompt).call().content();
    } catch (AiAnalysisException e) {
      throw e;
    } catch (RuntimeException e) {
      log.error("LLM call failed: error={}", e.getMessage());
      throw new AiAnalysisException(
          AiErrorCode.LLM_CALL_FAILURE, "LLM call failed: " + e.getMessage(), e);
    }

    if (content == null || content.isBlank()) {
      throw new AiAnalysisException(
          AiErrorCode.EMPTY_RESPONSE, "LLM returned empty response");
    }
    return content;
  }
}
