package com.example.pythia.ai.service;

import com.example.pythia.ai.config.AiAnalysisProperties;
import com.example.pythia.ai.dto.MetricAnalysisRequest;
import com.example.pythia.ai.exception.AiAnalysisException;
import com.example.pythia.ai.exception.AiErrorCode;
import com.example.pythia.ai.prompt.MetricAnalysisPromptFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.retry.TransientAiException;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricAnalysisService {

  private static final Pattern CONTROL_EXCEPT_NL_TAB =
      Pattern.compile("[\\p{Cntrl}&&[^\\n\\t]]");

  private final ChatClient chatClient;
  private final MetricAnalysisPromptFactory promptFactory;
  private final AiAnalysisProperties properties;
  private final RetryRegistry retryRegistry;
  private final CircuitBreakerRegistry circuitBreakerRegistry;

  public String analyze(MetricAnalysisRequest request) {
    Retry retry = retryRegistry.retry("llmAnalysis");
    CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("llmAnalysis");
    return retry.executeSupplier(() -> circuitBreaker.executeSupplier(() -> doAnalyze(request)));
  }

  private String doAnalyze(MetricAnalysisRequest request) {
    Prompt prompt = promptFactory.build(request);

    String content;
    try {
      content = chatClient.prompt(prompt).call().content();
    } catch (AiAnalysisException e) {
      throw e;
    } catch (TransientAiException e) {
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

    String sanitized = CONTROL_EXCEPT_NL_TAB.matcher(content).replaceAll("");
    if (sanitized.length() > properties.getMaxResponseChars()) {
      throw new AiAnalysisException(AiErrorCode.RESPONSE_TOO_LARGE);
    }

    return sanitized;
  }
}
