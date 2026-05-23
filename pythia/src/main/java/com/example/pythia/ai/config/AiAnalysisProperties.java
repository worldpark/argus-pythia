package com.example.pythia.ai.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pythia.ai")
@Getter
@Setter
public class AiAnalysisProperties {

  private int maxResponseChars = 5000;
}
