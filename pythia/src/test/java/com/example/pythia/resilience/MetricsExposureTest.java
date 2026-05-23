package com.example.pythia.resilience;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class MetricsExposureTest {

  @Autowired
  private MockMvc mockMvc;

  @Test
  @DisplayName("GET /actuator/metrics 가 200 OK 를 반환한다")
  void actuator_metrics_endpoint_200_반환() throws Exception {
    mockMvc.perform(get("/actuator/metrics"))
        .andExpect(status().isOk());
  }

  @Test
  @DisplayName("GET /actuator/metrics 응답에 resilience4j retry 메트릭 이름이 포함된다")
  void actuator_metrics_resilience4j_retry_포함() throws Exception {
    mockMvc.perform(get("/actuator/metrics"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.names[?(@ =~ /resilience4j\\.retry.*/)]").exists());
  }
}
