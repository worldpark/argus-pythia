package com.example.argus.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PrometheusResponse {

  private String status;
  private Datas data;

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Datas {

    private String resultType;
    private List<Result> result;
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Result {

    private Map<String, String> metric;
    // value = [epochSeconds(Number), value(String)] 형태이므로 List<Object>로 수신
    private List<Object> value;
  }
}
