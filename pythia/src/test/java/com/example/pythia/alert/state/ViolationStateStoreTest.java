package com.example.pythia.alert.state;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ViolationStateStoreTest {

  private ViolationStateStore store;
  private ViolationKey key;

  @BeforeEach
  void setUp() {
    store = new ViolationStateStore();
    key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
  }

  @Test
  @DisplayName("window 미달 시 shouldSend는 false를 반환한다")
  void window_미달시_shouldSend는_false를_반환한다() {
    assertThat(store.shouldSend(key, Severity.WARNING, 2)).isFalse();
  }

  @Test
  @DisplayName("window 만족 시 shouldSend는 true를 반환한다")
  void window_만족시_shouldSend는_true를_반환한다() {
    store.shouldSend(key, Severity.WARNING, 2);
    assertThat(store.shouldSend(key, Severity.WARNING, 2)).isTrue();
  }

  @Test
  @DisplayName("window=1이면 첫 호출에서 true를 반환한다")
  void window가_1이면_첫_호출에서_true를_반환한다() {
    assertThat(store.shouldSend(key, Severity.CRITICAL, 1)).isTrue();
  }

  @Test
  @DisplayName("같은 severity 재호출 시 false를 반환한다 (lastSent 가드)")
  void 같은_severity_재호출시_false를_반환한다() {
    store.shouldSend(key, Severity.WARNING, 1);
    assertThat(store.shouldSend(key, Severity.WARNING, 1)).isFalse();
  }

  @Test
  @DisplayName("severity 승격 시 재발송한다 (WARNING → CRITICAL)")
  void severity_승격시_재발송한다() {
    store.shouldSend(key, Severity.WARNING, 1);
    assertThat(store.shouldSend(key, Severity.CRITICAL, 1)).isTrue();
  }

  @Test
  @DisplayName("severity 강등 시 재발송한다 (CRITICAL → WARNING)")
  void severity_강등시_재발송한다() {
    store.shouldSend(key, Severity.CRITICAL, 1);
    store.shouldSend(key, Severity.WARNING, 2);
    assertThat(store.shouldSend(key, Severity.WARNING, 2)).isTrue();
  }

  @Test
  @DisplayName("clear 후 카운터가 리셋되어 window를 다시 채워야 한다")
  void clear_후_카운터가_리셋된다() {
    store.shouldSend(key, Severity.WARNING, 2);
    store.clear(key);
    assertThat(store.shouldSend(key, Severity.WARNING, 2)).isFalse();
  }

  @Test
  @DisplayName("clear 후 재위반 시 window 충족하면 재발송한다")
  void clear_후_재위반시_window_충족하면_재발송한다() {
    store.shouldSend(key, Severity.WARNING, 1);
    store.clear(key);
    assertThat(store.shouldSend(key, Severity.WARNING, 1)).isTrue();
  }

  @Test
  @DisplayName("서로 다른 key는 독립적으로 카운팅된다")
  void 서로_다른_key는_독립적으로_카운팅된다() {
    ViolationKey key2 = new ViolationKey(MetricKind.JVM_HEAP, "argus", "localhost:8080", null);
    store.shouldSend(key, Severity.WARNING, 1);
    assertThat(store.shouldSend(key2, Severity.WARNING, 1)).isTrue();
  }

  @Test
  @DisplayName("severity가 교차해서 들어오면 반대 severity 카운트가 리셋되어 연속성이 끊긴다")
  void severity_교차시_연속_카운트가_리셋된다() {
    store.shouldSend(key, Severity.WARNING, 3);  // w=1, c=0
    store.shouldSend(key, Severity.CRITICAL, 3); // c=1, w=0 (reset!)
    store.shouldSend(key, Severity.WARNING, 3);  // w=1, c=0 (reset!)
    store.shouldSend(key, Severity.CRITICAL, 3); // c=1, w=0 (reset!)
    assertThat(store.shouldSend(key, Severity.WARNING, 3)).isFalse(); // w=1, not >= 3
  }
}
