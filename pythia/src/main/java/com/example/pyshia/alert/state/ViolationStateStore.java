package com.example.pyshia.alert.state;

import com.example.pyshia.alert.domain.Severity;
import com.example.pyshia.alert.domain.ViolationKey;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Component;

@Component
public class ViolationStateStore {

  private final ConcurrentHashMap<ViolationKey, ViolationState> store = new ConcurrentHashMap<>();

  public boolean shouldSend(ViolationKey key, Severity severity, int window) {
    AtomicBoolean result = new AtomicBoolean(false);
    store.compute(key, (k, state) -> {
      if (state == null) {
        state = new ViolationState();
      }
      state.recordViolation(severity);
      if (state.getCount(severity) >= window && state.getLastSentSeverity() != severity) {
        state.markSent(severity);
        result.set(true);
      }
      return state;
    });
    return result.get();
  }

  public void clear(ViolationKey key) {
    store.remove(key);
  }
}
