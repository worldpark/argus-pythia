package com.example.pyshia.alert.state;

import com.example.pyshia.alert.domain.Severity;

class ViolationState {

  private int warningCount;
  private int criticalCount;
  private Severity lastSentSeverity;

  void recordViolation(Severity severity) {
    if (severity == Severity.WARNING) {
      warningCount++;
      criticalCount = 0;
    } else {
      criticalCount++;
      warningCount = 0;
    }
  }

  int getCount(Severity severity) {
    return severity == Severity.WARNING ? warningCount : criticalCount;
  }

  Severity getLastSentSeverity() {
    return lastSentSeverity;
  }

  void markSent(Severity severity) {
    lastSentSeverity = severity;
  }

  void reset() {
    warningCount = 0;
    criticalCount = 0;
    lastSentSeverity = null;
  }
}
