package com.example.pythia.metric.exception;

import com.example.pythia.common.exception.ErrorCode;

public enum MetricStoreErrorCode implements ErrorCode {

    METRIC_PERSIST_FAILED("METRIC_STORE_001", "metric snapshot persistence failed"),
    INVALID_SNAPSHOT_PAYLOAD("METRIC_STORE_002", "snapshot payload missing required fields");

    private final String code;
    private final String defaultMessage;

    MetricStoreErrorCode(String code, String defaultMessage) {
        this.code = code;
        this.defaultMessage = defaultMessage;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public String defaultMessage() {
        return defaultMessage;
    }
}
