package com.example.argus.exception;

public class MetricBufferException extends RuntimeException {

    public MetricBufferException(String message) {
        super("metric-buffer: " + message);
    }

    public MetricBufferException(String message, Throwable cause) {
        super("metric-buffer: " + message, cause);
    }
}
