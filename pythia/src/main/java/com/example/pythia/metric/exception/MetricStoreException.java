package com.example.pythia.metric.exception;

import com.example.pythia.common.exception.CustomException;

public class MetricStoreException extends CustomException {

    public MetricStoreException(MetricStoreErrorCode errorCode) {
        super(errorCode, errorCode.defaultMessage());
    }

    public MetricStoreException(MetricStoreErrorCode errorCode, Throwable cause) {
        super(errorCode, errorCode.defaultMessage(), cause);
    }
}
