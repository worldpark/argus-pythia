package com.example.argus.dto.metric.buffer;

import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;

public enum MetricBufferType {

    JVM("jvm", JvmMetricSnapshotDto.class),
    HTTP("http", HttpMetricSnapshotDto.class),
    HIKARI("hikari", HikariMetricSnapshotDto.class);

    private final String keySuffix;
    private final Class<?> dtoClass;

    MetricBufferType(String keySuffix, Class<?> dtoClass) {
        this.keySuffix = keySuffix;
        this.dtoClass = dtoClass;
    }

    public String getKeySuffix() {
        return keySuffix;
    }

    public Class<?> getDtoClass() {
        return dtoClass;
    }
}
