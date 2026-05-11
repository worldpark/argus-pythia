package com.example.pyshia.metric.domain;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;

@Converter(autoApply = true)
public class KstOffsetDateTimeConverter implements AttributeConverter<OffsetDateTime, LocalDateTime> {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    @Override
    public LocalDateTime convertToDatabaseColumn(OffsetDateTime attribute) {
        if (attribute == null) {
            return null;
        }
        return attribute.atZoneSameInstant(KST).toLocalDateTime();
    }

    @Override
    public OffsetDateTime convertToEntityAttribute(LocalDateTime dbData) {
        if (dbData == null) {
            return null;
        }
        return dbData.atZone(KST).toOffsetDateTime();
    }
}
