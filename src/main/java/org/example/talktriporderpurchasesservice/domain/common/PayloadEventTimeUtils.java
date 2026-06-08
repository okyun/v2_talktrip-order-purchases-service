package org.example.talktriporderpurchasesservice.domain.common;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class PayloadEventTimeUtils {

    private PayloadEventTimeUtils() {
    }

    public static Instant resolveCreatedAt(Map<String, Object> payload) {
        return resolveInstant(payload, "createdAt");
    }

    public static Map<String, Object> enrichCreatedAt(Map<String, Object> payload, Instant createdAt) {
        Map<String, Object> enriched = payload != null ? new HashMap<>(payload) : new HashMap<>();
        if (createdAt != null && !enriched.containsKey("createdAt")) {
            enriched.put("createdAt", createdAt.toString());
        }
        return enriched;
    }

    public static Instant resolveInstant(Map<String, Object> payload, String field) {
        if (payload == null || field == null || field.isBlank()) {
            return null;
        }
        Object value = payload.get(field);
        if (value == null) {
            return null;
        }
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof Number number) {
            return Instant.ofEpochMilli(number.longValue());
        }
        if (value instanceof String text && !text.isBlank()) {
            try {
                return Instant.parse(text);
            } catch (DateTimeParseException ignored) {
                try {
                    return LocalDateTime.parse(text).atZone(ZoneId.systemDefault()).toInstant();
                } catch (DateTimeParseException ignoredAgain) {
                    return null;
                }
            }
        }
        if (value instanceof List<?> list) {
            return parseLocalDateTimeList(list);
        }
        return null;
    }

    private static Instant parseLocalDateTimeList(List<?> list) {
        if (list.size() < 3) {
            return null;
        }
        try {
            int year = toInt(list.get(0));
            int month = toInt(list.get(1));
            int day = toInt(list.get(2));
            int hour = list.size() > 3 ? toInt(list.get(3)) : 0;
            int minute = list.size() > 4 ? toInt(list.get(4)) : 0;
            int second = list.size() > 5 ? toInt(list.get(5)) : 0;
            int nano = list.size() > 6 ? toInt(list.get(6)) : 0;
            return LocalDateTime.of(year, month, day, hour, minute, second, nano)
                    .atZone(ZoneId.systemDefault())
                    .toInstant();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static int toInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }
}
