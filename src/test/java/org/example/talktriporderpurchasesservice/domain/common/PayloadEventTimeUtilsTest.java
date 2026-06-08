package org.example.talktriporderpurchasesservice.domain.common;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PayloadEventTimeUtilsTest {

    @Test
    void resolveCreatedAt_parsesIsoInstantString() {
        Instant expected = Instant.parse("2026-05-27T10:15:30.123Z");

        Instant actual = PayloadEventTimeUtils.resolveCreatedAt(Map.of(
                "orderId", 1L,
                "createdAt", "2026-05-27T10:15:30.123Z"
        ));

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void resolveCreatedAt_parsesLocalDateTimeString() {
        Instant actual = PayloadEventTimeUtils.resolveCreatedAt(Map.of(
                "createdAt", "2026-05-27T19:15:30"
        ));

        assertThat(actual).isNotNull();
    }

    @Test
    void resolveCreatedAt_parsesJacksonLocalDateTimeArray() {
        Instant actual = PayloadEventTimeUtils.resolveCreatedAt(Map.of(
                "createdAt", List.of(2026, 5, 27, 19, 15, 30, 0)
        ));

        assertThat(actual).isNotNull();
    }

    @Test
    void enrichCreatedAt_addsMissingField() {
        Instant createdAt = Instant.parse("2026-05-27T10:15:30.123Z");

        Map<String, Object> enriched = PayloadEventTimeUtils.enrichCreatedAt(Map.of("orderId", 1L), createdAt);

        assertThat(enriched.get("createdAt")).isEqualTo(createdAt.toString());
    }
}
