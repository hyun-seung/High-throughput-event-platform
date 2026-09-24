package event.delivery.dispatch.model;

import java.time.Instant;

public record DispatchAttempt(
        String deliveryId,
        String attemptId,
        String provider,
        long version,
        int retryCount,
        Instant primaryDeadline
) {
}
