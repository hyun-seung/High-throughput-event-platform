package event.delivery.dispatch.model;

public record DispatchAttempt(
        String deliveryId,
        String attemptId,
        String provider,
        long version
) {
}
