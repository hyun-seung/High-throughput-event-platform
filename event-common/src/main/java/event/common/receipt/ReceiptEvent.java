package event.common.receipt;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/** An authenticated provider observation, not a validated delivery state transition. */
public record ReceiptEvent(String eventId, String eventType, int schemaVersion,
                           String receiptId, String deliveryId, String attemptId,
                           String provider, int routeOrder, ReceiptOutcome outcome, String code,
                           Instant occurredAt, Instant receivedAt) {
    public static ReceiptEvent received(String receiptId, String deliveryId, String attemptId,
                                        String provider, int routeOrder, ReceiptOutcome outcome, String code,
                                        Instant occurredAt, Instant receivedAt) {
        String id = UUID.nameUUIDFromBytes(("receipt:" + provider + ":" + receiptId)
                .getBytes(StandardCharsets.UTF_8)).toString();
        return new ReceiptEvent(id, "DeliveryReceiptReceived", 1, receiptId, deliveryId, attemptId,
                provider, routeOrder, outcome, code, occurredAt, receivedAt);
    }
}
