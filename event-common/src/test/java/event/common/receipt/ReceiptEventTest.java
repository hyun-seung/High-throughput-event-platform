package event.common.receipt;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class ReceiptEventTest {
    @Test
    void identitySurvivesRedeliveryButSeparatesProvidersAndPreservesConflictingEvidence() {
        Instant now = Instant.parse("2026-09-24T00:00:00Z");
        var first = ReceiptEvent.received("receipt-1", "delivery-1", "attempt-1", "primary", 1,
                ReceiptOutcome.FAILED, "FALLBACK", now, now);
        var repeat = ReceiptEvent.received("receipt-1", "delivery-1", "attempt-1", "primary", 1,
                ReceiptOutcome.FAILED, "FALLBACK", now, now.plusSeconds(10));
        var other = ReceiptEvent.received("receipt-1", "delivery-1", "attempt-2", "secondary", 2,
                ReceiptOutcome.DELIVERED, "DELIVERED", now, now);
        var conflict = ReceiptEvent.received("receipt-1", "delivery-2", "attempt-3", "primary", 1,
                ReceiptOutcome.DELIVERED, "DELIVERED", now, now);
        assertEquals(first.eventId(), repeat.eventId());
        assertEquals(now.plusSeconds(10), repeat.receivedAt());
        assertNotEquals(first.eventId(), other.eventId());
        assertEquals(first.eventId(), conflict.eventId());
        assertEquals("delivery-2", conflict.deliveryId()); // A later processor must detect the conflict, never overwrite it.
    }
}
