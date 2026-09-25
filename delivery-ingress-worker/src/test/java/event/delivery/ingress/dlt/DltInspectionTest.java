package event.delivery.ingress.dlt;

import event.common.delivery.DeliveryEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.KafkaHeaders;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.Map;
import static event.delivery.ingress.dlt.DltInspection.Decision.*;
import static org.junit.jupiter.api.Assertions.*;

class DltInspectionTest {
    static final Instant NOW = Instant.parse("2026-09-25T00:00:00Z");
    static final String ID = "12345678-1234-1234-1234-123456789abc";
    static final JsonMapper MAPPER = JsonMapper.builder().build();
    final DltInspection inspection = new DltInspection("source", Duration.ofHours(3));

    static DeliveryEvent event(Instant received) {
        return DeliveryEvent.requested(ID, 1L, "SMS", Map.of("text", "private-payload"), received, true).forAdmission();
    }
    static ConsumerRecord<byte[], byte[]> record(DeliveryEvent event) {
        return record(MAPPER.writeValueAsBytes(event));
    }
    static ConsumerRecord<byte[], byte[]> record(byte[] value) {
        var record = new ConsumerRecord<>("dlt", 0, 5L, ID.getBytes(StandardCharsets.UTF_8), value);
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, "source".getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(4).putInt(1).array());
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(8).putLong(42).array());
        return record;
    }

    @Test void validEventRequiresDurableStateCheckAndOutputContainsNoPayload() {
        var row = inspection.assess(record(event(NOW.minusSeconds(1))), NOW);
        assertEquals(REQUIRES_STATE_CHECK, row.decision());
        assertEquals(new DltInspection.Source("source", 1, 42), row.source());
        assertEquals(ID, row.requestKey());
        assertEquals(NOW.minusSeconds(1).plus(Duration.ofHours(3)), row.primaryDeadline());
        assertEquals(64, row.valueSha256().length());
        assertFalse(MAPPER.writeValueAsString(row).contains("private-payload"));
    }
    @Test void originalOccurrenceControlsDeadlineIncludingExactBoundary() {
        var row = inspection.assess(record(event(NOW.minus(Duration.ofHours(3)))), NOW);
        assertEquals(EXPIRED_REQUIRES_FINALIZATION, row.decision());
        assertEquals(NOW, row.primaryDeadline());
        assertEquals(NOW.minus(Duration.ofHours(3)), row.occurredAt());
    }
    @Test void conflictIsHeldEvenWhenOldAndNestedUnderListenerException() {
        var record = record(event(NOW.minus(Duration.ofHours(4))));
        record.headers().add(KafkaHeaders.DLT_EXCEPTION_FQCN, "listener.Exception".getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN,
                "event.delivery.ingress.repository.IdempotencyConflictException".getBytes(StandardCharsets.UTF_8));
        assertEquals(IDEMPOTENCY_CONFLICT, inspection.assess(record, NOW).decision());
    }
    @Test void malformedPayloadNullAndTrailingJsonAreSanitized() {
        for (byte[] bytes : new byte[][] {null, "{private-data".getBytes(StandardCharsets.UTF_8),
                (MAPPER.writeValueAsString(event(NOW)) + " {}").getBytes(StandardCharsets.UTF_8)}) {
            var row = inspection.assess(record(bytes), NOW);
            assertEquals(MALFORMED_EVENT, row.decision());
            assertNull(row.requestKey());
            assertFalse(MAPPER.writeValueAsString(row).contains("private-data"));
        }
    }
    @Test void missingAmbiguousOrWrongSourceHeadersCannotBecomeReplayCandidate() {
        var missing = record(event(NOW)); missing.headers().remove(KafkaHeaders.DLT_ORIGINAL_OFFSET);
        assertEquals(INVALID_SOURCE, inspection.assess(missing, NOW).decision());
        var duplicate = record(event(NOW)); duplicate.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, new byte[8]);
        assertEquals(INVALID_SOURCE, inspection.assess(duplicate, NOW).decision());
        var wrong = record(event(NOW)); wrong.headers().remove(KafkaHeaders.DLT_ORIGINAL_TOPIC);
        wrong.headers().add(KafkaHeaders.DLT_ORIGINAL_TOPIC, "other".getBytes(StandardCharsets.UTF_8));
        assertEquals(INVALID_SOURCE, inspection.assess(wrong, NOW).decision());
    }
    @Test void oldSchemaAndFutureOccurrenceAreNotOrdinaryCandidates() {
        assertEquals(UNSUPPORTED_EVENT, inspection.assess(record(DeliveryEvent.requested(ID, 1L, "SMS", Map.of(), NOW)), NOW).decision());
        assertEquals(MALFORMED_EVENT, inspection.assess(record(event(NOW.plusSeconds(1))), NOW).decision());
    }
    @Test void keyMismatchAndDispatchEventsAreRejected() {
        var source = record(event(NOW));
        var mismatch = new ConsumerRecord<byte[], byte[]>("dlt", 0, 0, new byte[0], source.value());
        source.headers().forEach(h -> mismatch.headers().add(h));
        assertEquals(MALFORMED_EVENT, inspection.assess(mismatch, NOW).decision());
        assertEquals(UNSUPPORTED_EVENT, inspection.assess(record(event(NOW).toDispatchRequested()), NOW).decision());
    }
    @Test void readerRejectsNonLocalBrokerAndUnboundedPageBeforeConnecting() {
        assertThrows(IllegalArgumentException.class, () -> DltInspector.read("remote:9092", "dlt", 0, 0, 1, inspection));
        assertThrows(IllegalArgumentException.class, () -> DltInspector.read("localhost:9092", "dlt", 0, 0, 101, inspection));
    }
}
