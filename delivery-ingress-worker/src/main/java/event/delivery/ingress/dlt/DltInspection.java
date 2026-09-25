package event.delivery.ingress.dlt;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryEventType;
import event.common.delivery.DeliveryIds;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.KafkaHeaders;
import tools.jackson.databind.json.JsonMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;

/** Read-only triage. No decision here authorizes replay: durable execution/history checks come next. */
public final class DltInspection {
    public enum Decision { INVALID_SOURCE, MALFORMED_EVENT, UNSUPPORTED_EVENT, IDEMPOTENCY_CONFLICT,
        EXPIRED_REQUIRES_FINALIZATION, REQUIRES_STATE_CHECK }
    public record Source(String topic, int partition, long offset) { }
    public record Assessment(String topic, int partition, long offset, Source source, String requestKey,
                             Long tenantId, Instant occurredAt, Instant primaryDeadline, Decision decision,
                             String valueSha256, int valueBytes) { }
    private final JsonMapper mapper = JsonMapper.builder().enable(tools.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private final String sourceTopic;
    private final Duration primaryTtl;

    public DltInspection(String sourceTopic, Duration primaryTtl) {
        if (sourceTopic == null || !sourceTopic.matches("[A-Za-z0-9._-]{1,249}") || sourceTopic.equals(".") || sourceTopic.equals("..")
                || primaryTtl == null || primaryTtl.isZero() || primaryTtl.isNegative() || primaryTtl.compareTo(Duration.ofDays(7)) > 0) {
            throw new IllegalArgumentException("Invalid source topic or primary TTL");
        }
        this.sourceTopic = sourceTopic;
        this.primaryTtl = primaryTtl;
    }

    public Assessment assess(ConsumerRecord<byte[], byte[]> record, Instant now) {
        Source source = source(record.headers());
        DeliveryEvent event = null;
        Instant deadline = null;
        Decision decision = Decision.INVALID_SOURCE;
        if (source != null) {
            decision = Decision.MALFORMED_EVENT;
            try {
                if (record.value() == null || record.value().length > 1024 * 1024) throw new IllegalArgumentException();
                event = mapper.readValue(record.value(), DeliveryEvent.class);
                if (event == null || event.deliveryId() == null || !event.deliveryId().matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                        || !event.deliveryId().equals(event.requestKey()) || event.tenantId() == null || event.tenantId() <= 0
                        || event.payload() == null || event.deliveryType() == null || event.deliveryType().isBlank()
                        || event.occurredAt() == null || event.occurredAt().isAfter(now)
                        || record.key() == null || !java.util.Arrays.equals(record.key(), event.deliveryId().getBytes(StandardCharsets.UTF_8))) {
                    throw new IllegalArgumentException();
                }
                if (event.schemaVersion() != 2 || event.eventType() != DeliveryEventType.DELIVERY_REQUESTED
                        || !DeliveryIds.eventId(event.deliveryId(), DeliveryEventType.DELIVERY_REQUESTED).equals(event.eventId())) {
                    decision = Decision.UNSUPPORTED_EVENT;
                } else {
                    deadline = event.occurredAt().plus(primaryTtl);
                    boolean conflict = conflict(record.headers(), KafkaHeaders.DLT_EXCEPTION_FQCN)
                            || conflict(record.headers(), KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
                    decision = conflict ? Decision.IDEMPOTENCY_CONFLICT : !now.isBefore(deadline)
                            ? Decision.EXPIRED_REQUIRES_FINALIZATION : Decision.REQUIRES_STATE_CHECK;
                }
            } catch (RuntimeException invalid) {
                // Never expose parser exceptions, payload, keys or exception header text in the output.
                event = null;
                deadline = null;
                decision = Decision.MALFORMED_EVENT;
            }
        }
        return new Assessment(record.topic(), record.partition(), record.offset(), source,
                event == null ? null : event.requestKey(), event == null ? null : event.tenantId(),
                event == null ? null : event.occurredAt(), deadline, decision,
                digest(record.value()), record.value() == null ? 0 : record.value().length);
    }

    private Source source(Headers headers) {
        try {
            byte[] topic = single(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC);
            byte[] partition = single(headers, KafkaHeaders.DLT_ORIGINAL_PARTITION);
            byte[] offset = single(headers, KafkaHeaders.DLT_ORIGINAL_OFFSET);
            if (topic == null || !java.util.Arrays.equals(topic, sourceTopic.getBytes(StandardCharsets.UTF_8))
                    || partition == null || partition.length != 4 || offset == null || offset.length != 8) return null;
            int p = ByteBuffer.wrap(partition).getInt();
            long o = ByteBuffer.wrap(offset).getLong();
            return p < 0 || o < 0 ? null : new Source(sourceTopic, p, o);
        } catch (IllegalArgumentException invalid) { return null; }
    }

    private static boolean conflict(Headers headers, String key) {
        for (var header : headers.headers(key)) {
            if (java.util.Arrays.equals(header.value(),
                    "event.delivery.ingress.repository.IdempotencyConflictException".getBytes(StandardCharsets.UTF_8))) return true;
        }
        return false;
    }

    private static byte[] single(Headers headers, String key) {
        var iterator = headers.headers(key).iterator();
        if (!iterator.hasNext()) return null;
        byte[] value = iterator.next().value();
        if (iterator.hasNext()) throw new IllegalArgumentException("Ambiguous source headers");
        return value;
    }

    private static String digest(byte[] value) {
        if (value == null) return null;
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
}
