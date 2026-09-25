package event.delivery.ingress.dlt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Exact inspected input plus the immutable authorized command; no Kafka retention dependency on resume. */
public record DltRecoveryCheckpoint(DltRecoveryPlanner.Plan plan, String topic, int partition, long offset,
                                    byte[] key, byte[] value, List<Header> headers) {
    public record Header(String name, byte[] value) { }

    public static DltRecoveryCheckpoint capture(DltRecoveryPlanner.Plan plan, ConsumerRecord<byte[], byte[]> record) {
        var headers = new java.util.ArrayList<Header>();
        record.headers().forEach(h -> headers.add(new Header(h.key(), h.value())));
        var checkpoint = new DltRecoveryCheckpoint(plan, record.topic(), record.partition(), record.offset(),
                record.key(), record.value(), List.copyOf(headers));
        checkpoint.validate();
        return checkpoint;
    }

    public ConsumerRecord<byte[], byte[]> record() {
        var record = new ConsumerRecord<byte[], byte[]>(topic, partition, offset, key, value);
        headers.forEach(h -> record.headers().add(h.name(), h.value()));
        return record;
    }

    public void validate() {
        if (plan == null || !plan.eligible()) throw new IllegalArgumentException("Eligible checkpoint required");
        var original = plan.preview().dlt();
        var inspection = new DltInspection(original.source().topic(),
                Duration.between(original.occurredAt(), original.primaryDeadline()));
        var actual = inspection.assess(record(), Instant.now().isBefore(original.occurredAt())
                ? original.occurredAt() : Instant.now());
        if (!original.source().equals(actual.source()) || !original.valueSha256().equals(actual.valueSha256())
                || !original.requestKey().equals(actual.requestKey()) || !original.tenantId().equals(actual.tenantId())
                || !original.occurredAt().equals(actual.occurredAt()) || !original.topic().equals(topic)
                || original.partition() != partition || original.offset() != offset
                || (actual.decision() != DltInspection.Decision.REQUIRES_STATE_CHECK
                    && actual.decision() != DltInspection.Decision.EXPIRED_REQUIRES_FINALIZATION)) {
            throw new IllegalArgumentException("Recovery checkpoint identity conflict");
        }
    }
}
