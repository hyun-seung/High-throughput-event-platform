package event.delivery.result;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class FinalizedConsumer {
    private final FinalizedCodec codec;
    private final FinalizedStore store;
    private final MeterRegistry meters;

    public FinalizedConsumer(FinalizedCodec codec, FinalizedStore store, MeterRegistry meters) {
        this.codec = codec;
        this.store = store;
        this.meters = meters;
    }

    @KafkaListener(topics = "${result.topic:delivery.finalized.v1}", containerFactory = "finalizedListenerContainerFactory")
    public void consume(ConsumerRecord<String, byte[]> record) {
        event.common.lifecycle.DeliveryFinalized event;
        try { event = codec.decode(record.key(), record.value()); }
        catch (IllegalArgumentException e) {
            meters.counter("delivery.result.records", "outcome", "invalid").increment();
            throw e;
        }
        store.save(event); // Returns only after both SQL rows commit; RECORD ack follows this return.
    }
}
