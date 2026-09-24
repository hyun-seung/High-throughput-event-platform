package event.receipt.kafka;

import event.common.delivery.DeliveryTopics;
import event.common.receipt.ReceiptEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaReceiptPublisher implements ReceiptPublisher {
    private final KafkaTemplate<String, ReceiptEvent> template;
    private final String topic;

    public KafkaReceiptPublisher(KafkaTemplate<String, ReceiptEvent> template,
                                 @Value("${receipt.topic:" + DeliveryTopics.RECEIPT_RECEIVED + "}") String topic) {
        this.template = template;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> publish(ReceiptEvent event) {
        return template.send(topic, event.deliveryId(), event).thenApply(ignored -> null);
    }
}
