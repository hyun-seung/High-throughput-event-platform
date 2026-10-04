package messaging.api.delivery.kafka;

import messaging.common.delivery.DeliveryEvent;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

public interface DeliveryEventPublisher {

    CompletableFuture<SendResult<String, DeliveryEvent>> send(DeliveryEvent event);
}
