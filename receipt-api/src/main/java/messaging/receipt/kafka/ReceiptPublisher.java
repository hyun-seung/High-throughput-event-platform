package messaging.receipt.kafka;

import messaging.common.receipt.ReceiptEvent;
import java.util.concurrent.CompletableFuture;

public interface ReceiptPublisher {
    CompletableFuture<Void> publish(ReceiptEvent event);
}
