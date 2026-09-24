package event.receipt.kafka;

import event.common.receipt.ReceiptEvent;
import java.util.concurrent.CompletableFuture;

public interface ReceiptPublisher {
    CompletableFuture<Void> publish(ReceiptEvent event);
}
