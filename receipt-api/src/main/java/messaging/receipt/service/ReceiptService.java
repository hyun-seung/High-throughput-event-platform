package messaging.receipt.service;

import messaging.common.metrics.DeliveryMetrics;
import messaging.common.receipt.ReceiptEvent;
import messaging.receipt.api.ReceiptRequest;
import messaging.receipt.config.ReceiptProperties;
import messaging.receipt.kafka.ReceiptPublisher;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;

@Service
public class ReceiptService {
    private final ReceiptPublisher publisher;
    private final ReceiptProperties properties;
    private final DeliveryMetrics metrics;
    private final Clock clock;

    public ReceiptService(ReceiptPublisher publisher, ReceiptProperties properties, DeliveryMetrics metrics, Clock clock) {
        this.publisher = publisher;
        this.properties = properties;
        this.metrics = metrics;
        this.clock = clock;
    }

    public CompletableFuture<ReceiptAccepted> accept(String provider, ReceiptRequest request) {
        var event = ReceiptEvent.received(request.receiptId(), request.deliveryId(), request.attemptId(), provider,
                properties.routeOrder(provider), request.outcome(), request.code(), request.occurredAt(), clock.instant(), request.invocation());
        try {
            return metrics.measureAsync(DeliveryMetrics.Stage.RECEIPT_PUBLISH, () -> publisher.publish(event))
                    .handle((ignored, failure) -> {
                        audit(event, failure == null ? "receipt_accepted" : "receipt_unconfirmed");
                        if (failure != null) throw new ReceiptAcceptanceException(failure);
                        return new ReceiptAccepted(event.eventId(), "RECEIVED");
                    });
        } catch (RuntimeException failure) {
            audit(event, "receipt_unconfirmed");
            return CompletableFuture.failedFuture(new ReceiptAcceptanceException(failure));
        }
    }

    private void audit(ReceiptEvent event, String outcome) {
        LoggerFactory.getLogger("delivery.audit").atInfo().addKeyValue("stage", "receipt")
                .addKeyValue("outcome", outcome).addKeyValue("provider", event.provider())
                .addKeyValue("eventId", event.eventId()).log("Provider receipt ingestion observed");
    }

    public record ReceiptAccepted(String eventId, String status) { }
    public static class ReceiptAcceptanceException extends RuntimeException {
        public ReceiptAcceptanceException(Throwable cause) { super("Receipt storage unconfirmed", cause); }
    }
}
