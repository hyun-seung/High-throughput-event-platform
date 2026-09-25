package event.delivery.dispatch.retry;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.delivery.dispatch.model.DispatchAttempt;
import java.time.Instant;

/** Fixed source and target. Redelivery never manufactures a new invocation. */
public record RetryCommand(DeliveryEvent event, DispatchAttempt source, int targetRetryCount, Instant notBefore) {
    public RetryCommand {
        if (event == null || source == null || notBefore == null || event.schemaVersion() != 2
                || !event.deliveryId().equals(source.deliveryId()) || source.version() < 1
                || source.retryCount() < 0 || targetRetryCount != source.retryCount() + 1 || targetRetryCount > 3
                || source.routeOrder() < 1 || source.routeOrder() > 2
                || !DeliveryIds.attemptId(event.deliveryId(), source.provider(), source.routeOrder(), 1).equals(source.attemptId()))
            throw new IllegalArgumentException("Invalid fixed-invocation retry command");
    }
}
