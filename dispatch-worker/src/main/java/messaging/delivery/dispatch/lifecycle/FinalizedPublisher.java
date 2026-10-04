package messaging.delivery.dispatch.lifecycle;

import messaging.common.lifecycle.DeliveryFinalized;

@FunctionalInterface
public interface FinalizedPublisher {
    /** Returns only after broker acknowledgement; uncertainty must throw and retain PENDING. */
    void publish(DeliveryFinalized result);
}
