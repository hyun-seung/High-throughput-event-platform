package event.delivery.dispatch.lifecycle;

import event.common.lifecycle.DeliveryFinalized;

@FunctionalInterface
public interface FinalizedPublisher {
    /** Returns only after broker acknowledgement; uncertainty must throw and retain PENDING. */
    void publish(DeliveryFinalized result);
}
