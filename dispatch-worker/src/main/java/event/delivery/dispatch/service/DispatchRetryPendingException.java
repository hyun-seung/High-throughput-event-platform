package event.delivery.dispatch.service;

/** Keep the original Kafka record uncommitted until its durable reservation is resolved. */
public class DispatchRetryPendingException extends IllegalStateException {
    public DispatchRetryPendingException(String deliveryId) {
        super("Dispatch retry is durably scheduled. deliveryId=" + deliveryId);
    }
}
