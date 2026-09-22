package event.delivery.dispatch.service;

public class DispatchAttemptInProgressException extends RuntimeException {

    public DispatchAttemptInProgressException(String deliveryId) {
        super("Dispatch attempt is already in progress. deliveryId=" + deliveryId);
    }
}
