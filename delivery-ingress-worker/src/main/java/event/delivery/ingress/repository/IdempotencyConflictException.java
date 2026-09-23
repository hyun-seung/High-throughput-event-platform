package event.delivery.ingress.repository;

/** Replaying this input cannot resolve a conflict with an already accepted request. */
public class IdempotencyConflictException extends IllegalStateException {

    public IdempotencyConflictException(String deliveryId) {
        super("Idempotency key collision with a different delivery request. deliveryId=" + deliveryId);
    }
}
