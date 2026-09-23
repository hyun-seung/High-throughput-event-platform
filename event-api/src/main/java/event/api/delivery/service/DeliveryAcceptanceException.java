package event.api.delivery.service;

/** Publication failed or its acknowledgement was not observed; the record may still exist. */
public class DeliveryAcceptanceException extends RuntimeException {

    public DeliveryAcceptanceException(Throwable cause) {
        super("Delivery acceptance could not be confirmed", cause);
    }
}
