package event.api.delivery.dto;

/** deliveryId is the stable admission/request key; finalized results expose their execution ID and requestKey. */
public record DeliveryResponse(
        String deliveryId,
        String status
) {
}
