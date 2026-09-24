package event.api.delivery.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Map;

public record DeliveryRequest(
        @NotBlank
        @Size(max = 100)
        String deliveryType,

        @NotNull
        Map<String, Object> payload,
        Boolean fallbackAllowed
) {
    public DeliveryRequest {
        fallbackAllowed = Boolean.TRUE.equals(fallbackAllowed);
    }
    public DeliveryRequest(String deliveryType, Map<String, Object> payload) {
        this(deliveryType, payload, false);
    }
}
