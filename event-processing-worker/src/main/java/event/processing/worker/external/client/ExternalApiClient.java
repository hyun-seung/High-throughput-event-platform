package event.processing.worker.external.client;

import event.common.delivery.DeliveryEvent;
import event.processing.worker.external.dto.ProviderDispatchRequest;
import event.processing.worker.external.dto.ProviderDispatchResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Objects;

@Component
@RequiredArgsConstructor
public class ExternalApiClient {

    private final RestClient externalApiRestClient;

    public ProviderDispatchResponse send(DeliveryEvent event) {
        ProviderDispatchRequest request = ProviderDispatchRequest.from(event);

        ProviderDispatchResponse response = externalApiRestClient
                .post()
                .uri("/api/v1/deliveries")
                .header("Idempotency-Key", event.deliveryId())
                .body(request)
                .retrieve()
                .body(ProviderDispatchResponse.class);

        return Objects.requireNonNull(response, "External API response must not be null");
    }
}
