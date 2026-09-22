package event.delivery.dispatch.external.client;

import event.common.delivery.DeliveryEvent;
import event.delivery.dispatch.external.dto.ProviderDispatchRequest;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.port.DeliveryProviderClient;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Objects;

@Component
@RequiredArgsConstructor
public class ExternalApiClient implements DeliveryProviderClient {

    private final RestClient externalApiRestClient;

    @Override
    public ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey) {
        ProviderDispatchRequest request = ProviderDispatchRequest.from(event);

        ProviderDispatchResponse response = externalApiRestClient
                .post()
                .uri("/api/v1/deliveries")
                .header("Idempotency-Key", idempotencyKey)
                .body(request)
                .retrieve()
                .body(ProviderDispatchResponse.class);

        return Objects.requireNonNull(response, "External API response must not be null");
    }
}
