package event.delivery.dispatch.external.client;

import event.common.delivery.DeliveryEvent;
import event.common.metrics.DeliveryAudit;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchRequest;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.port.DeliveryProviderClient;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.ResourceAccessException;

import java.util.Objects;

@Component
@RequiredArgsConstructor
public class ExternalApiClient implements DeliveryProviderClient {

    private final RestClient externalApiRestClient;
    private final DispatchProperties dispatchProperties;

    @Override
    public ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey) {
        ProviderDispatchRequest request = ProviderDispatchRequest.from(event);

        try {
            ProviderDispatchResponse response = externalApiRestClient
                    .post()
                    .uri("/api/v1/deliveries")
                    .header("Idempotency-Key", idempotencyKey)
                    .body(request)
                    .retrieve()
                    .body(ProviderDispatchResponse.class);

            return Objects.requireNonNull(response, "External API response must not be null");
        } catch (RestClientResponseException failure) {
            DeliveryAudit.record(event, "provider", "http_error", idempotencyKey, dispatchProperties.provider(),
                    Integer.toString(failure.getStatusCode().value()));
            throw failure;
        } catch (ResourceAccessException failure) {
            DeliveryAudit.record(event, "provider", "transport_error", idempotencyKey, dispatchProperties.provider(), "io_error");
            throw failure;
        }
    }
}
