package event.delivery.dispatch.external.client;

import event.common.delivery.DeliveryEvent;
import event.common.metrics.DeliveryAudit;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchRequest;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.port.DeliveryProviderClient;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.ResourceAccessException;

import java.net.SocketException;
import java.net.SocketTimeoutException;

@Component
@RequiredArgsConstructor
public class ExternalApiClient implements DeliveryProviderClient {

    private final RestClient externalApiRestClient;
    private final DispatchProperties dispatchProperties;

    @Override
    public ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey) {
        ProviderDispatchRequest request = ProviderDispatchRequest.from(event);

        try {
            ResponseEntity<ProviderDispatchResponse> response = externalApiRestClient
                    .post()
                    .uri("/api/v1/deliveries")
                    .header("Idempotency-Key", idempotencyKey)
                    .body(request)
                    .retrieve()
                    .onStatus(HttpStatusCode::isError, (sent, received) -> { })
                    .toEntity(ProviderDispatchResponse.class);

            return ProviderResponseClassifier.classify(event.deliveryId(), response.getStatusCode().value(), response.getBody());
        } catch (ProviderFailureException failure) {
            throw observedFailure(event, idempotencyKey, failure);
        } catch (ResourceAccessException failure) {
            throw observedFailure(event, idempotencyKey,
                    new ProviderFailureException(ProviderFailureException.Kind.NO_RESPONSE));
        } catch (RestClientException failure) {
            throw observedFailure(event, idempotencyKey,
                    new ProviderFailureException(isTransportFailure(failure)
                            ? ProviderFailureException.Kind.NO_RESPONSE : ProviderFailureException.Kind.INVALID_RESPONSE));
        }
    }

    private boolean isTransportFailure(Throwable failure) {
        // RestClient also wraps I/O during status/body extraction in a plain RestClientException.
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ResourceAccessException || cause instanceof SocketTimeoutException
                    || cause instanceof SocketException) {
                return true;
            }
        }
        return false;
    }

    private ProviderFailureException observedFailure(DeliveryEvent event, String attemptId, ProviderFailureException failure) {
        String outcome = failure.kind() == ProviderFailureException.Kind.NO_RESPONSE ? "transport_error" : "http_error";
        DeliveryAudit.record(event, "provider", outcome, attemptId, dispatchProperties.provider(), failure.kind().name());
        return failure;
    }
}
