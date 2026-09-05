package event.processing.worker.external.client;

import event.contract.message.EventMessage;
import event.processing.worker.external.dto.ExternalEventRequest;
import event.processing.worker.external.dto.ExternalEventResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Objects;

@Component
@RequiredArgsConstructor
public class ExternalApiClient {

    private final RestClient externalApiRestClient;

    public ExternalEventResponse send(EventMessage message) {
        ExternalEventRequest request = ExternalEventRequest.from(message);

        ExternalEventResponse response = externalApiRestClient
                .post()
                .uri("/api/v1/events")
                .body(request)
                .retrieve()
                .body(ExternalEventResponse.class);

        return Objects.requireNonNull(response, "External API response must not be null");
    }
}