package messaging.carrier.sender;

import messaging.common.messages.HttpSendCommand;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import java.util.Objects;

/** Carrier URLs differ by pod; the wire request and response parser are shared. */
public class CarrierProviderClient {
    private final RestClient client;
    private final JsonMapper mapper;
    private final String requestPath;

    public CarrierProviderClient(RestClient client, JsonMapper mapper, String requestPath) {
        this.client = Objects.requireNonNull(client);
        this.mapper = Objects.requireNonNull(mapper);
        this.requestPath = Objects.requireNonNull(requestPath);
    }

    public CarrierProviderReply send(HttpSendCommand command) {
        try {
            var response = client.post().uri(requestPath).body(command.request()).retrieve()
                    .onStatus(HttpStatusCode::isError, (request, received) -> { })
                    .toEntity(String.class);
            int status = response.getStatusCode().value();
            if (status == 200) return new CarrierProviderReply.Accepted();
            ProviderFailure failure;
            try {
                failure = mapper.readValue(response.getBody(), ProviderFailure.class);
            } catch (RuntimeException malformed) {
                failure = null;
            }
            ProviderError error = failure == null ? null : failure.error();
            return new CarrierProviderReply.Failed(status, failure == null ? null : failure.status(),
                    error == null ? null : error.code(), error == null ? null : error.message());
        } catch (ResourceAccessException noResponse) {
            return new CarrierProviderReply.TimedOut();
        }
    }

    private record ProviderFailure(String status, ProviderError error) { }
    private record ProviderError(String code, String message) { }
}
