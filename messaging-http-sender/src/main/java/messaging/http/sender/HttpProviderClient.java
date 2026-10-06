package messaging.http.sender;

import messaging.common.messages.MessageSubmission;
import messaging.common.messages.HttpOutcome;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Instant;
import java.util.Map;
import java.net.SocketException;
import java.net.SocketTimeoutException;

@Component
public class HttpProviderClient {
    private final RestClient client;

    public HttpProviderClient(RestClient providerRestClient) { this.client = providerRestClient; }

    public Observation send(MessageSubmission event, String attemptId, int invocation) {
        var request = new ProviderRequest(event.clientMsgId(), event.clientId(), event.messageCategory().name(),
                event.recipientNumber(), event.payload(), event.receivedAt(), invocation);
        try {
            ResponseEntity<ProviderResponse> response = client.post().uri("/api/v1/deliveries")
                    .header("Idempotency-Key", attemptId).body(request).retrieve()
                    .onStatus(HttpStatusCode::isError, (sent, received) -> { })
                    .toEntity(ProviderResponse.class);
            ProviderResponse body = response.getBody();
            if (body == null || !event.clientMsgId().equals(body.deliveryId())
                    || body.accepted() == null || body.processedAt() == null) {
                return new Observation(HttpOutcome.Kind.INVALID_RESPONSE, null);
            }
            int status = response.getStatusCode().value();
            if (body.accepted()) {
                return status >= 200 && status < 300 && (body.code() == null || "ACCEPTED".equals(body.code()))
                        ? new Observation(HttpOutcome.Kind.ACCEPTED, body.processedAt())
                        : new Observation(HttpOutcome.Kind.INVALID_RESPONSE, null);
            }
            if (status < 200 || status >= 300 && status < 400) {
                return new Observation(HttpOutcome.Kind.INVALID_RESPONSE, null);
            }
            HttpOutcome.Kind kind = switch (body.code() == null ? "" : body.code()) {
                case "RETRY_1S" -> HttpOutcome.Kind.RETRY_1S;
                case "RETRY_10S" -> HttpOutcome.Kind.RETRY_10S;
                case "FALLBACK" -> HttpOutcome.Kind.FALLBACK_REQUIRED;
                case "REJECTED" -> HttpOutcome.Kind.PERMANENT_REJECTION;
                default -> status >= 400 && body.code() == null
                        ? HttpOutcome.Kind.HTTP_ERROR : HttpOutcome.Kind.INVALID_RESPONSE;
            };
            return new Observation(kind, body.processedAt());
        } catch (ResourceAccessException unknown) {
            return new Observation(HttpOutcome.Kind.NO_RESPONSE, null);
        } catch (RestClientException invalid) {
            for (Throwable cause = invalid; cause != null; cause = cause.getCause()) {
                if (cause instanceof SocketException || cause instanceof SocketTimeoutException)
                    return new Observation(HttpOutcome.Kind.NO_RESPONSE, null);
            }
            return new Observation(HttpOutcome.Kind.INVALID_RESPONSE, null);
        }
    }

    record ProviderRequest(String deliveryId, long tenantId, String deliveryType, String recipientNumber,
                           Map<String, Object> payload, Instant occurredAt, int invocation) { }
    record ProviderResponse(String deliveryId, Boolean accepted, Instant processedAt, String code) { }
    public record Observation(HttpOutcome.Kind kind, Instant providerProcessedAt) { }
}
