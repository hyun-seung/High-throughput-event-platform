package messaging.common.messages;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** A single Kafka record containing one authenticated webhook request of 1-100 results. */
public record MessageWebhookBatch(String traceId, String source, HttpCarrier carrier, Instant receivedAt,
                                  List<MessageWebhookResult> results) {
    public MessageWebhookBatch {
        if (traceId == null || traceId.isBlank()) throw new IllegalArgumentException("Trace ID is required");
        if (!"WEBHOOK".equals(source)) throw new IllegalArgumentException("Invalid result source");
        Objects.requireNonNull(carrier);
        Objects.requireNonNull(receivedAt);
        Objects.requireNonNull(results);
        if (results.isEmpty() || results.size() > 100 || results.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Webhook must contain 1-100 results");
        }
        var ids = new HashSet<String>();
        for (MessageWebhookResult result : results) {
            if (!ids.add(result.clientMsgId())) throw new IllegalArgumentException("Duplicate clientMsgId in webhook");
        }
        results = List.copyOf(results);
    }
}
