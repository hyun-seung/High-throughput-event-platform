package messaging.webhook.receive;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageWebhookBatch;
import messaging.common.messages.MessageWebhookResult;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class MessageWebhookReceiveService {
    private final MessageWebhookPublisher publisher;
    private final Clock clock;

    public MessageWebhookReceiveService(MessageWebhookPublisher publisher, Clock webhookClock) {
        this.publisher = Objects.requireNonNull(publisher);
        this.clock = Objects.requireNonNull(webhookClock);
    }

    public CompletableFuture<Accepted> accept(HttpCarrier carrier, List<MessageWebhookResult> results) {
        if (carrier == null || results == null) throw new IllegalArgumentException("Webhook carrier and results are required");
        String traceId = UUID.randomUUID().toString().replace("-", "");
        MessageWebhookBatch batch = new MessageWebhookBatch(traceId, "WEBHOOK", carrier, clock.instant(), results);
        try {
            return publisher.publish(batch).handle((ignored, failure) -> {
                if (failure != null) throw new PublicationUnconfirmed(failure);
                return new Accepted(traceId, "RECEIVED");
            });
        } catch (RuntimeException failed) {
            return CompletableFuture.failedFuture(new PublicationUnconfirmed(failed));
        }
    }

    public record Accepted(String traceId, String status) { }
    public static class PublicationUnconfirmed extends RuntimeException {
        public PublicationUnconfirmed(Throwable cause) { super("Webhook Kafka publication unconfirmed", cause); }
    }
}
