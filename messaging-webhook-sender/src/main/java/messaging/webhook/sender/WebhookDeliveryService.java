package messaging.webhook.sender;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "messaging.webhook.sender", name = "enabled", havingValue = "true")
public class WebhookDeliveryService {
    private final WebhookBatchRepository repository;
    private final CustomerWebhookClient client;
    private final WebhookSenderProperties properties;
    private final MeterRegistry meters;

    public WebhookDeliveryService(WebhookBatchRepository repository, CustomerWebhookClient client,
                                  WebhookSenderProperties properties, MeterRegistry meters) {
        this.repository = repository;
        this.client = client;
        this.properties = properties;
        this.meters = meters;
    }

    public void deliver(long clientId) {
        var destination = properties.customers().get(clientId);
        if (destination == null) return;
        var reserved = repository.claim(clientId, destination, properties);
        if (reserved.isEmpty()) return;
        var claim = reserved.get();
        if (!repository.owns(claim)) return;
        var timer = Timer.start(meters);
        var result = client.send(claim, destination);
        timer.stop(meters.timer("messaging.webhook.http.duration", "acknowledged", Boolean.toString(result.acknowledged())));
        if (repository.complete(claim, result, properties.retryAfter(claim.attempt()))) {
            meters.counter("messaging.webhook.attempts", "outcome", result.acknowledged() ? "delivered"
                    : claim.attempt() >= 21 ? "exhausted" : "retry").increment();
            meters.summary("messaging.webhook.batch.size").record(claim.size());
        }
    }
}
