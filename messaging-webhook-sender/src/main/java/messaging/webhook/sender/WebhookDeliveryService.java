package messaging.webhook.sender;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.function.LongSupplier;

@Component
@ConditionalOnProperty(prefix = "messaging.webhook.sender", name = "enabled", havingValue = "true")
public class WebhookDeliveryService {
    private final WebhookBatchRepository repository;
    private final CustomerWebhookClient client;
    private final WebhookSenderProperties properties;
    private final MeterRegistry meters;
    private final LongSupplier ticker;

    @Autowired
    public WebhookDeliveryService(WebhookBatchRepository repository, CustomerWebhookClient client,
                                  WebhookSenderProperties properties, MeterRegistry meters) {
        this(repository, client, properties, meters, System::nanoTime);
    }

    WebhookDeliveryService(WebhookBatchRepository repository, CustomerWebhookClient client,
                           WebhookSenderProperties properties, MeterRegistry meters, LongSupplier ticker) {
        this.repository = repository;
        this.client = client;
        this.properties = properties;
        this.meters = meters;
        this.ticker = ticker;
    }

    /** True only when successful work filled a turn and may continue at the queue's tail. */
    public boolean deliver(long clientId) {
        var destination = properties.customers().get(clientId);
        if (destination == null) return false;
        long started = ticker.getAsLong();
        for (int batch = 0; batch < properties.maxBatchesPerTurn(); batch++) {
            if (Thread.currentThread().isInterrupted()) return false;
            // Finish a claimed batch before yielding; the time budget is checked between batches.
            if (batch > 0 && ticker.getAsLong() - started >= properties.maxTurnDuration().toNanos()) return true;
            var reserved = repository.claim(clientId, destination, properties);
            if (reserved.isEmpty()) return false;
            var claim = reserved.get();
            if (!repository.owns(claim)) return false;
            var timer = Timer.start(meters);
            var result = client.send(claim, destination);
            timer.stop(meters.timer("messaging.webhook.http.duration", "acknowledged", Boolean.toString(result.acknowledged())));
            boolean completed = repository.complete(claim, result, properties.retryAfter(claim.attempt()));
            if (completed) {
                meters.counter("messaging.webhook.attempts", "outcome", result.acknowledged() ? "delivered"
                        : claim.attempt() >= 21 ? "exhausted" : "retry").increment();
                meters.summary("messaging.webhook.batch.size").record(claim.size());
            }
            if (!completed || !result.acknowledged()) return false;
        }
        return !Thread.currentThread().isInterrupted();
    }
}
