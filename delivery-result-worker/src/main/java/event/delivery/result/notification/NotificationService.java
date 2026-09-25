package event.delivery.result.notification;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class NotificationService {
    private final NotificationRepository repository;
    private final CustomerNotificationClient client;
    private final NotificationProperties settings;
    private final MeterRegistry meters;

    public NotificationService(NotificationRepository repository, CustomerNotificationClient client, NotificationProperties settings, MeterRegistry meters) {
        this.repository = repository; this.client = client; this.settings = settings; this.meters = meters;
    }

    public void deliver(long tenantId) {
        var destination = settings.customers().get(tenantId);
        if (destination == null) return;
        var reserved = repository.claim(tenantId, destination, settings);
        if (reserved.isEmpty()) return;
        var claim = reserved.get();
        if (!repository.owns(claim)) { count("stale_claim"); return; }
        var sample = Timer.start(meters);
        var result = client.send(claim, destination);
        sample.stop(meters.timer("delivery.notification.http.duration", "outcome", result.acknowledged() ? "acknowledged" : "failed"));
        count(result.acknowledged() ? "http_acknowledged" : "http_failed");
        if (!repository.complete(claim, result.acknowledged(), result.error(), settings.retryAfter(claim.attempt()))) {
            count("stale_completion");
            return;
        }
        count(result.acknowledged() ? "delivered" : claim.attempt() >= 21 ? "exhausted" : "retry_scheduled");
        meters.summary("delivery.notification.batch.size").record(claim.size());
    }

    private void count(String outcome) { meters.counter("delivery.notification.events", "outcome", outcome).increment(); }
}
