package event.delivery.dispatch.retry;

import event.delivery.dispatch.repository.DispatchAttemptRepository;
import event.delivery.dispatch.service.*;
import event.delivery.dispatch.config.*;
import event.delivery.dispatch.model.DispatchClaimStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.time.Clock;

@Component
public class RetryConsumer {
    private final DispatchAttemptRepository store;
    private final DispatchService primary;
    private final SecondaryDispatchService secondary;
    private final DispatchProperties config;
    private final SecondaryDispatchProperties secondaryConfig;
    private final Clock clock;
    public RetryConsumer(DispatchAttemptRepository store, DispatchService primary, SecondaryDispatchService secondary,
                         DispatchProperties config, SecondaryDispatchProperties secondaryConfig, Clock clock) {
        this.store = store; this.primary = primary; this.secondary = secondary; this.config = config;
        this.secondaryConfig = secondaryConfig; this.clock = clock;
    }
    @KafkaListener(topics = RetryConfiguration.TOPIC, groupId = "delivery-retry-worker", containerFactory = "retryListenerContainerFactory")
    public void consume(RetryCommand command) {
        String provider = command.source().routeOrder() == 1 ? config.provider() : secondaryConfig.provider();
        if (!provider.equals(command.source().provider())) throw new IllegalStateException("Retry provider is not configured");
        var now = clock.instant();
        if (!now.isBefore(command.source().deadline())) return; // Durable lifecycle index owns expiry.
        if (now.isBefore(command.notBefore())) throw new DispatchRetryPendingException(command.event().deliveryId());
        var claim = store.claimRetry(command, now, now.plus(config.leaseDuration()));
        if (claim.status() != DispatchClaimStatus.CLAIMED) return;
        if (command.source().routeOrder() == 1) primary.invokeRetry(command.event(), claim);
        else secondary.invokeRetry(command.event(), claim.attempt());
    }
}
