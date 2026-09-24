package event.delivery.dispatch.port;

import event.common.delivery.DeliveryEvent;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.model.DispatchFailureDecision;
import event.delivery.dispatch.model.SecondaryRoute;

import java.time.Instant;
import java.time.Duration;
import java.util.Optional;

public interface DispatchAttemptStore {

    DispatchClaim claim(
            DeliveryEvent event,
            String attemptId,
            String provider,
            Instant now,
            Instant leaseUntil,
            Instant primaryDeadline
    );

    void markAccepted(DispatchAttempt attempt, Instant providerProcessedAt, Instant now);

    void recordFailure(DispatchAttempt attempt, DispatchFailureDecision decision);

    Optional<SecondaryRoute> prepareSecondary(DeliveryEvent event, String primaryAttemptId, String provider, Duration ttl);

    DispatchClaim claimSecondary(DeliveryEvent event, SecondaryRoute route, Instant now, Instant leaseUntil);
}
