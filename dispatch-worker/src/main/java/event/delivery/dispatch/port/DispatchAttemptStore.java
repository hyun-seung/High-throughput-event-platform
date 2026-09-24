package event.delivery.dispatch.port;

import event.common.delivery.DeliveryEvent;
import event.delivery.dispatch.model.DispatchAttempt;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.model.DispatchFailureDecision;

import java.time.Instant;

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
}
