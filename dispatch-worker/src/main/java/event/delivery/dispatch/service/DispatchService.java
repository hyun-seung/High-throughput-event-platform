package event.delivery.dispatch.service;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
import event.common.metrics.DeliveryMetrics;
import event.common.metrics.DeliveryAudit;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.model.DispatchClaim;
import event.delivery.dispatch.port.DeliveryProviderClient;
import event.delivery.dispatch.port.DispatchAttemptStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchService {

    private static final int INITIAL_ROUTE_ORDER = 1;
    private static final int INITIAL_ATTEMPT_NUMBER = 1;

    private final DispatchAttemptStore dispatchAttemptStore;
    private final DeliveryProviderClient deliveryProviderClient;
    private final DispatchProperties dispatchProperties;
    private final Clock dispatchClock;
    private final DeliveryMetrics metrics;

    public void dispatch(DeliveryEvent event) {
        metrics.measure(DeliveryMetrics.Stage.DISPATCH_PROCESS, () -> process(event));
    }

    private void process(DeliveryEvent event) {
        String attemptId = DeliveryIds.attemptId(
                event.deliveryId(),
                dispatchProperties.provider(),
                INITIAL_ROUTE_ORDER,
                INITIAL_ATTEMPT_NUMBER
        );
        Instant now = dispatchClock.instant();
        DispatchClaim claim = metrics.measure(DeliveryMetrics.Stage.DISPATCH_CLAIM, () -> dispatchAttemptStore.claim(
                event,
                attemptId,
                dispatchProperties.provider(),
                now,
                now.plus(dispatchProperties.leaseDuration())
        ));

        switch (claim.status()) {
            case ALREADY_ACCEPTED -> {
                DeliveryAudit.record(event, "dispatch", "duplicate", attemptId, dispatchProperties.provider(), "already_accepted");
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_DUPLICATE);
                log.debug("Completed dispatch skipped. deliveryId={}, attemptId={}", event.deliveryId(), attemptId);
            }
            case IN_PROGRESS -> {
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_IN_PROGRESS);
                throw new DispatchAttemptInProgressException(event.deliveryId());
            }
            case REVIEW_REQUIRED -> {
                DeliveryAudit.record(event, "dispatch", "review_required", attemptId, dispatchProperties.provider(), "result_unknown");
                metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_REVIEW);
                log.warn(
                    "Dispatch requires operational review; provider not called. deliveryId={}, attemptId={}",
                    event.deliveryId(), attemptId);
            }
            case CLAIMED -> invokeProvider(event, claim, attemptId);
        }
    }

    private void invokeProvider(DeliveryEvent event, DispatchClaim claim, String attemptId) {
        ProviderDispatchResponse response = metrics.measure(DeliveryMetrics.Stage.DISPATCH_HTTP, () -> {
            ProviderDispatchResponse received = deliveryProviderClient.send(event, attemptId);
            if (!Boolean.TRUE.equals(received.accepted())) {
                throw new IllegalStateException("Provider did not accept dispatch. attemptId=" + attemptId);
            }
            return received;
        });

        metrics.measure(DeliveryMetrics.Stage.DISPATCH_STORE, () -> dispatchAttemptStore.markAccepted(
                claim.attempt(),
                response.processedAt(),
                dispatchClock.instant()
        ));
        metrics.outcome(DeliveryMetrics.Outcome.DISPATCH_ACCEPTED);
        metrics.accepted(event.occurredAt(), dispatchClock.instant());
        DeliveryAudit.record(event, "dispatch", "accepted", attemptId, dispatchProperties.provider(), "accepted");

        log.debug("Dispatch completed. deliveryId={}, attemptId={}, processedAt={}",
                event.deliveryId(), attemptId, response.processedAt());
    }
}
