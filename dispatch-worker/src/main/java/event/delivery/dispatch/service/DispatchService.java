package event.delivery.dispatch.service;

import event.common.delivery.DeliveryEvent;
import event.common.delivery.DeliveryIds;
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

    public void dispatch(DeliveryEvent event) {
        String attemptId = DeliveryIds.attemptId(
                event.deliveryId(),
                dispatchProperties.provider(),
                INITIAL_ROUTE_ORDER,
                INITIAL_ATTEMPT_NUMBER
        );
        Instant now = dispatchClock.instant();
        DispatchClaim claim = dispatchAttemptStore.claim(
                event,
                attemptId,
                dispatchProperties.provider(),
                now,
                now.plus(dispatchProperties.leaseDuration())
        );

        switch (claim.status()) {
            case ALREADY_ACCEPTED -> log.debug(
                    "Completed dispatch skipped. deliveryId={}, attemptId={}", event.deliveryId(), attemptId);
            case IN_PROGRESS -> throw new DispatchAttemptInProgressException(event.deliveryId());
            case CLAIMED -> invokeProvider(event, claim, attemptId);
        }
    }

    private void invokeProvider(DeliveryEvent event, DispatchClaim claim, String attemptId) {
        ProviderDispatchResponse response = deliveryProviderClient.send(event, attemptId);
        if (!response.accepted()) {
            throw new IllegalStateException("Provider did not accept dispatch. attemptId=" + attemptId);
        }

        dispatchAttemptStore.markAccepted(
                claim.attempt(),
                response.processedAt(),
                dispatchClock.instant()
        );

        log.debug("Dispatch completed. deliveryId={}, attemptId={}, processedAt={}",
                event.deliveryId(), attemptId, response.processedAt());
    }
}
