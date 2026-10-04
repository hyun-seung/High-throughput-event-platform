package messaging.delivery.dispatch.port;

import messaging.common.delivery.DeliveryEvent;
import messaging.delivery.dispatch.external.dto.ProviderDispatchResponse;

public interface DeliveryProviderClient {

    ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey);

    default ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey, int invocation) {
        return send(event, idempotencyKey);
    }
}
