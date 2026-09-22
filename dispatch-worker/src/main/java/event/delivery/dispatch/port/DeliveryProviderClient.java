package event.delivery.dispatch.port;

import event.common.delivery.DeliveryEvent;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;

public interface DeliveryProviderClient {

    ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey);
}
