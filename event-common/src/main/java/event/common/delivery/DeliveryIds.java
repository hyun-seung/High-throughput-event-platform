package event.common.delivery;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public final class DeliveryIds {

    private DeliveryIds() {
    }

    public static String deliveryId(long tenantId, String idempotencyKey) {
        return nameBasedId("delivery:" + tenantId + ":" + idempotencyKey);
    }

    public static String eventId(String deliveryId, DeliveryEventType eventType) {
        return nameBasedId("event:" + deliveryId + ":" + eventType.name() + ":v1");
    }

    private static String nameBasedId(String source) {
        return UUID.nameUUIDFromBytes(source.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
