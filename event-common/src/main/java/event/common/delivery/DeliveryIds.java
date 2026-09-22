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

    public static String attemptId(
            String deliveryId,
            String provider,
            int routeOrder,
            int attemptNumber
    ) {
        return nameBasedId("attempt:" + deliveryId + ":" + provider + ":" + routeOrder + ":" + attemptNumber);
    }

    private static String nameBasedId(String source) {
        return UUID.nameUUIDFromBytes(source.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
