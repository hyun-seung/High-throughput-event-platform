package messaging.carrier.sender;

import messaging.common.messages.HttpCarrier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CarrierSenderPropertiesTest {
    @Test
    void refusesToRunWithAnotherCarriersTopicOrMissingProviderCodeMapping() {
        assertThrows(IllegalArgumentException.class, () -> properties(HttpCarrier.KT,
                "message.skt.http.send.v1", "41001", "42002"));
        assertThrows(IllegalArgumentException.class, () -> properties(HttpCarrier.KT,
                "message.kt.http.send.v1", "", "42002"));
    }

    @Test
    void givesEachCarrierPodItsOwnRouteAndGroup() {
        for (HttpCarrier carrier : HttpCarrier.values()) {
            String name = carrier.name().toLowerCase();
            var config = properties(carrier, "message." + name + ".http.send.v1", "41001", "42002");
            assertEquals("http://" + name + ".test", config.baseUrl());
            assertEquals("messaging-" + name + "-http-sender", config.groupId());
            assertEquals("/api/v1/messages", config.requestPath());
        }
    }

    private static CarrierSenderProperties properties(HttpCarrier carrier, String topic,
                                                       String mismatch, String tps) {
        return new CarrierSenderProperties(carrier, topic,
                "messaging-" + carrier.name().toLowerCase() + "-http-sender",
                "http://" + carrier.name().toLowerCase() + ".test",
                null, null, null, null, null, mismatch, tps);
    }
}
