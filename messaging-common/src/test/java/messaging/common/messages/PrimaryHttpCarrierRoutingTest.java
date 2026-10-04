package messaging.common.messages;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimaryHttpCarrierRoutingTest {
    @Test
    void skipsPreviouslyAttemptedCarriersInFixedOrderEvenWhenCacheInitiallySelectsKt() {
        assertEquals(HttpCarrier.SKT, PrimaryHttpCarrierRouting.nextUntried(Set.of(HttpCarrier.KT)).orElseThrow());
        assertEquals(HttpCarrier.LGU, PrimaryHttpCarrierRouting.nextUntried(Set.of(HttpCarrier.KT, HttpCarrier.SKT)).orElseThrow());
        assertTrue(PrimaryHttpCarrierRouting.nextUntried(Set.of(HttpCarrier.SKT, HttpCarrier.KT, HttpCarrier.LGU)).isEmpty());
    }
}
