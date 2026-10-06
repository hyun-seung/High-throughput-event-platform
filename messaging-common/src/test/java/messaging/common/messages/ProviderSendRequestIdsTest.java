package messaging.common.messages;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProviderSendRequestIdsTest {
    @Test
    void differentCarriersAndInvocationsKeepTheSameSearchableMessageId() {
        String root = "00000000-0000-0000-0000-000000000001";
        assertEquals(new ProviderSendRequestIds.Parsed(root, HttpCarrier.SKT, 1),
                ProviderSendRequestIds.parse(ProviderSendRequestIds.forInvocation(root, HttpCarrier.SKT, 1)));
        assertEquals(new ProviderSendRequestIds.Parsed(root, HttpCarrier.KT, 1),
                ProviderSendRequestIds.parse(ProviderSendRequestIds.forInvocation(root, HttpCarrier.KT, 1)));
        assertEquals(new ProviderSendRequestIds.Parsed(root, HttpCarrier.KT, 2),
                ProviderSendRequestIds.parse(ProviderSendRequestIds.forInvocation(root, HttpCarrier.KT, 2)));
    }

    @Test
    void rejectsMalformedOrAmbiguousProviderIds() {
        assertThrows(IllegalArgumentException.class, () -> ProviderSendRequestIds.parse("request-1"));
        assertThrows(IllegalArgumentException.class, () -> ProviderSendRequestIds.parse("request-1:SKT:0"));
        assertThrows(IllegalArgumentException.class, () -> ProviderSendRequestIds.parse("request-1:SKT:01"));
        assertThrows(IllegalArgumentException.class, () -> ProviderSendRequestIds.parse("request-1:UNKNOWN:1"));
    }
}
