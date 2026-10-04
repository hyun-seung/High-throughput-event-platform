package messaging.common.messages;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PrimaryHttpFailureDecisionTest {
    private static final Instant DECIDED_AT = Instant.parse("2026-10-05T00:00:00Z");

    @Test
    void mismatchSkipsEveryAttemptedCarrierEvenWhenKtWasSelectedFirst() {
        assertEquals(new PrimaryHttpFailureDecision.Send(HttpCarrier.SKT, 1, DECIDED_AT),
                decide(66001, HttpCarrier.KT, 1, Set.of(HttpCarrier.KT)));
        assertEquals(new PrimaryHttpFailureDecision.Send(HttpCarrier.LGU, 1, DECIDED_AT),
                decide(66001, HttpCarrier.SKT, 1, Set.of(HttpCarrier.KT, HttpCarrier.SKT)));
        assertEquals(new PrimaryHttpFailureDecision.FailPrimary(40002),
                decide(66001, HttpCarrier.LGU, 1, Set.of(HttpCarrier.KT, HttpCarrier.SKT)));
    }

    @Test
    void tpsExceededAllowsThreeRetriesAfterTheFirstSend() {
        for (int invocation = 1; invocation <= 3; invocation++) {
            assertEquals(new PrimaryHttpFailureDecision.Send(HttpCarrier.KT, invocation + 1,
                            DECIDED_AT.plusSeconds(60)),
                    decide(66002, HttpCarrier.KT, invocation, Set.of(HttpCarrier.KT)));
        }
        assertEquals(new PrimaryHttpFailureDecision.FailPrimary(40001),
                decide(66002, HttpCarrier.KT, 4, Set.of(HttpCarrier.KT)));
    }

    @Test
    void unknownCodeAndImpossibleInvocationDoNotCauseAnotherSend() {
        assertThrows(IllegalArgumentException.class,
                () -> decide(66003, HttpCarrier.SKT, 1, Set.of(HttpCarrier.SKT)));
        assertThrows(IllegalArgumentException.class,
                () -> decide(66002, HttpCarrier.SKT, 5, Set.of(HttpCarrier.SKT)));
    }

    private static PrimaryHttpFailureDecision.Action decide(int code, HttpCarrier carrier, int invocation,
                                                             Set<HttpCarrier> attempted) {
        return PrimaryHttpFailureDecision.decide(code, carrier, invocation, attempted, DECIDED_AT);
    }
}
