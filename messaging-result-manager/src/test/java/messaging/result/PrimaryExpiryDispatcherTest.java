package messaging.result;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.mockito.Mockito.*;

class PrimaryExpiryDispatcherTest {
    private static final Instant NOW = Instant.parse("2026-10-07T03:00:00Z");
    private static final String ID = "a".repeat(32);
    private final PrimaryStageDecisionStore decisions = mock(PrimaryStageDecisionStore.class);
    private final PrimaryExpiryDispatcher dispatcher = new PrimaryExpiryDispatcher(mock(DynamoDbClient.class),
            decisions, Clock.fixed(NOW, ZoneOffset.UTC), 100);
    private final Map<String, AttributeValue> candidate = Map.of(
            "pk", AttributeValue.fromS("DELIVERY#" + ID), "sk", AttributeValue.fromS("META"));

    @Test
    void unresolvedEarlierResultDelaysAnotherExpiryCheck() {
        when(decisions.fromExpiry(ID)).thenReturn(PrimaryStageDecisionStore.Outcome.WAITING);

        dispatcher.dispatch(candidate, NOW);

        verify(decisions).deferExpiry(ID, NOW.plusSeconds(10));
    }

    @Test
    void storedExpiryDoesNotScheduleAnotherCheck() {
        when(decisions.fromExpiry(ID)).thenReturn(PrimaryStageDecisionStore.Outcome.STORED);

        dispatcher.dispatch(candidate, NOW);

        verify(decisions, never()).deferExpiry(anyString(), any());
    }
}
