package messaging.complete;

import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PrimaryStageDecision;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class FinalizedResultConsumerTest {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final FinalizedHistoryStore history = mock(FinalizedHistoryStore.class);
    private final FinalizedResultConsumer consumer = new FinalizedResultConsumer(history, mapper);

    @Test
    void matchingKeyPassesFinalResultToSqlStore() {
        var finalized = success("a".repeat(32));

        consumer.receive(new ConsumerRecord<>("MSG-RESULT-FINALIZED", 0, 1L,
                finalized.decision().clientMsgId(), mapper.writeValueAsString(finalized)));

        verify(history).store(finalized);
    }

    @Test
    void mismatchedKafkaKeyCannotWriteHistoryOrCdr() {
        var finalized = success("a".repeat(32));

        assertThrows(IllegalArgumentException.class, () -> consumer.receive(
                new ConsumerRecord<>("MSG-RESULT-FINALIZED", 0, 1L, "wrong",
                        mapper.writeValueAsString(finalized))));

        verifyNoInteractions(history);
    }

    private static FinalizedMessageResult success(String id) {
        var now = Instant.parse("2026-10-07T12:00:00Z");
        var decision = new PrimaryStageDecision("decision-1", id, PrimaryStageDecision.Kind.SUCCESS,
                "WEBHOOK", null, null, HttpCarrier.SKT, 1, false, now);
        var submission = new MessageSubmission(id, 42L, "customer-message", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "hello"), null, now.minusSeconds(30));
        return new FinalizedMessageResult(decision, submission);
    }
}
