package messaging.pre.send;

import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.PreSendFailure;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class PreSendReceivedConsumerTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:00:00Z");
    private final MessageSubmission admission = new MessageSubmission("a".repeat(32), 42L, "customer-id",
            "01012345678", MessageCategory.GENERAL, Map.of("text", "hello"), null, NOW);
    private final PreSendDecisionStore decisions = mock(PreSendDecisionStore.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
    private final PreSendReceivedConsumer consumer = new PreSendReceivedConsumer(decisions, kafka);

    @Test
    void routesTheStoredCommandToTheCarrierTopic() throws Exception {
        var command = new HttpSendCommand("attempt-1", HttpCarrier.KT, 1, NOW.plusSeconds(100),
                new HttpProviderRequest(admission.clientMsgId(), 42L, "GENERAL", "01012345678",
                        admission.payload(), NOW));
        when(decisions.prepareOrLoad(admission)).thenReturn(Optional.of(new PreSendDispatch(command, null)));
        when(kafka.send(MessageTopics.KT_HTTP_SEND, admission.clientMsgId(), command))
                .thenReturn(CompletableFuture.completedFuture(null));

        consumer.receive(record());

        verify(kafka).send(MessageTopics.KT_HTTP_SEND, admission.clientMsgId(), command);
    }

    @Test
    void routesRejectionToResultAndPropagatesPublishFailure() {
        var failure = PreSendFailure.of(admission.clientMsgId(), PreSendFailure.Reason.CONTRACT_MISSING, NOW);
        when(decisions.prepareOrLoad(admission)).thenReturn(Optional.of(new PreSendDispatch(null, failure)));
        var failed = new CompletableFuture<org.springframework.kafka.support.SendResult<String, Object>>();
        failed.completeExceptionally(new IllegalStateException("Kafka unavailable"));
        when(kafka.send(MessageTopics.MSG_RESULT, admission.clientMsgId(), failure)).thenReturn(failed);

        assertThrows(java.util.concurrent.ExecutionException.class, () -> consumer.receive(record()));
    }

    private ConsumerRecord<String, MessageSubmission> record() {
        return new ConsumerRecord<>(MessageTopics.RECEIVED, 0, 1L, admission.clientMsgId(), admission);
    }
}
