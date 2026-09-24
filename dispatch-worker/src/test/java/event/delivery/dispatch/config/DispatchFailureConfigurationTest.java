package event.delivery.dispatch.config;

import event.delivery.dispatch.service.DispatchAttemptInProgressException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.*;

class DispatchFailureConfigurationTest {

    @Test
    void leaseWaitAndStorageErrorsAreNotDiscardedAfterDefaultRetryBudget() {
        assertRemainsUnrecovered(new DispatchAttemptInProgressException("delivery-1"));
        assertRemainsUnrecovered(new IllegalStateException("review storage unavailable"));
    }

    @Test
    void malformedRecordIsNotSilentlyDroppedWithoutDurableDlt() {
        assertRemainsUnrecovered(new DeserializationException(
                "invalid JSON", new byte[]{0}, false, new IllegalArgumentException("invalid JSON")));
    }

    private void assertRemainsUnrecovered(RuntimeException failure) {
        var handler = new DispatchFailureConfiguration().dispatchErrorHandler(0);
        var record = new ConsumerRecord<>("delivery.dispatch-requested.v1", 0, 42, "delivery-1", "input");
        var consumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);
        when(container.isRunning()).thenReturn(true);
        for (int attempt = 0; attempt < 12; attempt++) {
            assertFalse(handler.handleOne(failure, record, consumer, container),
                    "No successful processing or durable handoff has occurred");
        }
    }
}
